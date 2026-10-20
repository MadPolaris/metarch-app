package net.imadz.metarch.application.projection.repository

import org.slf4j.LoggerFactory
import scalikejdbc.{DBSession, _}

/**
 * R1 实例层投影存储（批4·kn_kb_storage_plan §2.3/§4.3）——kn_instance_* 表族唯一写者。
 *
 * 手写领地声明（挂 EP_S2_48 批1·writeTerritorial）：本文件族
 * （KnInstanceStore/KnInstanceProjection/KnInstanceProjectionHandler/KnInstanceProjectionModule/
 * conf/sql/4.sql）为行为侧手写代码，**不在 CodegenMain 生成产物清单内**（新文件名无碰撞面），
 * 禁止未来把 Kn* 前缀加入生成器输出集；如需引擎接管须先迁移领地声明。
 *
 * 幂等纪律：
 *  - kn_instance_event：event_id 主键，SELECT 先行再 INSERT（exactlyOnce 重放/重启重投安全）；
 *  - kn_aggregate_instance：version=sequenceNr 的 GREATEST 护栏 upsert（乱序/重放不回退）。
 * DDL 与 conf/sql/4.sql 同源（改这里必同步那边）。
 */
class KnInstanceStore {

  private val log = LoggerFactory.getLogger(getClass)

  /** 建表幂等（运行时自建；表已存在零副作用）。 */
  def ensureTables(): Unit = DB.autoCommit { implicit s =>
    sql"""CREATE TABLE IF NOT EXISTS kn_aggregate_instance (
          instance_id VARCHAR(255) NOT NULL,
          agg_type VARCHAR(128) NOT NULL,
          tenant_id VARCHAR(128),
          status VARCHAR(64),
          version BIGINT NOT NULL,
          updated_at_millis BIGINT NOT NULL,
          PRIMARY KEY (instance_id, agg_type)
        )""".update.apply()
    sql"""CREATE TABLE IF NOT EXISTS kn_instance_event (
          event_id VARCHAR(255) NOT NULL,
          instance_id VARCHAR(255) NOT NULL,
          agg_type VARCHAR(128) NOT NULL,
          event_type VARCHAR(128) NOT NULL,
          sequence_nr BIGINT NOT NULL,
          persistence_id VARCHAR(255) NOT NULL,
          occurred_at_millis BIGINT NOT NULL,
          tenant_id VARCHAR(128),
          causation_id VARCHAR(255),
          PRIMARY KEY (event_id)
        )""".update.apply()
    sql"""CREATE TABLE IF NOT EXISTS kn_policy_firing (
          id BIGINT NOT NULL AUTO_INCREMENT,
          policy_id VARCHAR(128) NOT NULL,
          trigger_event_id VARCHAR(255) NOT NULL,
          target_command VARCHAR(128),
          fired_at_millis BIGINT NOT NULL,
          tenant_id VARCHAR(128),
          PRIMARY KEY (id),
          CONSTRAINT uq_kn_policy_firing UNIQUE (policy_id, trigger_event_id)
        )""".update.apply()
    sql"""CREATE TABLE IF NOT EXISTS kn_projection_dlq (
          event_id VARCHAR(255) NOT NULL,
          agg_type VARCHAR(128),
          event_type VARCHAR(128),
          persistence_id VARCHAR(255),
          sequence_nr BIGINT,
          error VARCHAR(1024),
          failed_at_millis BIGINT NOT NULL,
          PRIMARY KEY (event_id)
        )""".update.apply()
  }

  /**
   * 落一笔实例事件＋刷新聚合实例行（同一事务——与 offset 提交原子）。
   * 幂等：event_id 已存在则整笔跳过（重放零副作用）。
   * @return true=实际写入；false=重放去重跳过
   */
  def recordEvent(event: KnInstanceEventData)(implicit s: DBSession): Boolean = {
    val exists = sql"SELECT COUNT(*) FROM kn_instance_event WHERE event_id = ${event.eventId}"
      .map(_.int(1)).single.apply().getOrElse(0) > 0
    if (exists) false
    else {
      sql"""INSERT INTO kn_instance_event
            (event_id, instance_id, agg_type, event_type, sequence_nr, persistence_id,
             occurred_at_millis, tenant_id, causation_id)
            VALUES (
              ${event.eventId}, ${event.instanceId}, ${event.aggType}, ${event.eventType},
              ${event.sequenceNr}, ${event.persistenceId},
              ${event.occurredAtMillis}, ${event.tenantId}, ${event.causationId})""".update.apply()
      upsertInstance(event)
      true
    }
  }

  /** 聚合实例 upsert：version GREATEST 护栏（乱序/重放不回退）；首事件 INSERT。 */
  private def upsertInstance(event: KnInstanceEventData)(implicit s: DBSession): Unit = {
    val existing = sql"SELECT version FROM kn_aggregate_instance WHERE instance_id = ${event.instanceId} AND agg_type = ${event.aggType}"
      .map(_.long("version")).single.apply()
    existing match {
      case Some(v) if v >= event.sequenceNr => () // 旧序号迟到/重放：不回退
      case Some(_) =>
        sql"""UPDATE kn_aggregate_instance SET version = ${event.sequenceNr},
              tenant_id = COALESCE(${event.tenantId}, tenant_id),
              updated_at_millis = ${event.occurredAtMillis}
              WHERE instance_id = ${event.instanceId} AND agg_type = ${event.aggType}""".update.apply()
      case None =>
        sql"""INSERT INTO kn_aggregate_instance
              (instance_id, agg_type, tenant_id, status, version, updated_at_millis)
              VALUES (
                ${event.instanceId}, ${event.aggType}, ${event.tenantId},
                NULL /* R1: 事件 PO 无 status 字段（仅 State PO 有）——状态映射挂账后续批 */,
                ${event.sequenceNr}, ${event.occurredAtMillis})""".update.apply()
    }
  }

  /** 毒丸收容：handler 失败落 DLQ 后 offset 越过（tag 不堵）。幂等（event_id 主键）。 */
  def recordDlq(event: KnInstanceEventData, error: Throwable)(implicit s: DBSession): Unit = {
    try {
      val exists = sql"SELECT COUNT(*) FROM kn_projection_dlq WHERE event_id = ${event.eventId}"
        .map(_.int(1)).single.apply().getOrElse(0) > 0
      if (!exists) {
        val msg = Option(error.getMessage).getOrElse(error.getClass.getSimpleName).take(1000)
        sql"""INSERT INTO kn_projection_dlq
              (event_id, agg_type, event_type, persistence_id, sequence_nr, error, failed_at_millis)
              VALUES (
                ${event.eventId}, ${event.aggType}, ${event.eventType},
                ${event.persistenceId}, ${event.sequenceNr}, ${msg}, ${System.currentTimeMillis()})""".update.apply()
      }
    } catch {
      case e: Throwable => log.warn(s"DLQ 收容失败（事件 ${event.eventId} 双落空，仅日志留痕）: ${e.getMessage}")
    }
  }
}

/** 一笔实例事件的全量投影数据（由 handler 从 envelope 泛化抽取）。 */
final case class KnInstanceEventData(
    eventId: String,
    instanceId: String,
    aggType: String,
    eventType: String,
    sequenceNr: Long,
    persistenceId: String,
    occurredAtMillis: Long,
    tenantId: Option[String],
    causationId: Option[String] = None
)
