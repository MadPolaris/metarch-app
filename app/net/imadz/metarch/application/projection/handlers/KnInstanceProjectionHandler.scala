package net.imadz.metarch.application.projection.handlers

import akka.projection.eventsourced.EventEnvelope
import akka.projection.jdbc.scaladsl.JdbcHandler
import net.imadz.common.application.projection.ScalikeJdbcSession
import net.imadz.metarch.application.projection.KnInstanceFamily
import net.imadz.metarch.application.projection.repository.{KnInstanceEventData, KnInstanceStore}
import org.slf4j.LoggerFactory
import scalapb.{GeneratedMessage, GeneratedOneof}
import scalikejdbc.DBSession

import scala.util.control.NonFatal
import scalapb.descriptors.{PLong, PString}

/**
 * R1 实例层投影 handler（批4·手写领地·声明见 KnInstanceStore 头注）。
 *
 * 泛化抽取（零逐事件样板）：ScalaPB GeneratedOneof 是 Product——
 *   - event.productPrefix＝事件类型名（ConceptEstablished…）；
 *   - event.value＋companion.scalaDescriptor.findFieldByName＝按名取字段（tenantId 等，
 *     覆盖面 108/146 PO——缺字段事件 tenant_id 落 NULL，R2 图物化侧按无租户事实处理）；
 *   - persistenceId 形如 "Concept|c-42"（PersistenceId(entityTypeKey.name, entityId)）——
 *     拆前缀得 aggType，拆后缀得 instanceId。
 *
 * 失败语义（DLQ＋offset 越过·kn_kb_storage_plan §八风险⑥）：任何 NonFatal 进
 * kn_projection_dlq 后吞掉——offset 推进，tag 不堵；DLQ 写失败再降级为日志（双落空留痕）。
 */
class KnInstanceProjectionHandler[E <: GeneratedOneof](
    family: KnInstanceFamily[E],
    store: KnInstanceStore
) extends JdbcHandler[EventEnvelope[E], ScalikeJdbcSession] {

  private val log = LoggerFactory.getLogger(getClass)

  override def process(session: ScalikeJdbcSession, envelope: EventEnvelope[E]): Unit = {
    val event = envelope.event
    if (event.isEmpty) return // 空 oneof（Empty 哨兵）：跳过

    val payload = event.value.asInstanceOf[GeneratedMessage]
    val data = KnInstanceEventData(
      eventId = s"${envelope.persistenceId}|${envelope.sequenceNr}",
      instanceId = entityIdOf(envelope.persistenceId),
      aggType = aggTypeOf(envelope.persistenceId),
      eventType = event.productPrefix,
      sequenceNr = envelope.sequenceNr,
      persistenceId = envelope.persistenceId,
      // 时间真源优先级：PO 载荷 timestamp（95/146 PO 带·业务真值）→ envelope.timestamp
      // （MongoReadJournal 无事件时间戳·批3 实证，缺省 0）→ 墙钟；
      // 重放场景已有 event_id 去重护栏，回退值不参与幂等
      occurredAtMillis = payloadLongField(payload, "timestamp")
        .orElse(if (envelope.timestamp > 0) Some(envelope.timestamp) else None)
        .getOrElse(System.currentTimeMillis()),
      tenantId = payloadField(payload, "tenantId"),
      causationId = None // R0.5 批3 裁决：command_audit 对齐注入位（预留）
    )

    try {
      implicit val s: DBSession = session.db.withinTxSession()
      store.recordEvent(data)
    } catch {
      case NonFatal(e) =>
        log.warn(s"kn_instance 投影失败，转 DLQ（${family.tagPrefix}·${data.eventType} ${data.eventId}）: ${e.getMessage}")
        try {
          implicit val s: DBSession = session.db.withinTxSession()
          store.recordDlq(data, e)
        } catch { case NonFatal(_) => () /* recordDlq 内部已留痕 */ }
    }
  }

  private def entityIdOf(persistenceId: String): String = persistenceId.split('|') match {
    case Array(_, id) => id
    case _            => persistenceId // 非常规形态：整体充当 instanceId（不丢事件）
  }

  private def aggTypeOf(persistenceId: String): String = persistenceId.split('|') match {
    case Array(tpe, _) => tpe
    case _             => family.tagPrefix.stripSuffix("Tag")
  }

  private def payloadField(payload: GeneratedMessage, name: String): Option[String] =
    payload.companion.scalaDescriptor.findFieldByName(name).flatMap { f =>
      payload.getField(f) match {
        case PString(v) if v.nonEmpty => Some(v)
        case _                        => None
      }
    }

  private def payloadLongField(payload: GeneratedMessage, name: String): Option[Long] =
    payload.companion.scalaDescriptor.findFieldByName(name).flatMap { f =>
      payload.getField(f) match {
        case PLong(v) if v > 0 => Some(v)
        case _                 => None
      }
    }
}
