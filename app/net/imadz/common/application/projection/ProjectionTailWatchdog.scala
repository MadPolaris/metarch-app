package net.imadz.common.application.projection

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.typed.ActorSystem
import akka.persistence.query.Offset
import akka.projection.ProjectionId
import akka.projection.scaladsl.ProjectionManagement
import com.mongodb.ConnectionString
import com.typesafe.config.ConfigFactory
import org.bson.BsonObjectId
import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Sorts.descending
import org.mongodb.scala.MongoClient
import org.slf4j.LoggerFactory
import scalikejdbc._

/**
 * ProjectionTailWatchdog (BUG-012)：eventsByTag 活订阅（akka-persistence-mongo 3.0.8
 * tailable-await 尾巴）在半死连接/游标静默死亡时不产生 onError，投影偏移冻结且无错误日志。
 *
 * ★ 2026-09-26 判据升级（用户裁决「甲」·逐投影停滞检测）：
 *   旧判据＝**全表** `MAX(last_updated)` 连续零变化。**覆盖缺口**：只要**任一**投影还在提交，
 *   全表 MAX 就变 ⇒ 计数归零 ⇒ **「一条投影静默死亡、其余照常」永不触发自愈**。实证：本轮
 *   `capture_note_decision_view` 单条冻结 30+ 分钟、其余 60 条在动，watchdog 只打了 1/2 就再无下文。
 *   新判据＝**逐投影**：对每个 (projection_name, projection_key) 判「它**该不该**动」——
 *   ① 取当前位点（`current_offset` 是 base64 的 ObjectId）；
 *   ② 查 **journal**（非 capped·`_tg` 索引）该 tag 下有无 `_id > 位点` 的行（journal 是真实权威；
 *      realtime 是 capped 镜像，仅用于「写侧是否推进」）；
 *   ③ 有未消费行 且 `last_updated` 两个周期未变 ⇒ 该投影停滞 ⇒ 计数；
 *   ④ 计数达阈 ⇒ 只治**这一条**（`updateOffset` 单流重启），而非旧版的「全表 61 条一起重建」。
 *   ⇒ 既闭合了覆盖缺口，又把自愈面从「全表」收窄到「真有病的那条」（更外科）。
 *
 * ★ 2026-10-01 二级自愈（BUG-012 根治批·P5a）：heal 无效的机理＝updateOffset 重建的**新 tailable
 *   cursor 仍挂回同一条 ReactiveMongo 连接池**（09-29 实证：一场风暴 heal 40 次/3.5h 无效·同批
 *   投影反复冻结·最后靠整服务重启断根）。故加升级线：**heal 后 `healStallIntervals` 轮仍停滞** ⇒
 *   判「流级重建无效」⇒ 发 `ProjectionBehavior.Stop` 给该投影的 daemon entity actor ——
 *   ShardedDaemonProcess keepAlivePinger（keep-alive-interval=10s）对已停 entity 周期重发
 *   StartEntity ⇒ behaviorFactory 重跑 ⇒ ProjectionBehavior/事件流整体重生（单投影粒度·
 *   等效「只重启这一条」）。entity 寻址＝EntityTypeKey(s"sharded-daemon-process-<daemonName>")
 *   ＋entityId "0"（ShardedDaemonProcessImpl 字节码实证：typeKey 前缀＋entityId=Integer.toString(index)·
 *   本仓全部投影 tags.size=1 ⇒ index 恒 0）。daemonName 反解＝snake(projection_name) 的逆：
 *   offset store 名为 snake_case ⇒ PascalCase（policy runner 例外实证：offset=policy_on_apply_knowledge_patch
 *   daemon=PolicyOnApplyKnowledgePatchRunner·同规则）；kn_instance_projection 特例＝daemon
 *   name 带 tag 前缀 `kn-instance-<TagPrefix>`（19 族各自 daemon 集·tag=TagPrefix+index 可反解）。
 *
 * 保留的旧能力：realtime 头部持续推进＝写侧健康（头部不动则整机无写入，不是投影病，不治）。
 * 自愈两级：L1 updateOffset（流级·轻）→ L2 ProjectionBehavior.Stop（actor 级·重）。
 */
object ProjectionTailWatchdog {

  private val log = LoggerFactory.getLogger(getClass)

  /** 位点行（2026-09-26 判据升级：带 `offset` 与 `lastUpdated`，供**逐投影**停滞判定）。 */
  private final case class OffsetRow(projection: String, key: String, offsetB64: String, lastUpdated: Long)
  private final case class Snapshot(headIdHex: Option[String], rows: Seq[OffsetRow]) {
    def maxLastUpdated: Long = if (rows.isEmpty) 0L else rows.map(_.lastUpdated).max
  }

  // ── L2 actor 寻址（2026-10-01）──
  /** snake_case → PascalCase（offset store projection_name → ShardedDaemonProcess daemon name）。
    * policy_on_apply_knowledge_patch → PolicyOnApplyKnowledgePatch（daemon 实为
    * PolicyOnApplyKnowledgePatchRunner·带 Runner 后缀）——此换算**不处理后缀差异**，
    * 后缀全集在 daemonNameCandidates 兜底。 */
  private def pascalOf(snake: String): String =
    snake.split("_").filter(_.nonEmpty).map { w => val cs = w.toCharArray; cs(0) = cs(0).toUpper; new String(cs) }.mkString

  /** offset 名 → 候选 daemon name 集（按命中概率排序）。Runner 系后缀差异由候选集兜底。 */
  private def daemonNameCandidates(offsetName: String): Seq[String] = {
    val p = pascalOf(offsetName)
    if (offsetName.startsWith("policy_on_")) Seq(p + "Runner", p)
    else Seq(p, p + "Runner")
  }

  /** kn_instance_projection 的 tag（如 ConceptTag0）→ daemon name（kn-instance-ConceptTag）。 */
  private def knInstanceDaemonOf(tag: String): String =
    s"kn-instance-${tag.replaceAll("\\d+$", "")}"

  def start()(implicit system: ActorSystem[_]): Unit = {
    val root = system.settings.config
    val defaults = ConfigFactory.parseString(
      """
        |metarch.projection-tail-watchdog {
        |  enabled = true
        |  check-interval = 30 seconds
        |  stall-intervals = 4
        |  heal-cooldown = 5 minutes
        |  escalate-intervals = 3
        |  escalate-cooldown = 15 minutes
        |}
      """.stripMargin)
    val wcfg = root.withFallback(defaults).getConfig("metarch.projection-tail-watchdog")

    if (!wcfg.getBoolean("enabled")) {
      log.info("ProjectionTailWatchdog disabled by config")
      return
    }

    val checkInterval = wcfg.getDuration("check-interval").toMillis.millis
    val stallIntervals = wcfg.getInt("stall-intervals")
    val healCooldownMs = wcfg.getDuration("heal-cooldown").toMillis
    val escalateIntervals = wcfg.getInt("escalate-intervals")   // L2：heal 后仍停滞 N 轮 ⇒ actor 级重启
    val escalateCooldownMs = wcfg.getDuration("escalate-cooldown").toMillis

    val mongouri = root.getString("akka.contrib.persistence.mongodb.mongo.mongouri")
    val realtimeCollection = {
      val mc = root.getConfig("akka.contrib.persistence.mongodb.mongo")
      if (mc.hasPath("realtime-collection")) mc.getString("realtime-collection") else "akka_persistence_realtime"
    }
    val dbName = new ConnectionString(mongouri).getDatabase
    if (dbName == null || dbName.isEmpty) {
      log.error("ProjectionTailWatchdog not started: mongouri [{}] carries no database name", mongouri)
      return
    }

    val client: MongoClient = MongoClient(mongouri)
    val mdb = client.getDatabase(dbName)
    val rtColl = mdb.getCollection(realtimeCollection)
    // journal＝**非 capped** 权威集合（2026-09-26 实测：realtime 是 capped·maxSize 100MB 会淘汰
    // ⇒ 「有无未消费行」的判据必须走 journal；realtime 仅用于「写侧头部是否推进」）。
    val journalColl = mdb.getCollection("akka_persistence_journal")

    implicit val ec =
      system.classicSystem.dispatchers.lookup("akka.projection.jdbc.blocking-jdbc-dispatcher")

    val prevSnapshot = new AtomicReference[Option[Snapshot]](None)
    val stallStreak = new java.util.concurrent.ConcurrentHashMap[String, Integer]()   // 键＝"<proj>/<key>"
    val postHealStreak = new java.util.concurrent.ConcurrentHashMap[String, Integer]() // L2：heal 后仍停滞计数
    val lastHealAtMs = new AtomicLong(0L)
    val lastEscalateAtMs = new AtomicLong(0L)
    def streakKey(r: OffsetRow): String = s"${r.projection}/${r.key}"

    def fetchRealtimeHead(): Future[Option[String]] =
      rtColl.find().sort(descending("_id")).limit(1).first().headOption()
        .map(maybeDoc =>
          maybeDoc.flatMap(_.get("_id").collect {
            case oid: BsonObjectId => oid.getValue.toHexString
          }))
        .recover { case ex =>
          log.warn(s"ProjectionTailWatchdog: realtime head query failed: ${ex.getMessage}")
          None
        }

    def fetchOffsetStore(): Future[Snapshot] = Future {
      DB.readOnly { implicit session =>
        val rows =
          sql"select projection_name, projection_key, current_offset, last_updated from akka_projection_offset_store"
            .map(rs => (rs.string(1), rs.string(2), rs.string(3), rs.long(4)))
            .list()
            .apply()
        Snapshot(None, rows.collect {
          case (n, k, off, lu) if n != null && k != null => OffsetRow(n, k, if (off == null) "" else off, lu)
        })
      }
    }

    /** 位点 base64 → 可比较的 hex（空/损坏 ⇒ None）。 */
    def offsetHex(b64: String): Option[String] =
      try {
        if (b64 == null || b64.isEmpty) None
        else {
          val h = java.util.Base64.getDecoder.decode(b64)
          if (h.length >= 12) Some(h.take(12).map((b) => f"${b & 0xff}%02x").mkString) else None
        }
      } catch { case _: Throwable => None }

    def decodeHex8(b64: String): String = offsetHex(b64).map(_.take(8)).getOrElse("?")

    /** 该 tag 下是否有 `_id > offset` 的未消费事件（journal·_tg 索引）。
      *  ⚠️ 2026-09-26 实测修正：**不能只看「有未消费事件」**——投影重建后可能**已提交到最新**
      *  （位点 == tag 头），此时它「不动」是**正常的**（没有新事件可处理），却被误判为停滞并反复重建
      *  ⇒ **越治越僵**（kn_instance_projection / note_graph_view 实证：位点已是 6ab71b58… 却被重建）。
      *  故此处**返回两组**：`(落后于头, 头位点)`，调用方须以「**严格落后**」为治的条件。
      */
    def tagProbe(key: String, offsetB64: String): Future[(Boolean, Option[String])] = {
      val fromId: Option[ObjectId] =
        try {
          if (offsetB64 == null || offsetB64.isEmpty) None
          else {
            val h = java.util.Base64.getDecoder.decode(offsetB64)
            if (h.length >= 12) Some(new ObjectId(h)) else None
          }
        } catch { case _: Throwable => None }
      // 该 tag 的**头部**（最新文档）
      journalColl.find(Filters.in("_tg", key)).sort(descending("_id")).limit(1).first().headOption()
        .flatMap { headDoc =>
          val headHex = headDoc.flatMap(_.get("_id").collect { case oid: BsonObjectId => oid.getValue.toHexString })
          fromId match {
            case None => Future.successful((true, headHex)) // 从未推进 ⇒ 保守判落后
            case Some(oid) => Future.successful((headHex.exists(_ > oid.toHexString), headHex))
          }
        }
        .recover { case ex =>
          log.debug(s"ProjectionTailWatchdog: tag probe failed for [$key]: ${ex.getMessage}")
          (false, None)
        }
    }

    /** 只治给定的几条投影（逐投影自愈·2026-09-26 升级）——旧版是「全表 61 条一起重建」。 */
    def healOne(rows: Seq[OffsetRow]): Future[Int] = {
      val mgmt = ProjectionManagement(system)
      rows.foldLeft(Future.successful(0)) { (acc, r) =>
        acc.flatMap { healed =>
          val pid = ProjectionId(r.projection, r.key)
          mgmt.getOffset[Offset](pid).flatMap {
            case Some(offset) => mgmt.updateOffset(pid, offset).map(_ => healed + 1)
            case None         => Future.successful(healed)
          }.recover {
            case ex =>
              log.warn(s"ProjectionTailWatchdog: failed to heal [$pid]: ${ex.getMessage}")
              healed
          }
        }
      }
    }

    /** L2 actor 级重启（2026-10-01 二级自愈）：向 daemon entity 发 ProjectionBehavior.Stop——
      * ShardedDaemonProcess keepAlivePinger（10s）对已停 entity 重发 StartEntity ⇒ behaviorFactory
      * 重跑 ⇒ ProjectionBehavior/事件流整体重生。entity 寻址＝EntityTypeKey
      * s"sharded-daemon-process-<daemonName>" ＋ entityId "0"（tags.size=1 ⇒ index 恒 0）。
      * Stop 消息＝Bootstrap stopMessage=Some(ProjectionBehavior.Stop)·与 daemon 实体协议一致。
      */
    def escalateOne(rows: Seq[OffsetRow]): Int = {
      val sharding = akka.cluster.sharding.typed.scaladsl.ClusterSharding(system)
      var sent = 0
      for (r <- rows) {
        val daemonNames =
          if (r.projection == "kn_instance_projection") Seq(knInstanceDaemonOf(r.key))
          else daemonNameCandidates(r.projection)
        var delivered = false
        for (dn <- daemonNames if !delivered) {
          try {
            val key = akka.cluster.sharding.typed.scaladsl.EntityTypeKey[akka.projection.ProjectionBehavior.Command](s"sharded-daemon-process-$dn")
            val ref = sharding.entityRefFor(key, "0")
            ref ! akka.projection.ProjectionBehavior.Stop
            delivered = true
            sent += 1
            log.warn(s"ProjectionTailWatchdog: L2 escalate → Stop sent to daemon [$dn] entity[0] (proj=${r.projection} key=${r.key}) — keepAlive will re-pull within ~10s")
          } catch {
            case ex: Throwable =>
              log.warn(s"ProjectionTailWatchdog: L2 escalate failed for daemon [$dn]: ${ex.getMessage}")
          }
        }
        if (!delivered) log.warn(s"ProjectionTailWatchdog: L2 escalate — no daemon candidate hit for proj=${r.projection} (tried ${daemonNames.mkString(",")})")
      }
      sent
    }

    def runOnce(): Unit = {
      val probe =
        for {
          head <- fetchRealtimeHead()
          snap <- fetchOffsetStore()
        } yield (head, snap)

      probe.onComplete {
        case Failure(ex) =>
          log.debug(s"ProjectionTailWatchdog: probe skipped: ${ex.getMessage}")
        case Success((None, _)) =>
          () // 写侧头部不可得 ⇒ 不作判定（不是投影病）
        case Success((Some(head), snap)) =>
          val prev = prevSnapshot.getAndSet(Some(snap.copy(headIdHex = Some(head))))
          // 2026-10-08 根修（三小批收口·流静默死亡二犯）：原 headAdvanced 门有盲区——写入停止后
          //   投影卡在历史事件上（tailable 游标静默断·BUG-012 家族新形态·位点<head·零 ERROR 零
          //   Restart 日志）时，head 两轮不变 ⇒ stallStreak 永远 clear ⇒ 死流永不被发现（两日实证）。
          //   修法：去掉 head 门·把「空闲≠落后」保护下沉到 per-projection 判据本身——
          //   last_updated 两轮未变（无进展）∧ 位点严格落后于 tag 头（tagProbe behind=true）
          //   才计数；有进展或已追平自动清零。空闲健康流（位点==头）天然不触发。
          val prevLu: Map[(String, String), Long] =
            prev.map(_.rows.map((r) => (r.projection, r.key) -> r.lastUpdated).toMap).getOrElse(Map.empty)
          // 逐投影：① **严格落后**于该 tag 头部（不是「有未消费」——见 tagProbe 注）
          //          ② last_updated 两个周期未变 ⇒ 判停滞
          val checks: Seq[Future[Option[OffsetRow]]] = snap.rows.map { r =>
            val unchanged = prevLu.get((r.projection, r.key)).contains(r.lastUpdated)
            if (!unchanged) Future.successful(None)
            else tagProbe(r.key, r.offsetB64).map { case (behind, _) => if (behind) Some(r) else None }
          }
            Future.sequence(checks).foreach { results =>
              val stalled = results.flatten
              // 未停滞的投影：清零其计数（恢复即销账）
              val stalledKeys = stalled.map(streakKey).toSet
              snap.rows.foreach { r =>
                if (!stalledKeys.contains(streakKey(r))) {
                  stallStreak.remove(streakKey(r))
                  postHealStreak.remove(streakKey(r))
                }
              }
              // 停滞的投影：计数 +1
              val ripe = stalled.filter { r =>
                val n = stallStreak.merge(streakKey(r), 1, (a, b) => a + b)
                n >= stallIntervals
              }
              if (ripe.nonEmpty) {
                val now = System.currentTimeMillis()
                if (now - lastHealAtMs.get() >= healCooldownMs) {
                  lastHealAtMs.set(now)
                  ripe.foreach((r) => stallStreak.remove(streakKey(r)))
                  log.warn(
                    s"ProjectionTailWatchdog: ${ripe.size} livetail(s) presumed dead " +
                      s"(rt-head=$head) — 逐投影重建：" +
                      ripe.map((r) => streakKey(r)).mkString(", "))
                  healOne(ripe).onComplete {
                    case Success(cnt) =>
                      log.warn(s"ProjectionTailWatchdog: rebuilt $cnt projection stream(s) from stored offsets")
                      // L1 heal 后进入「观察期」——每条 heal 过的投影记入 postHeal 观察名单
                      ripe.foreach((r) => postHealStreak.put(streakKey(r), 0))
                    case Failure(ex) =>
                      log.error("ProjectionTailWatchdog: rebuild pass failed", ex)
                  }
                }
              } else if (stalled.nonEmpty) {
                val desc = stalled
                  .map((r) => s"${streakKey(r)}@${stallStreak.get(streakKey(r))}")
                  .mkString(", ")
                // 凶手线索（2026-09-26）：**同 tag 上所有**投影的位点——停滞者大多并非尾部死亡，
                //   而是被同 tag 的「永久失败者」连坐（实测：knowledge_semantic_view 撞 Postgres
                //   不可用 ⇒ recovery 策略永久停流 ⇒ 同 tag 的健康投影一起被卡在它前面的事件上）。
                val sameTag = snap.rows.filter((r) => r.key == stalled.head.key)
                val census = sameTag.map((r) => s"${r.projection}=${decodeHex8(r.offsetB64)}").mkString(", ")
                log.warn(
                  s"ProjectionTailWatchdog: ${stalled.size} projection(s) have unconsumed events but " +
                    s"no commits for <$stallIntervals check(s): $desc｜同 tag(${stalled.head.key}) 位点普查＝$census")
                // ── L2 升级判定（2026-10-01）：heal 观察期内的投影仍停滞 ⇒ postHealStreak+1；
                //    达 escalateIntervals ⇒ actor 级重启（Stop→keepAlive 重拉）。
                val escalatable = stalled.filter { r =>
                  val k = streakKey(r)
                  if (postHealStreak.containsKey(k)) {
                    val n = postHealStreak.merge(k, 1, (a: Integer, b: Integer) => a + b)
                    n >= escalateIntervals
                  } else false
                }
                if (escalatable.nonEmpty) {
                  val now = System.currentTimeMillis()
                  if (now - lastEscalateAtMs.get() >= escalateCooldownMs) {
                    lastEscalateAtMs.set(now)
                    escalatable.foreach((r) => postHealStreak.remove(streakKey(r)))
                    val sent = escalateOne(escalatable)
                    log.warn(s"ProjectionTailWatchdog: L2 escalated $sent projection(s) to actor-level restart " +
                      s"(heal was ineffective — ${escalatable.map(streakKey).mkString(", ")})")
                  }
                }
              }
            }
      }
    }

    system.whenTerminated.onComplete { _ =>
      try client.close()
      catch { case _: Throwable => () }
    }(ec)

    log.info(
      s"ProjectionTailWatchdog started: db=$dbName rt=$realtimeCollection " +
        s"interval=$checkInterval stallIntervals=$stallIntervals cooldownMs=$healCooldownMs " +
        s"escalateIntervals=$escalateIntervals escalateCooldownMs=$escalateCooldownMs")

    system.classicSystem.scheduler.scheduleWithFixedDelay(checkInterval, checkInterval)(
      new Runnable { override def run(): Unit = runOnce() })(ec)
  }

}
