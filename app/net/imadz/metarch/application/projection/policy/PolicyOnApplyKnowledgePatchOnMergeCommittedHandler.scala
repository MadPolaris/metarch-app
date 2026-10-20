// MANUAL（EP_Catalog_Concept_Write_Model §3.4·2026-09-22；Option A 改订 2026-09-22）
//
// 职责：把「KnowledgePatchMergeCommitted」事件翻成「ApplyKnowledgePatch」命令，发给 ModelCatalog 聚合。
// 纯搬运——策略语义全在 domain/policy/ModelCatalogPolicies.PolicyOnApplyKnowledgePatch（可单测）。
//
// ⚠️ Option A 两阶段（用户裁决 2026-09-22）：本 handler 原订阅 `KnowledgePatchMerged`，
//   而该事件在 Option A 下**已改义**——`MergeKnowledgePatch` 不再自称终态，只发
//   `KnowledgePatchMergeCommitted`（＝已提交目录应用）；patch 真正 MERGED 要等 catalog 侧
//   应用成功、由 `PolicyOnConfirmPatchMerged` 回执 `ConfirmPatchMerged` 才达成。
//   故订阅点随之改指 Commit 事件（否则「合并」在 catalog 判它合格之前就自称完成＝谎报）。
//
// 失败语义（同 PolicyOnCreateCatalogVersion·用户裁决 2026-09-22）：Policy 必须对已发生的事实作出反应，
// 没有「拒绝」语义。做不完 → 返回 failed Future → akka-projection 不提交 offset → 重试（at-least-once）。
// 重试安全前提＝策略声明的 `idempotencyKey: command-level`（ApplyKnowledgePatch 在
// parentPatchId == state.headPatchId 时是空操作）。
//
// ★ DLQ 吞毒（挂账 L6 落地·2026-09-29）：AGG_NOT_INITIALIZED 这类**回执级永久失败**
//   （catalog 是投影行非活聚合·存量毒事件重试一万次也不会成功）原语义会把整条
//   KnowledgePatchTag0 投影流卡死在固定 offset——后续所有合并事件（含挂账A guards 回锚）
//   永远到不了图。修法＝KnInstanceProjectionHandler 同款：落 kn_projection_dlq 留痕后
//   吞掉返回 Done，offset 推进（tag 不堵）；可观测性由 DLQ 表承载。毒事件不再堵流，
//   与「投影毒丸 DLQ」建表注释（conf/sql/4.sql）语义对齐。
package net.imadz.metarch.application.projection.policy

import akka.Done
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.util.Timeout
import net.imadz.metarch.application.aggregates.ModelCatalogAggregate
import net.imadz.metarch.application.aggregates.ModelCatalogProtocol.{ApplyKnowledgePatch, ApplyKnowledgePatchReply}
import net.imadz.metarch.domain.policy.ModelCatalogPolicies.PolicyOnApplyKnowledgePatch
import net.imadz.metarch.infrastructure.persistence.converters.KnowledgePatchProtoConverters.KnowledgePatchMergeCommittedConv
import net.imadz.metarch.infrastructure.proto.knowledge_patch.KnowledgePatchEventPO
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * KnowledgePatchMergeCommitted → ApplyKnowledgePatch。
 *
 * @param sharding 目标聚合（ModelCatalog）的 cluster sharding 句柄
 */
class PolicyOnApplyKnowledgePatchOnMergeCommittedHandler(sharding: ClusterSharding)(implicit ec: ExecutionContext)
    extends Handler[EventEnvelope[KnowledgePatchEventPO.Event]] {

  private implicit val askTimeout: Timeout = Timeout(10.seconds)
  private val log = LoggerFactory.getLogger(getClass)

  /** DLQ 吞毒（挂账 L6·2026-09-29）：永久性回执失败落 kn_projection_dlq 留痕后吞掉——offset 推进，tag 不堵。幂等（event_id 主键）。 */
  private def dlqAndSwallow(envelope: EventEnvelope[KnowledgePatchEventPO.Event], what: String): Future[Done] = {
    val eventId = s"${envelope.persistenceId}|${envelope.sequenceNr}"
    log.warn(s"[policy] 投影毒丸转 DLQ（apply_knowledge_patch·$what·offset 推进不堵流）: $eventId")
    try {
      NamedDB('default).localTx { implicit s =>
        sql"""INSERT IGNORE INTO kn_projection_dlq
              (event_id, agg_type, event_type, persistence_id, sequence_nr, error, failed_at_millis)
              VALUES (${eventId}, 'KnowledgePatch', 'KnowledgePatchMergeCommitted',
                ${envelope.persistenceId}, ${envelope.sequenceNr}, ${what.take(1000)}, ${System.currentTimeMillis()})""".update.apply()
      }
    } catch { case e: Throwable => log.warn(s"DLQ 收容失败（$eventId 双落空，仅日志留痕）: ${e.getMessage}") }
    Future.successful(Done)
  }

  override def process(envelope: EventEnvelope[KnowledgePatchEventPO.Event]): Future[Done] =
    envelope.event match {
      case KnowledgePatchEventPO.Event.KnowledgePatchMergeCommitted(po) =>
        val e = KnowledgePatchMergeCommittedConv.fromProto(po)
        val params = PolicyOnApplyKnowledgePatch.on(e)
        Future
          .traverse(params) { p =>
            // sharding key ＝ 事件载荷里的 catalogId（merge 后 catalog 已知自己在链上哪）
            sharding
              .entityRefFor(ModelCatalogAggregate.entityTypeKey, p.catalogId.toString)
              .ask[ApplyKnowledgePatchReply](ApplyKnowledgePatch(
                p.catalogId, p.sessionId, p.patchId, p.patchHash, p.parentPatchId, p.concepts, p.triggeredBy, _))
              .flatMap {
                case ApplyKnowledgePatchReply(None)    => Future.successful(Done)
                case ApplyKnowledgePatchReply(Some(err)) =>
                  // 挂账 L6（2026-09-29）：回执级永久失败（AGG_NOT_INITIALIZED＝catalog 非活聚合）
                  // 重试不可愈 → DLQ 留痕吞掉（offset 推进·tag 不堵）；其余失败维持原重试语义
                  if (err.code == "AGG_NOT_INITIALIZED")
                    dlqAndSwallow(envelope, s"补丁合并后应用到目录失败：patchId=${e.patchId} catalogId=${e.catalogId} err=${err.code}")
                  else
                    Future.failed(new RuntimeException(
                      s"[policy] 补丁合并后应用到目录失败：patchId=${e.patchId} catalogId=${e.catalogId} err=${err.code}"))
              }
          }
          .map(_ => Done)
      case _ => Future.successful(Done) // 未知事件优雅跳过（勿打爆投影流）
    }
}
