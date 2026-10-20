// MANUAL（EP_Catalog_Concept_Write_Model §3.4·Option A 两阶段·2026-09-22 用户裁决）
//
// 职责：把「KnowledgePatched」事件翻成「ConfirmPatchMerged」命令，发给 KnowledgePatch 聚合。
// 纯搬运——策略语义全在 domain/policy/KnowledgePatchPolicies.PolicyOnConfirmPatchMerged（可单测）。
//
// 这是 Option A 闭环的**收口一跳**：
//   MergeKnowledgePatch → KnowledgePatchMergeCommitted（提交目录应用·**非终态**）
//     → [本策略前置] PolicyOnApplyKnowledgePatch → ApplyKnowledgePatch → KnowledgePatched
//       → 本 handler → ConfirmPatchMerged → KnowledgePatchMerged（**此刻才真 MERGED**）
//   修的是「合并第 1 步就自称完成」的谎报：catalog 是权威，它说应用成功了才算数。
//
// ⚠️ `patchId` 取自**事件载荷**而非策略 Params（实测 2026-09-22）：
//   生成器把命令的 Params VO 定为「**被该命令规则/发射引用**的参数 ＋ triggeredBy」，而
//   ConfirmPatchMerged 的发射从 `state.patchId` 取自身身份（这本身是对的——聚合知道自己是谁），
//   于是 patchId 不入 Params VO，策略 `on()` 根本拿不到它，无法凭 Params 构造命令。
//   handler 手上有事件载荷（KnowledgePatched.patchId 就在其中），直接取用——
//   与本文件同族 handler「sharding key 取自事件载荷」同一手法。
//   （同现象：`MergeKnowledgePatchParams` 亦只有 triggeredBy。**登记为生成器缺口**，
//    正解应是把策略 `info.bindings` 引用的命令参数一并补进 Params VO，与 ④层 emission
//    bindings 的 G-4a 补给同构；未做前各 handler 各自回退取事件值。）
//
// 失败语义（同 PolicyOnCreateCatalogVersion·用户裁决 2026-09-22）：Policy 必须对已发生的事实作出反应，
// 没有「拒绝」语义。做不完 → 返回 failed Future → akka-projection 不提交 offset → 重试（at-least-once）。
// 重试安全前提＝策略声明的 `idempotencyKey: command-level`（ConfirmPatchMerged 的状态门
// `MERGE_REQUESTED → MERGED` 本身即幂等屏障：第二次进来状态已是 MERGED，门直接拒绝且不改状态）。
package net.imadz.metarch.application.projection.policy

import akka.Done
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.util.Timeout
import net.imadz.metarch.application.aggregates.KnowledgePatchAggregate
import net.imadz.metarch.application.aggregates.KnowledgePatchProtocol.{ConfirmPatchMerged, ConfirmPatchMergedReply}
import net.imadz.metarch.domain.policy.KnowledgePatchPolicies.PolicyOnConfirmPatchMerged
import net.imadz.metarch.infrastructure.persistence.converters.ModelCatalogProtoConverters.KnowledgePatchedConv
import net.imadz.metarch.infrastructure.proto.model_catalog.ModelCatalogEventPO

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * KnowledgePatched → ConfirmPatchMerged。
 *
 * @param sharding 目标聚合（KnowledgePatch）的 cluster sharding 句柄
 */
class PolicyOnConfirmPatchMergedOnPatchedHandler(sharding: ClusterSharding)(implicit ec: ExecutionContext)
    extends Handler[EventEnvelope[ModelCatalogEventPO.Event]] {

  private implicit val askTimeout: Timeout = Timeout(10.seconds)

  override def process(envelope: EventEnvelope[ModelCatalogEventPO.Event]): Future[Done] =
    envelope.event match {
      case ModelCatalogEventPO.Event.KnowledgePatched(po) =>
        val e = KnowledgePatchedConv.fromProto(po)
        val params = PolicyOnConfirmPatchMerged.on(e)
        Future
          .traverse(params) { p =>
            // sharding key ＝ 事件载荷里的 patchId（catalog 刚应用的那个补丁＝要回执的那个）
            sharding
              .entityRefFor(KnowledgePatchAggregate.entityTypeKey, e.patchId.toString)
              .ask[ConfirmPatchMergedReply](ConfirmPatchMerged(e.patchId, p.triggeredBy, _))
              .flatMap {
                case ConfirmPatchMergedReply(None) => Future.successful(Done)
                case ConfirmPatchMergedReply(Some(err)) if err.code == "KP_NOT_MERGE_REQUESTED" =>
                  // 终态幂等逃生门（bug012-escalate 批·毒流根修 2026-10-01）：
                  // KP_NOT_MERGE_REQUESTED＝聚合不在 MERGE_REQUESTED——在本策略场景只可能是**已 MERGED**
                  // （confirm 首轮已成功落终态）。重试不可能改变结果（状态机单向 MERGE_REQUESTED→MERGED
                  // 无回路），按失败无限重试反而毒死整条流（实测：seq755 首轮成功后，watchdog heal 重放
                  // 吐出重复 KnowledgePatched(seq773)→333 次失败死循环）。目标已达成＝成功。
                  Future.successful(Done)
                case ConfirmPatchMergedReply(Some(err)) =>
                  // 失败传播 → offset 不提交 → 重试（契约：命令幂等）
                  Future.failed(new RuntimeException(
                    s"[policy] 目录应用后确认补丁合并失败：patchId=${e.patchId} err=${err.code}"))
              }
          }
          .map(_ => Done)
      case _ => Future.successful(Done) // 未知事件优雅跳过（勿打爆投影流）
    }
}
