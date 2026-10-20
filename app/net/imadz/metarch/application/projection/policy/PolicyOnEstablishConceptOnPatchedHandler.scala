// MANUAL（EP_Catalog_Concept_Write_Model §3.4.3 #4d·2026-09-22）：概念真落 Concept 聚合的执行侧
//
// 职责：把「KnowledgePatched」事件翻成 N 条「EstablishConcept」命令，各发给**对应的** Concept 实体。
// 纯搬运——策略语义全在 domain/policy/ConceptPolicies.PolicyOnEstablishConcept（可单测）。
//
// 这是写侧闭环的最后一跳：
//   KnowledgePatched（catalog 应用了补丁）
//     → 本 handler → 逐条（只取 action=insert 的增量）EstablishConcept
//     → Concept 聚合真出现这批概念
//   「聚合只发自己的事件」铁律下 catalog **编译期发不出** EstablishConcept，故必须走跨聚合策略。
//
// ★ 分片键 = `p.conceptId`（策略 Params 携带）——**不是**事件标量：
//   本策略是全仓首条**真扇出**件：一个事件带 List[ConceptDelta]，每条增量对应**不同**的 Concept
//   实体，故不能像同族 handler 那样从事件取一个标量当 sharding key，必须逐条用各自的 conceptId。
//   Params VO 之所以有 conceptId，是生成器 2026-09-22 的修补：策略 info.bindings 引用的命令参数
//   补进 Params VO（原先 identity 参数不入 Params——该缺口登记见 PolicyOnConfirmPatchMerged 头注，
//   本次一并关闭 #32 缺口①）。
//
// 失败语义（同族一致）：Policy 对已发生的事实作反应，无「拒绝」语义。做不完 → failed Future →
//   akka-projection 不提交 offset → 重试（at-least-once）。⚠️ 与同族不同处：EstablishConcept 自身
//   **无重复守卫**（只有 initialized 白名单门 + OPERATOR_REQUIRED），同键重复确立会重发
//   ConceptEstablished 并递增版本。幂等因此**不在本跳兜底**，而在上游语义：delta 的 action 只在
//   真新建时为 insert（判定归 ConceptDeltaResolver），策略据此过滤。这是已知语义边界，不是冗余。
package net.imadz.metarch.application.projection.policy

import akka.Done
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.util.Timeout
import net.imadz.metarch.application.aggregates.ConceptAggregate
import net.imadz.metarch.application.aggregates.ConceptProtocol.{EstablishConcept, EstablishConceptReply}
import net.imadz.metarch.domain.policy.ConceptPolicies.PolicyOnEstablishConcept
import net.imadz.metarch.infrastructure.persistence.converters.ModelCatalogProtoConverters.KnowledgePatchedConv
import net.imadz.metarch.infrastructure.proto.model_catalog.ModelCatalogEventPO

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * KnowledgePatched → N × EstablishConcept（每条 insert 增量一条）。
 *
 * @param sharding 目标聚合（Concept）的 cluster sharding 句柄
 */
class PolicyOnEstablishConceptOnPatchedHandler(sharding: ClusterSharding)(implicit ec: ExecutionContext)
    extends Handler[EventEnvelope[ModelCatalogEventPO.Event]] {

  private implicit val askTimeout: Timeout = Timeout(10.seconds)

  override def process(envelope: EventEnvelope[ModelCatalogEventPO.Event]): Future[Done] =
    envelope.event match {
      case ModelCatalogEventPO.Event.KnowledgePatched(po) =>
        val e = KnowledgePatchedConv.fromProto(po)
        val params = PolicyOnEstablishConcept.on(e)
        Future
          .traverse(params) { p =>
            // 分片键 ＝ 本条增量自己的 conceptId（扇出件——每条对应不同 Concept 实体）
            sharding
              .entityRefFor(ConceptAggregate.entityTypeKey, p.conceptId.toString)
              .ask[EstablishConceptReply](
                EstablishConcept(
                  p.conceptId, p.contextId, p.name, p.kind, p.description,
                  p.sourceSessionId, p.noteId, p.sourceStage, p.sourceNoteIds,
                  p.tenantId, p.triggeredBy, _))
              .flatMap {
                case EstablishConceptReply(None) => Future.successful(Done)
                case EstablishConceptReply(Some(err)) =>
                  // 失败传播 → offset 不提交 → 重试（契约：上游 action 保证只发真新建）
                  Future.failed(new RuntimeException(
                    s"[policy] 概念确立失败：conceptId=${p.conceptId} err=${err.code}"))
              }
          }
          .map(_ => Done)
      case _ => Future.successful(Done) // 未知事件优雅跳过（勿打爆投影流）
    }
}
