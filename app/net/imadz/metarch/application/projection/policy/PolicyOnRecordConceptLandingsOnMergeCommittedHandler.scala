// MANUAL（回锚落点批·2026-09-26·用户裁决「甲」）：概念回锚的执行侧投影
//
// 职责：把「KnowledgePatchMergeCommitted」事件翻成 N 条「RecordConceptLandings」命令，
// 各发给**对应的** Concept 实体。纯搬运——策略语义全在
// domain/policy/ConceptPolicies.PolicyOnRecordConceptLandings（可单测）。
//
// ★ 与创建腿「概念增量后确立概念」的分职（**不要合并两条策略**）：
//   创建腿 = KnowledgePatched → EstablishConcept（只取 action=insert·那一刻 AST 还没导出、不可能有落点）；
//   回锚腿 = KnowledgePatchMergeCommitted → RecordConceptLandings（只取 action=update 且 anchors 非空）。
//   锚是**生成期发现**的代码落点（锚＝知识图与代码图的桥梁知识），HTTP 调用方手上没有它，
//   故只能由策略从补丁增量里搬运。原「给 EstablishConcept 加 anchors」方案已废——时刻不对。
//
// ★ 分片键 = `p.conceptId`（策略 Params 携带）——与 PolicyOnEstablishConcept 同款真扇出：
//   一个事件带 List[ConceptDelta]，每条增量对应**不同**的 Concept 实体，故不能取事件标量当键。
//   Params 里有 conceptId 是生成器 2026-09-22 的「策略引用参数补进 Params VO」机制在起作用。
//
// 失败语义（同族一致）：Policy 对已发生的事实作反应，无「拒绝」语义。做不完 → failed Future →
//   akka-projection 不提交 offset → 重试（at-least-once）。⚠️ 与创建腿同处：RecordConceptLandings
//   自身**无重复守卫**（只有 initialized 白名单门 + OPERATOR_REQUIRED），同键重复回锚会重发事件并
//   递增版本（幂等由「同一份锚反复回写＝同值」在语义上兜底，但版本号会走）。这是已知语义边界。
//
// ★ DLQ 吞毒（挂账 L6 落地·2026-09-29）：AGG_NOT_INITIALIZED＝对**非聚合宿主**（ConceptEdge/
//   ContextRelationNode 等投影内部标签被当概念回锚——09-26 白名单修复前的存量毒事件）回锚的
//   永久失败，重试不可愈且会把整条 KnowledgePatchTag0 流卡死在固定 offset。修法＝
//   KnInstanceProjectionHandler 同款：落 kn_projection_dlq 留痕后吞掉返回 Done，offset 推进。
package net.imadz.metarch.application.projection.policy

import akka.Done
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import akka.util.Timeout
import net.imadz.metarch.application.aggregates.ConceptAggregate
import net.imadz.metarch.application.aggregates.ConceptProtocol.{RecordConceptLandings, RecordConceptLandingsReply}
import net.imadz.metarch.domain.policy.ConceptPolicies.PolicyOnRecordConceptLandings
import net.imadz.metarch.infrastructure.persistence.converters.KnowledgePatchProtoConverters.KnowledgePatchMergeCommittedConv
import net.imadz.metarch.infrastructure.proto.knowledge_patch.KnowledgePatchEventPO
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * KnowledgePatchMergeCommitted → N × RecordConceptLandings（每条带锚的 update 增量一条）。
 *
 * @param sharding 目标聚合（Concept）的 cluster sharding 句柄
 */
class PolicyOnRecordConceptLandingsOnMergeCommittedHandler(sharding: ClusterSharding)(implicit ec: ExecutionContext)
    extends Handler[EventEnvelope[KnowledgePatchEventPO.Event]] {

  private implicit val askTimeout: Timeout = Timeout(10.seconds)
  private val log = LoggerFactory.getLogger(getClass)

  /** DLQ 吞毒（挂账 L6·2026-09-29）：永久性回执失败落 kn_projection_dlq 留痕后吞掉——offset 推进，tag 不堵。幂等（event_id 主键）。 */
  private def dlqAndSwallow(envelope: EventEnvelope[KnowledgePatchEventPO.Event], what: String): Future[Done] = {
    val eventId = s"${envelope.persistenceId}|${envelope.sequenceNr}"
    log.warn(s"[policy] 投影毒丸转 DLQ（record_concept_landings·$what·offset 推进不堵流）: $eventId")
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
        val params = PolicyOnRecordConceptLandings.on(e)
        Future
          .traverse(params) { p =>
            // 分片键 ＝ 本条增量自己的 conceptId（扇出件——每条对应不同 Concept 实体）
            sharding
              .entityRefFor(ConceptAggregate.entityTypeKey, p.conceptId.toString)
              .ask[RecordConceptLandingsReply](
                RecordConceptLandings(p.conceptId, p.anchors, p.triggeredBy, p.guards, p.aggregateRuleRefs, p.preconditionRefs, p.postconditionRefs, p.fields, p.emits, p.folds, p.emissions, p.steps, p.extensions, p.ucWrites, p.ucPublishes, _))
              .flatMap {
                case RecordConceptLandingsReply(None) => Future.successful(Done)
                case RecordConceptLandingsReply(Some(err)) =>
                  // 挂账 L6（2026-09-29）：非聚合宿主回锚＝永久失败（重试不可愈）→ DLQ 留痕吞掉；
                  // 其余失败维持原「失败传播 → offset 不提交 → 重试」语义（at-least-once）
                  if (err.code == "AGG_NOT_INITIALIZED")
                    dlqAndSwallow(envelope, s"概念回锚失败：conceptId=${p.conceptId} err=${err.code}")
                  else
                    Future.failed(new RuntimeException(
                      s"[policy] 概念回锚失败：conceptId=${p.conceptId} err=${err.code}"))
              }
          }
          .map(_ => Done)
      case _ => Future.successful(Done) // 未知事件优雅跳过（勿打爆投影流）
    }
}
