package net.imadz.metarch.application

import javax.inject.{Inject, Singleton}
import controllers.CaptureNoteReq
import net.imadz.metarch.application.projection.repository.{CaptureNoteDecisionViewRepository, OpenSessionDecisionViewRepository}
import net.imadz.metarch.application.projection.repository.CaptureNoteDecisionViewTable.CaptureNoteDecisionView
import net.imadz.common.auth.TenantCtx
import play.api.libs.json.Json

import scala.concurrent.{ExecutionContext, Future}

/** NoteParamEnricher：SYSTEM_QUERIED 参数装配服务（S2-89 WP-A 洞③·2026-09-14·手写应用服务——RegisterUserFlow 同族 MANUAL 实写面）。
 *
 *  冲突判定规则 v0.1（EP_S2_89 §1·用户口述→形式化·R1-R6）查询侧实现：
 *    R1  type ∈ {role, externalSystem, question, timer} 豁免（洞④裁决 2026-09-13：question/timer 同名跨 session 常见）
 *    R2  field/story/readModel 型天然键含 info.parent——同 parent 才进比较
 *    R3  同 type＋同 name 且非同一逻辑便签 → 冲突（3a 双方 knowledgeId 不同；3b 任一方无 knowledgeId）；
 *        3c 同 knowledgeId＋noteId 不同＝替身（非冲突·可合并）——剔除
 *    R4  无 knowledgeId 便签与知识库概念同名 → 冲突——【决策债】试点批未实现（需 Concept 视图查询面·EP 在册）
 *    R5  removedAt 墓碑不参与（软删除令）
 *    洞⑤ name 归一＝trim＋大小写敏感
 *
 *  scope＝workshop 范畴（洞②：sessionId → OpenSessionDecisionView 反查 workshopId → 同 workshop 各 session 兄弟便签）。
 *  聚合边界（洞③原委）：CaptureNote 建新 Note 实体看不见兄弟便签——跨实体查询本质是读模型职责，
 *  应用层 enricher 同步装配（竞态＝尽力语义·EP 在册）。
 */
final case class EnrichedCaptureNote(conflictingNoteIds: List[String] = Nil)

@Singleton
class NoteParamEnricher @Inject() (
  openSessionDecisionViewRepository: OpenSessionDecisionViewRepository,
  captureNoteDecisionViewRepository: CaptureNoteDecisionViewRepository,
)(implicit ec: ExecutionContext) {

  /** R1 豁免型（洞④裁决：与 role/externalSystem 同列）。 */
  private val ExemptTypes = Set("ROLE", "EXTERNAL_SYSTEM", "QUESTION", "TIMER")
  /** R2 天然键含 info.parent 的型（同 parent 才进比较）。 */
  private val ParentKeyedTypes = Set("FIELD", "STORY", "READ_MODEL")

  /** CaptureNote 的 conflictingNoteIds 装配（emitWhen cond 判据——空清单＝无条件分支·单事件）。 */
  def enrichCaptureNote(ts: TenantCtx, req: CaptureNoteReq): Future[EnrichedCaptureNote] = {
    if (ExemptTypes.contains(Option(req.`type`).map(_.toUpperCase).getOrElse(""))) return Future.successful(EnrichedCaptureNote())
    openSessionDecisionViewRepository.findBySessionId(req.sessionId).flatMap { sessions =>
      val workshopIdOpt = sessions.find(s => s.tenantId == ts.tenantId).map(_.workshopId)
      workshopIdOpt match {
        case None => Future.successful(EnrichedCaptureNote())
        case Some(workshopId) =>
          openSessionDecisionViewRepository.findByWorkshopId(workshopId).flatMap { wsSessions =>
            val siblingFutures = wsSessions
              .filter(s => s.tenantId == ts.tenantId)
              .map(s => captureNoteDecisionViewRepository.findBySessionId(s.sessionId))
            Future.sequence(siblingFutures).map { noteGroups =>
              val conflicts = noteGroups.flatten.filter(isConflict(ts, req, _)).map(_.noteId)
              EnrichedCaptureNote(conflicts.distinct)
            }
          }
      }
    }
  }

  private def isConflict(ts: TenantCtx, req: CaptureNoteReq, row: CaptureNoteDecisionView): Boolean = {
    row.removedAt.isEmpty &&                                        // R5 墓碑不参与（Option[Instant]·None==null 惯用法恒 false 退役）
      row.tenantId == ts.tenantId &&
      row.noteId != derivedNoteId(ts, req) &&                       // ★自排除（2026-09-26）：同 (tenant,sourceSystem,externalId) 派生同 id ⇒ 重同步的「自己」非冲突；否则每次重同步自撞 CollisionDetected
      row.name.trim == Option(req.name).getOrElse("").trim &&       // 洞⑤ trim＋大小写敏感
      row.`type` == Option(req.`type`).map(_.toUpperCase).getOrElse("") &&
      !ExemptTypes.contains(row.`type`) &&                          // R1
      (!ParentKeyedTypes.contains(row.`type`) || parentOf(row.info) == parentOf(req.info)) && // R2
      !isStunt(req, row)                                            // R3c 同 knowledgeId＝替身非冲突
  }

  /** 外部系统捕获的派生实体 id（与 NoteApiController 生成物同式逐字节：
    *   `UUID.nameUUIDFromBytes((tenantId + ":" + sourceSystem + ":" + externalId))`）。
    *   写侧结构唯一＝同键同实体（builder LANE_A_ADDRESSING.Note.CaptureNote r2c）。 */
  private def derivedNoteId(ts: TenantCtx, req: CaptureNoteReq): String =
    java.util.UUID.nameUUIDFromBytes(
      (ts.tenantId + ":" + Option(req.sourceSystem).getOrElse("") + ":" + Option(req.externalId).getOrElse("")).getBytes("UTF-8")
    ).toString


  /** R3c：新便签带 knowledgeId 且与兄弟一致 → 替身（非冲突）。 */
  private def isStunt(req: CaptureNoteReq, row: CaptureNoteDecisionView): Boolean = {
    val kid = Option(req.knowledgeId).getOrElse("")
    kid.nonEmpty && kid == row.knowledgeId
  }

  private def parentOf(info: String): String =
    try (Json.parse(Option(info).getOrElse("{}")) \ "parent").asOpt[String].getOrElse("")
    catch { case _: Exception => "" }
}
