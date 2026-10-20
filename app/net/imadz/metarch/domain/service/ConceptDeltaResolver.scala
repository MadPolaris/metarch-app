// MANUAL（EP_Catalog_Concept_Write_Model §3.4.0.2·2026-09-22）：概念增量判定领域服务
//
// 规格真源：backend-codegen/knowledge_base/functions/functions.yaml
//   ‣ ApplyConceptDeltas — 增量解析＋名字索引折叠（**折叠状态**方向·挂 EventHandler）
//   ‣ DetectConceptConflicts — 增量冲突检测（**生成事件**方向·挂 Rules.DetectCatalogConflictsRule）
//
// 纯函数·无副作用·无 I/O（functions.yaml `io: {reads:[], writes:[]}`）。**同一个 resolveAll
//   被两侧调用**（用户裁决 D9）：决策侧算一次（发冲突事件）、折叠侧再算一次（更新索引）——
//   同一纯函数两处调用，**逻辑不可能分叉**；replay 恒等价（纯函数＋同输入）。
//
// 入口形状（2026-09-22 用户裁决「甲」·改 typed）：**吃 `List[ConceptDelta]`**，不吃 JSON 串。
//   原版吃 `String (JSON)` 是因为 `ApplyKnowledgePatch.knowledgeDelta` 真源类型是 JSON 串，
//   而**该串从无生产者**——策略便签把它绑成 `literal:`（空串），于是整条判定链**空转**
//   （折叠 `applyDeltas("")` 恒无变化、冲突腿 `hasConflicts("")` 恒 false）。
//   真源改为 `concepts: List[ConceptDelta]` 后，typed 列表从
//   `KnowledgePatchMergeCommitted.concepts` 一路搬到此处，串通道随之拆除（`parseDeltas` 已删）。
//
// 判定规则（用户 2026-09-22 拍板·D8：铸号在**上游**）：唯一键 `(conceptName, contextId)` 先查库内既有
//   概念，再比 Id、比 hash。**七格**（⚠️ 初版表把「库内无＋增量有号」误判为冲突——D8 下上游对
//   **新建概念也铸号**，故那是正常新建。错版已废，以本表与代码体为准）：
//     库内无 + delta 无号            → 冲突 MISSING_ID（上游漏铸·下游不代铸）
//     库内无 + delta 有号 + 号挂别处  → 冲突 DUPLICATE_ID（一元素一 id 被破）
//     库内无 + delta 有号            → Insert（**正常新建**·号已由上游铸好）
//     库内有 + delta 无号            → 冲突 MISSING_ID（同名却无身份·分不清更新还是新建）
//     库内有 + 号异                  → 冲突 ID_MISMATCH（身份撞车）
//     库内有 + 号同 + hash 同        → Noop（幂等重放·静默跳过）
//     库内有 + 号同 + hash 异        → Update（**明确更新意图**）
//   hash 那两格是关键：光比 Id 分不出「真改了内容」与「同一份又提交一遍」——
//   前者该更新、后者该静默跳过（否则重放白白递增版本号，违反铁律 #2 读侧确定性折叠）。
//
// 分层：**domain service**。域层零 infra 依赖；`details` 组串用 play-json（domain.values 先例）。
// 铸号契约：本服务**不铸号**（D8 改判——铸在最上游时态）。上游没带号 → 判冲突，不代铸。
//
// ⚠️ MISSING_ID 在**当前接线**下仍不可达（改 typed 后实测复核·2026-09-22）：
//   载荷经 `ConceptDeltaConv.fromProto` 落地，其中 `IdConv.fromProto(s) = UUID.fromString(s)`
//   ——空串/非法串**直接抛 IllegalArgumentException**，根本到不了本判定层。
//   即「上游漏铸号」的实际表现是 **policy 处抛异常 → akka-projection 不提交 offset → 无限重试**
//   （毒消息），**不是**设计想要的「判冲突、交人裁决」。
//   故本分支保留为**契约防御**（供直接构造调用者：单测、未来的非 proto 生产者）；
//   「让漏号真能落成 MISSING_ID」＝已登记缺口（需 `ConceptDelta` 侧容忍缺失 id，涉共享转换器，另批）。
package net.imadz.metarch.domain.service

import play.api.libs.json._
import net.imadz.common.CommonTypes.Id
import net.imadz.metarch.domain.values.{CatalogConflict, ConceptDelta, ConceptIndexEntry}

/** 概念增量判定领域服务（唯一键 → Id → hash 三阶判定 ＋ 名字索引折叠）。 */
object ConceptDeltaResolver {

  // ── 判定结果（七格·见文件头）──────────────────────────────────────────────

  /** 一条 delta 的裁决。 */
  sealed trait Outcome
  /** 首次创建：库内无该键，上游已铸号。 */
  final case class Insert(conceptId: Id, conceptName: String, contextId: String, contentHash: String) extends Outcome
  /** 明确更新意图：键与 Id 都对上，内容指纹变了。 */
  final case class Update(conceptId: Id, conceptName: String, contextId: String, contentHash: String) extends Outcome
  /** 幂等重放：键、Id、指纹三者皆同——静默跳过（不递增版本·不打事件）。 */
  case object Noop extends Outcome
  /** 冲突：人裁决（铁律 #3 交集冲突化——不自动级联）。
    *  ⚠️ id 载体＝`Id`（与 `ConceptDelta`/`ConceptIndexEntry` 同型·`Id = java.util.UUID`）；
    *  「上游未铸号」在 Scala 侧表现为 `null`（见文件头 reachability 注）。 */
  final case class Conflict(conflictType: String, conceptName: String, contextId: String,
                            existingConceptId: Id, incomingConceptId: Id, details: String) extends Outcome

  /** 冲突类型词表（`CatalogConflict.conflictType`）。 */
  object ConflictType {
    val MissingId   = "MISSING_ID"   // 增量无号（上游未铸）——下游不代铸
    val IdMismatch  = "ID_MISMATCH"  // 同键异号——身份撞车
    val DuplicateId = "DUPLICATE_ID" // 同号异键——一个号挂了两个键（一元素一 id 被破）
  }

  /** 一次应用的全量裁决计划。 */
  final case class ResolutionPlan(outcomes: List[Outcome], conflicts: List[CatalogConflict], nextIndex: List[ConceptIndexEntry])

  /** 唯一键：`(conceptName, contextId)`（真源 ConceptDelta.conceptName 描述明写）。 */
  private def find(index: List[ConceptIndexEntry], name: String, ctx: String): Option[ConceptIndexEntry] =
    index.find(e => e != null && e.conceptName == name && e.contextId == ctx)

  /** 单条判定（七格·纯函数）。
    *  ⚠️ 铸号契约在**上游**（D8）：新概念**也**由上游铸号随 delta 带来——
    *  故「库内无键 ＋ 增量有号」是**正常新建**（Insert），**不是**冲突。 */
  def resolve(index: List[ConceptIndexEntry], d: ConceptDelta): Outcome = {
    val hasIncoming = d.conceptId != null
    find(index, d.conceptName, d.contextId) match {
      case None =>
        if (!hasIncoming)
          Conflict(ConflictType.MissingId, d.conceptName, d.contextId, null, null,
            "增量未携带 conceptId——铸号契约在**上游**（D8）；下游不代铸，判冲突待人裁决")
        else index.find(e => e != null && e.conceptId == d.conceptId) match {
          case Some(elsewhere) =>
            Conflict(ConflictType.DuplicateId, d.conceptName, d.contextId,
              elsewhere.conceptId, d.conceptId,
              s"号 ${d.conceptId} 已挂在键 ${elsewhere.conceptName}@${elsewhere.contextId} 下——一元素一 id 被破")
          case None => Insert(d.conceptId, d.conceptName, d.contextId, d.contentHash)
        }
      case Some(e) =>
        if (!hasIncoming)
          Conflict(ConflictType.MissingId, d.conceptName, d.contextId, e.conceptId, null,
            s"库内已有同键概念 ${e.conceptId}，增量却未携带 conceptId——无法判定是更新还是新建")
        else if (e.conceptId != d.conceptId)
          Conflict(ConflictType.IdMismatch, d.conceptName, d.contextId, e.conceptId, d.conceptId,
            s"同键异号：库内 ${e.conceptId}，增量 ${d.conceptId}——身份撞车")
        else if (e.contentHash == d.contentHash) Noop
        else Update(e.conceptId, d.conceptName, d.contextId, d.contentHash)
    }
  }

  /** 折叠：把裁决应用到名字索引。
    *   - Insert → 追加索引条目
    *   - Update → 更新该条目指纹
    *   - Noop   → 原样（幂等重放零 diff）
    *   - Conflict → **不落索引**（待裁决；冲突项不得污染权威索引）
    */
  private def foldIndex(index: List[ConceptIndexEntry], outcomes: List[Outcome]): List[ConceptIndexEntry] =
    outcomes.foldLeft(index) {
      case (acc, Insert(cid, nm, ctx, h)) => acc :+ ConceptIndexEntry(nm, ctx, cid, h)
      case (acc, Update(cid, nm, ctx, h)) => acc.map(e => if (e.conceptName == nm && e.contextId == ctx) ConceptIndexEntry(nm, ctx, cid, h) else e)
      case (acc, _)                       => acc
    }

  /** 冲突裁决 → `CatalogConflict` 值对象（塞进 `CatalogConflictSet`）。
    *  `details` 为 JSON 串（D6/D7：不加格·把「库内同键概念」的引用与增量号一并装进去）。 */
  private def toConflict(c: Conflict): CatalogConflict = {
    // Id → 串（人读面·`CatalogConflict` 无 id 字段，两个 id 装进 details）。
    // 抽成显式 val：`Json.obj` 是宏，内联 `Option(x).map(_.toString).getOrElse("")` 会被推断成 Object。
    val existingId: String = if (c.existingConceptId == null) "" else c.existingConceptId.toString
    val incomingId: String = if (c.incomingConceptId == null) "" else c.incomingConceptId.toString
    CatalogConflict(
      conflictId = java.util.UUID.randomUUID().toString,
      conflictType = c.conflictType,
      involvedSessionIds = Nil,
      details = Json.stringify(Json.obj(
        "conceptName" -> c.conceptName,
        "contextId" -> c.contextId,
        "existingConceptId" -> existingId,
        "incomingConceptId" -> incomingId,
        "reason" -> c.details)),
      resolvedAt = null, resolvedBy = "", resolutionAction = "", resolutionNote = "")
  }

  /** **同一纯函数**：两侧（决策/折叠）都走它。 */
  def resolveAll(index: List[ConceptIndexEntry], deltas: List[ConceptDelta]): ResolutionPlan = {
    val idx = Option(index).getOrElse(Nil).filter(_ != null)
    val ds  = Option(deltas).getOrElse(Nil).filter(_ != null)
    val outcomes = ds.map(d => resolve(idx, d))
    ResolutionPlan(
      outcomes = outcomes,
      conflicts = outcomes.collect { case c: Conflict => toConflict(c) },
      nextIndex = foldIndex(idx, outcomes))
  }

  // ── functions.yaml 两个 callExpr 的落点 ───────────────────────────────────
  //   注意参数序：函数表 signature.inputs 的序**决定** callExpr 的 {args} 序。

  /** 折叠方向（`fn:ApplyConceptDeltas`·挂 EventHandler 的 KnowledgePatched 折叠）。
    *  参数序 = functions.yaml inputs 序：`(deltas, conceptIndex)`。 */
  def applyDeltas(deltas: List[ConceptDelta], conceptIndex: List[ConceptIndexEntry]): List[ConceptIndexEntry] =
    resolveAll(conceptIndex, deltas).nextIndex

  /** 决策方向（`fn:DetectConceptConflicts`·挂 DetectCatalogConflictsRule）。
    *  参数序 = functions.yaml inputs 序：`(deltas, conceptIndex)`。 */
  def conflicts(deltas: List[ConceptDelta], conceptIndex: List[ConceptIndexEntry]): List[CatalogConflict] =
    resolveAll(conceptIndex, deltas).conflicts

  /** 便捷判定：本增量是否含冲突（冲突腿条件用）。 */
  def hasConflicts(deltas: List[ConceptDelta], conceptIndex: List[ConceptIndexEntry]): Boolean =
    conflicts(deltas, conceptIndex).nonEmpty
}
