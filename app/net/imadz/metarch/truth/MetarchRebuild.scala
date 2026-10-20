// MANUAL (S2-82-B4c · 2026-09-11 · s282 §12)：metarch 真源键投影重建。
// 输入＝CaptureNote 便签行 × TSP 四 payload（skeleton/fieldmodel/behaviormodel/flowmodel）；
// 输出＝metarch.json 文件形状 {workshop, sessions[{session, scopes[{scope, notes, 三兄弟件}]}]}。
//
// 排序规则（与 tools/s77/b4c_normalize_truth.mjs 镜像·改动须双侧同步）：
//   - sessions 按 name 字典序；scope 内 notes 按「便签自身 canonical 串」排（内容基全序——
//     完全相同便签实测 0 组；声明序不可复刻·DB 行序不稳定）。
//   - 禁碰：便签内容八键映射面/三兄弟件内部（payload verbatim）。
//
// 便签八键映射（full-load P3 逆函数）：type←TYPE 逆向词表·info←JSON 串 parse·createdAt←sourceCreatedAt
//   列；falsy elide 双侧吸收（description ''/author ''/links 空数组）；info 空对象不 elide（协议只
//   elide null/false/''/空数组）——恒发。
package net.imadz.metarch.truth

import net.imadz.metarch.application.projection.repository.CaptureNoteDecisionViewTable.CaptureNoteDecisionView
import net.imadz.metarch.truth.TruthCanonicalJson.canonicalString
import net.imadz.metarch.truth.TruthCanonicalJson.obj

import play.api.libs.json._

object MetarchRebuild {

  /** full-load TYPE_MAP 逆向（服务大写 → 文件小写）。 */
  private val TypeRev: Map[String, String] = Map(
    "COMMAND" -> "command", "EVENT" -> "event", "ROLE" -> "role", "EXTERNAL_SYSTEM" -> "externalSystem",
    "POLICY" -> "policy", "TIMER" -> "timer", "AGGREGATE" -> "aggregate", "SUBDOMAIN" -> "subdomain",
    "CONTEXT" -> "context", "STORY" -> "story", "READ_MODEL" -> "readModel", "QUESTION" -> "question",
    "CONTEXT_RELATION" -> "contextRelation", "INVARIANT" -> "invariant", "PROCESS_MANAGER" -> "processManager",
    "FIELD" -> "field", "RELATION" -> "relation",
  )

  /** 单便签重建（八键·elide 帮手吸收 falsy）。 */
  private def noteOf(r: CaptureNoteDecisionView): JsObject = obj(
    "type" -> JsString(TypeRev.getOrElse(r.`type`, r.`type`)),
    "name" -> JsString(r.name),
    "stage" -> JsString(r.stage),
    "description" -> JsString(r.description),
    "info" -> TruthCanonicalJson.elideDeep(if (r.info == null || r.info.trim.isEmpty) JsObject(Seq.empty) else Json.parse(r.info)),
    "links" -> JsArray(r.links.map(JsString(_))),
    "author" -> JsString(r.author),
    "createdAt" -> JsString(r.sourceCreatedAt),
  )

  /** {workshop, sessions} ← 便签行（全租户）× TSP 四 payload。
    *
    * skeleton payload：{workshop, scope, modelsSession, sessions:[{name, sessionId}]}。
    * 三兄弟件 payload：仅 modelsSession 指名的 session 的 scope 挂载（refining）。
    * 墓碑行（removedAt 非空＝remove-note 软删）不参与重建——文件侧该便签已删（B5 note.remove 服务承载）。
    */
  def apply(notes: List[CaptureNoteDecisionView],
            skeleton: JsObject,
            fieldModel: JsValue,
            behaviorModel: JsValue,
            flowModel: JsValue): JsObject = {
    val workshop = (skeleton \ "workshop").getOrElse(JsNull)
    val scopeName = (skeleton \ "scope").asOpt[String].getOrElse("")
    val modelsSession = (skeleton \ "modelsSession").asOpt[String].getOrElse("refining")
    val bySessionId: Map[String, List[CaptureNoteDecisionView]] =
      notes.filter(n => TombstoneFilter.isAlive(n.removedAt)).groupBy(_.sessionId)

    def sessionEntry(name: String, sessionId: String): JsObject = {
      val rows = bySessionId.getOrElse(sessionId, Nil)
      val rebuilt = rows.map(noteOf)
      val sorted = rebuilt.sortBy(canonicalString) // 内容基全序·与 b4c_normalize 镜像
      val scope: JsObject = if (name == modelsSession)
        JsObject(Seq(
          "scope" -> JsString(scopeName),
          "notes" -> JsArray(sorted),
          "fieldModel" -> fieldModel,
          "behaviorModel" -> behaviorModel,
          "flowModel" -> flowModel,
        ))
      else
        JsObject(Seq(
          "scope" -> JsString(scopeName),
          "notes" -> JsArray(sorted),
        ))
      JsObject(Seq("session" -> JsString(name), "scopes" -> JsArray(Seq(scope))))
    }

    val sessionDefs: Seq[(String, String)] = (skeleton \ "sessions").getOrElse(JsNull) match {
      case JsArray(items) => items.toSeq.flatMap {
        case o: JsObject =>
          for { n <- (o \ "name").asOpt[String]; sid <- (o \ "sessionId").asOpt[String] } yield (n, sid)
        case _ => None
      }
      case _ => Seq.empty
    }
    // sessions 按 name 字典序（与文件规范化镜像）
    JsObject(Seq(
      "workshop" -> workshop,
      "sessions" -> JsArray(sessionDefs.sortBy(_._1).map { case (n, sid) => sessionEntry(n, sid) }),
    ))
  }
}
