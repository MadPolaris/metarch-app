// MANUAL（EP 行为层锚 · 2026-09-21）：内容指纹领域服务
//
// 规格真源：backend-codegen/knowledge_base/functions/functions.yaml
//   ‣ ComputePatchHash        — PR≈commit id（知识补丁内容指纹·**生成事件**方向）
//   ‣ ComputeCatalogStateHash — 目录快照≈tree SHA（**折叠状态**方向）
//
// 纯函数·无副作用·无 I/O（functions.yaml `io: {reads:[], writes:[]}`）。
//   两个方向分家（用户裁决 2026-09-21）：
//     • patchHash 由命令输入算**事件字段**        → 挂 Rules.SubmitKnowledgePatchRule（生成事件）
//     • stateHash 由事件载荷（manifest）算**状态字段** → 挂 EventHandler 的 MetadataAssembled 折叠
//
// 分层：**domain service**。域层零 infra 依赖（Akka/Play-JSON 值类型/持久化），但 JDK stdlib
//   （java.time.Instant 先例）＋ canonical 串化（play-json 在 domain.values 已有先例）可用。
//
// 口径冻结（与 tools/truth_recon.mjs / functions/impl/*.mjs 逐字同构）：canonical JSON =
//   key 字典序 · 无空白 · 数组保序；sha256 hex。**改口径＝全链位移**（三侧须同时改）。
package net.imadz.metarch.domain.service

import java.security.MessageDigest
import play.api.libs.json._

/** 内容指纹领域服务（canonical JSON + sha256 + 两个内容指纹）。 */
object ContentFingerprint {

  // ── canonical JSON（协议冻结 · 与 truth_recon.mjs / impl/*.mjs 镜像）──

  /** 递归重排 JsObject 键为字典序（JsArray/叶子保序）。 */
  private[service] def canonicalize(v: JsValue): JsValue = v match {
    case JsObject(fields) => JsObject(fields.toSeq.sortBy(_._1)(Ordering.String).map { case (k, vv) => k -> canonicalize(vv) })
    case JsArray(items)   => JsArray(items.map(canonicalize))
    case other            => other
  }

  /** canonical compact 串（无空白·与 JS JSON.stringify(canonicalize(v)) 同口径）。 */
  def canonicalJson(v: JsValue): String = Json.stringify(canonicalize(v))

  /** sha256 hex（canonical 串的 UTF-8 字节）。 */
  def sha256Hex(s: String): String =
    MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
      .map(b => String.format("%02x", java.lang.Byte.valueOf(b))).mkString

  // ── ComputePatchHash（PR≈commit id · 生成事件方向）────────────────────────

  /**
   * 知识补丁内容指纹。
   * 输入：concepts / relations（JSON 数组）· parentPatchId · parentPatchHash。
   *   序是内容（deltas 保序·不排序）。
   *   **两个父项都入账**（用户裁决 2026-09-22·对齐 git commit hash）：
   *     ‣ parentPatchId  ＝ stacked PR 语义（基于哪个补丁）——**可读、可回溯**
   *     ‣ parentPatchHash ＝ **父指纹链**——改祖先内容 → 后代 hash 全变（git 的篡改可检性质）
   *   ⚠️ 只熔 id 不熔 hash，则改祖先内容**子 hash 不变、检不出**——那是「id 链」不是「哈希链」。
   * @return sha256 hex
   */
  def computePatchHash(concepts: JsArray, relations: JsArray, parentPatchId: String, parentPatchHash: String): String =
    sha256Hex(canonicalJson(JsArray(
      concepts.value.toSeq ++ relations.value.toSeq ++ Seq(JsString(Option(parentPatchId).getOrElse("")), JsString(Option(parentPatchHash).getOrElse(""))))))

  /** 域类型原生重载（事件的 concepts/relations 是 List[ConceptDelta]/List[RelationDelta]——
    * 生成侧绑定表达式可直接调用本重载，无需在 expr 里做 JsArray 转换）。 */
  def computePatchHash(
      concepts: scala.collection.immutable.List[net.imadz.metarch.domain.values.ConceptDelta],
      relations: scala.collection.immutable.List[net.imadz.metarch.domain.values.RelationDelta],
      parentPatchId: String,
      parentPatchHash: String
  ): String =
    computePatchHash(
      JsArray(concepts.map(conceptDeltaToJson)),
      JsArray(relations.map(relationDeltaToJson)),
      parentPatchId,
      parentPatchHash
    )

  /** ConceptDelta → JsValue（比照其 jsonFormat 的字段集与序）。 */
  private def conceptDeltaToJson(d: net.imadz.metarch.domain.values.ConceptDelta): JsValue = JsObject(Seq(
    "action" -> JsString(d.action),
    "conceptName" -> JsString(d.conceptName),
    "contextId" -> JsString(d.contextId),
    "noteIds" -> JsArray(Option(d.noteIds).map(_.map(JsString(_))).getOrElse(Nil)),
    "definition" -> JsString(d.definition)
  ))

  /** RelationDelta → JsValue（比照其 jsonFormat 的字段集与序）。 */
  private def relationDeltaToJson(d: net.imadz.metarch.domain.values.RelationDelta): JsValue = JsObject(Seq(
    "action" -> JsString(d.action),
    "relationName" -> JsString(d.relationName),
    "sourceConceptName" -> JsString(d.sourceConceptName),
    "targetConceptName" -> JsString(d.targetConceptName),
    "mapping" -> JsString(d.mapping)
  ))

  // ── ComputeCatalogStateHash（目录快照≈tree SHA · 折叠状态方向）────────────

  /**
   * 目录版本状态指纹。
   * 输入：manifest（MetadataAssembled 事件载荷·组装内容·JSON 文本）＋ parentStateHash（父快照指纹）。
   * **幂等**（首要要求）：先 parse 再 canonical 重排——键序/空白是表示不是内容，同语义必同指纹。
   * 关键词：折叠必须只吃**事件里有的东西**＋**折叠的 base**——故输入是 manifest（事件字段）
   *   与 state.stateHash（上一版指纹），不是 noteId→hash 映射（那是导出物，折叠不出来）。
   * **父入账**（用户裁决 2026-09-22·对齐 git）：commit hash 熔「tree hash + 父 commit hash」——
   *   故 stateHash 熔「manifest + parentStateHash」。只熔 manifest 则**检不出「祖先内容变了、
   *   当前全图本该不同」**——而那正是 git 安全性的来源。
   * @return sha256 hex
   */
  def computeCatalogStateHash(manifest: String, parentStateHash: String): String = {
    val text = Option(manifest).getOrElse("")
    val parsed = try Json.parse(text) catch { case _: Exception => JsString(text) }
    sha256Hex(canonicalJson(JsArray(Seq(parsed, JsString(Option(parentStateHash).getOrElse(""))))))
  }

  // ── 按规格 id 调度（用例生成器翻 ScalaTest 时用）──────────────────────────

  /**
   * 按 Function 规格 id + given JSON 调对应实现，返回 then.output 键值。
   * 生成器由 cases 翻 ScalaTest 时调用：invoke("ComputePatchHash", Json.parse(givenJson))。
   */
  def invoke(functionId: String, given: JsValue): Map[String, String] = functionId match {
    case "ComputePatchHash" =>
      val concepts = (given \ "concepts").asOpt[JsArray].getOrElse(JsArray())
      val relations = (given \ "relations").asOpt[JsArray].getOrElse(JsArray())
      val parent = (given \ "parentPatchId").asOpt[JsString].map(_.value).getOrElse("")
      val parentHash = (given \ "parentPatchHash").asOpt[JsString].map(_.value).getOrElse("")
      Map("patchHash" -> computePatchHash(concepts, relations, parent, parentHash))

    case "ComputeCatalogStateHash" =>
      val manifest = (given \ "manifest").asOpt[JsString].map(_.value).getOrElse("")
      val parentHash = (given \ "parentStateHash").asOpt[JsString].map(_.value).getOrElse("")
      Map("stateHash" -> computeCatalogStateHash(manifest, parentHash))

    case other => throw new IllegalArgumentException(s"未注册的 Function 规格：$other（补 functions.yaml + 本节 + 生成器模板）")
  }
}
