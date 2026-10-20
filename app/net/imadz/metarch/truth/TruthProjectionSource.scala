// MANUAL (S2-82-B4b · 2026-09-11 · s282 §11)：真源键投影重建——纯函数 rebuilder 族。
// 输入＝投影 DecisionView 行（MySQL 读侧）＋TSP payloadJson（包装/裁决面）；输出＝真源文件形状的 JsValue。
// 逆映射规格＝本仓 tools/full-load.mjs P7-P12 灌入映射（rebuild 是其逆函数），
// 对账门＝tools/s77/truth_projection_gate.mjs（服务重建字节 vs 文件字节 canonical diff=0）。
//
// 排序规则（与 tools/s77/b4b_normalize_truth.mjs 镜像·改动须双侧同步）：
//   - 排序：aggregates/stateFields/valueObjects（按 name）；readModels（按 viewName）；
//           edges（按 command,event）；roles（按 name）；roles[].commands 排序去重；
//           commandRoleMap 值＝角色排序去重逗号 join。
//   - 保持存储序（DB 行内 JSON 列灌入时按文件序保存·稳定·语义序不宜重排）：
//           commands/events/parameters/payloads/relations/VO members/RM fields/一切字符串数组。
package net.imadz.metarch.truth

import net.imadz.metarch.application.projection.repository.CemEdgeDecisionViewTable.CemEdgeDecisionView
import net.imadz.metarch.application.projection.repository.FieldModelDefinitionDecisionViewTable.FieldModelDefinitionDecisionView
import net.imadz.metarch.application.projection.repository.ReadModelDefinitionDecisionViewTable.ReadModelDefinitionDecisionView
import net.imadz.metarch.application.projection.repository.RoleCommandBindingDecisionViewTable.RoleCommandBindingDecisionView
import net.imadz.metarch.application.projection.repository.ValueObjectDefinitionDecisionViewTable.ValueObjectDefinitionDecisionView
import net.imadz.metarch.truth.TruthCanonicalJson.obj

import com.google.inject.ImplementedBy
import play.api.libs.json._

import java.time.Instant
import scala.concurrent.Future

/** 投影真源读路径：按真源键重建文件形状 JSON。
  * 未知键 → Future.failed(IllegalArgumentException)；数据缺位/不一致 → Future.failed（fail-loud·不静默猜）。
  */
@ImplementedBy(classOf[TruthProjectionRebuilder])
trait TruthProjectionSource {
  def rebuild(key: String): Future[JsValue]
}

/** 墓碑判定共用（2026-10-06·Instant→Option[Instant] 涟漪适配·TruthProjectionRebuildSpec 五例回归红总根因）：
  * removedAt/retiredAt 三态容忍——null（fixture 直构）／None（生产读侧常态）／Some(null)（fixture
  * Some 包 null 惯用法）均视为活行；仅 Some(非 null)＝真墓碑（remove-note/FieldModelRemoved/retire-edge 软删）。
  * 旧 `== null` 惯用法对 Option 恒 false（None==null 不成立）→ 活行全被滤·重建面空输出。
  */
object TombstoneFilter {
  def isAlive(t: Option[Instant]): Boolean = !Option(t).exists(_.exists(_ != null))
}

object FieldsTemplateRebuild {

  /** {meta, aggregates, valueObjects} ← FMD 行 × VOD 行 × TSP meta payload。
    * meta 来自 TSP(fields-template) payload 的 .meta 键（B4a P12 灌入·verbatim）。
    * 墓碑行（removedAt 非空＝FieldModelRemoved 软删·E5-WP1 B1）不参与重建——
    *   行留档审计、真源文件缺该聚合（MetarchRebuild 便签墓碑/CemRebuild retiredAt 同族）。
    */
  def apply(aggs: List[FieldModelDefinitionDecisionView],
            vos: List[ValueObjectDefinitionDecisionView],
            meta: JsValue): JsObject = {
    val aggregates = aggs.filter(a => TombstoneFilter.isAlive(a.removedAt)).sortBy(_.aggregateName).map { a =>
      val identifier = obj(
        "name" -> JsString(a.identifierName),
        "dataType" -> JsString(a.identifierType),
        "description" -> JsString(a.identifierDescription),
        "origin" -> JsString(a.identifierOrigin),
      )
      obj(
        "name" -> JsString(a.aggregateName),
        "entity" -> (if (a.entityName != a.aggregateName && a.entityName.nonEmpty) JsString(a.entityName) else JsNull),
        "description" -> JsString(a.description),
        "tenantScoped" -> (if (a.tenantScoped) JsBoolean(true) else JsNull),
        "terminalStates" -> JsArray(a.terminalStates.map(JsString(_))),
        "identifier" -> identifier,
        "statusEnum" -> JsArray(a.statusEnumValues.map(JsString(_))),
        "stateFields" -> JsArray(a.stateFields.sortBy(_.fieldName).map { f =>
          obj(
            "name" -> JsString(f.fieldName),
            "dataType" -> JsString(f.dataType),
            "description" -> JsString(f.description),
            "uiLabel" -> JsString(f.uiLabel),
            "businessDef" -> JsString(f.businessDef),
            "derivation" -> JsString(f.derivation),
            "lineageHint" -> JsString(f.lineageHint),
            "parent" -> JsString(f.parent),
            "fold" -> JsString(f.fold),
            "source" -> JsString(f.source),
            "origin" -> JsString(f.origin),
            "role" -> JsString(f.role),
            "writtenBy" -> JsArray(f.writtenBy.map(JsString(_))),
            // EP_Field_Nature_And_Concern_Anchors：问题空间性质标记（可选）——None 经 obj() 的
            // falsy-elide 落键缺省，与真源文件「未标记字段无 nature 键」惯例等价（字节判据两侧同）
            "nature" -> f.nature.map(JsString(_)).getOrElse(JsNull),
          )
        }),
        "commands" -> JsArray(a.commands.map { c =>
          obj(
            "name" -> JsString(c.commandName),
            "description" -> JsString(c.description),
            "produces" -> JsArray(c.produces.map(JsString(_))),
            // 服务侧 List[String]（full-load 对象 JSON.stringify 直发）——parse 回对象数组
            "preconditions" -> JsArray(c.preconditions.map(parseStringifiedObject)),
            "parameters" -> JsArray(c.parameters.map(paramFace)),
          )
        }),
        "events" -> JsArray(a.events.map { e =>
          obj(
            "name" -> JsString(e.eventName),
            "description" -> JsString(e.description),
            "humanDescription" -> JsString(e.humanDescription),
            "origin" -> JsString(e.origin),
            "payloads" -> JsArray(e.payloads.map(paramFace)),
            // 行为元数据（2026-09-28 批＋挂账B 归位批）：bindings/forEach/foldAssignments——
            //   空数组经 obj() falsy-elide 落键缺省（同真源「无绑定事件无键」惯例·字节判据两侧同）
            "bindings" -> (if (e.bindings == null || e.bindings.isEmpty) JsNull
                           else JsArray(e.bindings.map(b => obj("slot" -> JsString(b.slot), "expr" -> JsString(b.expr))))),
            "forEach" -> e.forEach.filter(_.nonEmpty).map(JsString(_)).getOrElse(JsNull),
            "foldAssignments" -> (if (e.foldAssignments == null || e.foldAssignments.isEmpty) JsNull
                                  else JsArray(e.foldAssignments.map(b => obj("slot" -> JsString(b.slot), "expr" -> JsString(b.expr))))),
          )
        }),
        // 语义位例外：toState ''→JsNull 必须保留（事件触发无目标态）——不走 obj() elide
        "statusTransitions" -> (if (a.statusTransitions.isEmpty) JsNull
                                else JsObject(a.statusTransitions.map(t => t.eventName -> (if (t.toState.isEmpty) JsNull else JsString(t.toState))))),
        // 语义位例外：toState ''→JsNull 必须保留；fromStates 空数组/initial=false 由文件侧 elide 规约吸收
        "stateGates" -> (if (a.stateGates.isEmpty) JsNull
                         else JsObject(a.stateGates.map { g =>
                           g.commandName -> JsObject(
                             Option.when(g.fromStates.nonEmpty)("fromStates" -> JsArray(g.fromStates.map(JsString(_)))).toSeq ++
                               Seq("toState" -> (if (g.toState.isEmpty) JsNull else JsString(g.toState))) ++
                               Option.when(g.initial)("initial" -> JsBoolean(true)).toSeq
                           )
                         })),
        "relations" -> JsArray(a.relations.map { r =>
          obj(
            "to" -> JsString(r.toAggregate),
            "cardinality" -> JsString(r.cardinality),
            "foreignKey" -> JsString(r.foreignKey),
            "writePath" -> JsString(r.writePath),
            "ruling" -> JsString(r.ruling),
            "semanticType" -> JsString(r.semanticType),
            // 服务侧 JSON 串（对象 stringify 入库）——parse 回对象
            "causal" -> parseStringifiedObject(r.causal),
            "temporal" -> (if (r.temporal) JsBoolean(true) else JsNull),
            "existence" -> JsString(r.existence),
            "targetDeletionPolicy" -> JsString(r.targetDeletionPolicy),
            "targetTypes" -> JsArray(r.targetTypes.map(JsString(_))),
          )
        }),
      )
    }
    // S2-82-B3（软删除令）：VO 墓碑行（removedAt 非空＝ValueObjectRemoved）不参与重建——FMD 同族。
    val valueObjects = vos.filter(v => TombstoneFilter.isAlive(v.removedAt)).sortBy(_.voName).map { v =>
      obj(
        "name" -> JsString(v.voName),
        "fields" -> JsArray(v.members.map { m =>
          obj(
            "name" -> JsString(m.fieldName),
            "dataType" -> JsString(m.dataType),
            "description" -> JsString(m.description),
          )
        }),
      )
    }
    JsObject(Seq(
      "meta" -> meta,
      "aggregates" -> JsArray(aggregates),
      "valueObjects" -> JsArray(valueObjects),
    ))
  }

  /** 参数面（commands[].parameters 与 events[].payloads 同型·FieldParamSpec 11 键）。 */
  private def paramFace(p: net.imadz.metarch.domain.values.FieldParamSpec): JsObject = obj(
    "name" -> JsString(p.paramName),
    "dataType" -> JsString(p.dataType),
    "description" -> JsString(p.description),
    "uiLabel" -> JsString(p.uiLabel),
    "businessDef" -> JsString(p.businessDef),
    "origin" -> JsString(p.origin),
    "lineageHint" -> JsString(p.lineageHint),
    "derivation" -> JsString(p.derivation),
    "role" -> JsString(p.role),
    "optional" -> (if (p.optional) JsBoolean(true) else JsNull),
    "optionalWhen" -> JsString(p.optionalWhen),
  )

  /** 服务侧 stringify 串 → JsObject；空串/非对象 parse 失败 → JsNull（elide 吸收）。 */
  private[truth] def parseStringifiedObject(s: String): JsValue =
    if (s == null || s.trim.isEmpty) JsNull
    else
      Json.parse(s) match {
        case o: JsObject => o
        case _           => JsNull
      }
}

object ReadmodelsRebuild {

  /** {meta, stats, readModels} ← RMD 行 × TSP readmodels payload（meta/stats 两键）。 */
  def apply(views: List[ReadModelDefinitionDecisionView], meta: JsValue, stats: JsValue): JsObject = {
    // S2-82-B3（软删除令）：RMD 墓碑行（removedAt 非空＝ReadModelRemoved）不参与重建——FMD 同族。
    val readModels = views.filter(v => TombstoneFilter.isAlive(v.removedAt)).sortBy(_.viewName).map { v =>
      obj(
        "id" -> JsString(v.viewId),
        "name" -> JsString(v.viewName),
        "description" -> JsString(v.description),
        "sourceAggregate" -> JsString(v.parentAggregate),
        "sourceAggregates" -> JsArray(v.sourceAggregates.map(JsString(_))),
        "consumesEvents" -> JsArray(v.consumesEvents.map(JsString(_))),
        "fields" -> JsArray(v.fields.map { f =>
          obj(
            "name" -> JsString(f.fieldName),
            "dataType" -> JsString(f.dataType),
            "uiLabel" -> JsString(f.uiLabel),
            "source" -> JsString(f.source),
            "isKey" -> (if (f.isKey) JsBoolean(true) else JsNull),
            "origin" -> JsString(f.origin),
            "writtenBy" -> JsArray(f.writtenBy.map(JsString(_))),
          )
        }),
        "decisionSupport" -> JsArray(v.decisionCommands.map(JsString(_))),
        "linkedStories" -> JsArray(v.linkedStories.map(JsString(_))),
      )
    }
    JsObject(Seq(
      "meta" -> meta,
      "stats" -> stats,
      "readModels" -> JsArray(readModels),
    ))
  }
}

object CemRebuild {

  /** {schema..stats wrapper＋edgeNotes, edges} ← CEM 行 × TSP cem payload。
    * wrapper 六件 verbatim；edgeNotes（B4b：文件 11 处 edge.note 裁决注释——CEM 服务面无 note 列·归 payload
    * 承载）按 `command->event` 键补回 edge.note；edges 按 (command,event) 排序。 */
  def apply(edges: List[CemEdgeDecisionView], wrapper: JsObject): JsObject = {
    val edgeNotes: Map[String, String] = (wrapper \ "edgeNotes").getOrElse(JsNull) match {
      case JsObject(fields) => fields.collect { case (k, JsString(v)) => k -> v }.toMap
      case _                => Map.empty
    }
    val edgeArray = JsArray(edges.filter(e => TombstoneFilter.isAlive(e.retiredAt)).sortBy(e => (e.commandName, e.eventName)).map { e =>
      val note = edgeNotes.get(s"${e.commandName}->${e.eventName}")
      obj(
        "command" -> JsString(e.commandName),
        "event" -> JsString(e.eventName),
        "aggregate" -> JsString(e.aggregateName),
        "sources" -> Json.toJson(e.sources), // B 组装适配（Phase 2）: CemEdge.sources 已由 JSON 串重型为 List[String]，直出数组
        "confidence" -> JsString(e.confidence),
        "note" -> note.fold[JsValue](JsNull)(JsString(_)),
      )
    })
    // edgeNotes 是承载通道不是输出面——wrapper 透传时剔出（文件面无此键）
    val wrapperOut = JsObject(wrapper.fields.filterNot(_._1 == "edgeNotes"))
    JsObject(wrapperOut.fields :+ ("edges" -> edgeArray))
  }
}

object RoleCommandMapRebuild {

  /** {meta, rulings, roles, commandRoleMap, danglingResolution, coverage} ← RCB 行 × TSP rcm payload。
    *
    * payload 承载（B4a P12）：roles[{name,origin,ruling,description}]/rulings/danglingResolution/coverage/meta。
    * 数据面重建：roles[].commands ← 绑定行按角色分组（排序去重）；commandRoleMap ← 按命令分组
    * （角色排序去重逗号 join）。ruling 不入 roles 输出（文件 roles 键集＝name/description/origin/commands）。
    */
  def apply(bindings: List[RoleCommandBindingDecisionView], payload: JsObject): JsObject = {
    // E4-e（2026-09-12·CemRebuild retiredAt 同族）：unbind 投影语义＝status='unbound' 墓碑行留档
    //   （boundAt/unboundAt 审计链），有效绑定面＝非 unbound——不滤则解绑后重建把墓碑行当活绑定（gate 红）。
    val live = bindings.filter(_.status != "unbound")
    val byCommand: Map[String, List[String]] = live.groupBy(_.commandName).view.mapValues(_.map(_.roleName).distinct.sorted).toMap
    val byRole: Map[String, List[String]] = live.groupBy(_.roleName).view.mapValues(_.map(_.commandName).distinct.sorted).toMap

    val commandRoleMap = JsObject(byCommand.toSeq.sortBy(_._1).map { case (cmd, roles) => cmd -> JsString(roles.mkString(",")) })

    val payloadRoles: Seq[JsObject] = (payload \ "roles").getOrElse(JsNull) match {
      case JsArray(items) => items.collect { case o: JsObject => o }.toSeq
      case _              => Seq.empty
    }
    // fail-loud：每个绑定行的 roleName 必须在 payload roles 中（悬空绑定＝数据不一致·不猜）
    val payloadRoleNames = payloadRoles.flatMap(r => (r \ "name").asOpt[String]).toSet
    val dangling = byRole.keys.filterNot(payloadRoleNames.contains)
    if (dangling.nonEmpty)
      throw new IllegalStateException(s"rcm 重建：绑定行角色不在 TSP payload roles 中：${dangling.mkString(",")}")

    val roles = payloadRoles.sortBy(r => (r \ "name").asOpt[String].getOrElse("")).map { r =>
      val name = (r \ "name").asOpt[String].getOrElse("")
      obj(
        "name" -> JsString(name),
        "description" -> (r \ "description").getOrElse(JsNull),
        "origin" -> (r \ "origin").getOrElse(JsNull),
        "commands" -> JsArray(byRole.getOrElse(name, Nil).map(JsString(_))),
      )
    }

    // fail-loud：meta.systemCommandExemption.commands 与绑定行 systemCommand 标志集不一致＝数据不一致
    val exemptPayload: Seq[String] = (payload \ "meta" \ "systemCommandExemption" \ "commands").asOpt[Seq[String]].getOrElse(Seq.empty).sorted
    val exemptBindings = live.filter(_.systemCommand).map(_.commandName).distinct.sorted
    if (exemptPayload != exemptBindings)
      throw new IllegalStateException(s"rcm 重建：systemCommandExemption 不一致（payload ${exemptPayload.length} vs 绑定 ${exemptBindings.length}）")

    JsObject(Seq(
      "meta" -> (payload \ "meta").getOrElse(JsNull),
      "rulings" -> (payload \ "rulings").getOrElse(JsNull),
      "roles" -> JsArray(roles),
      "commandRoleMap" -> commandRoleMap,
      "danglingResolution" -> (payload \ "danglingResolution").getOrElse(JsNull),
      "coverage" -> (payload \ "coverage").getOrElse(JsNull),
    ))
  }
}
