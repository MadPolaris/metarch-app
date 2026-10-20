package controllers.vo

import play.api.libs.json.{Json, Reads}

/**
 * Concept 命令 Req VO（EP_S2_9c·M2 spike：手写参考实现，gen_api_layer 模板批吸收后由生成器直出）。
 * VO 与领域命令解耦：不含 conceptId（来自路径）/tenantId/triggeredBy（来自 TenantSession）/replyTo（组合子注入）。
 * 可选字段 Option 化保持 asOpt 宽容语义（对齐 CommandParamValidationSpec 既有契约）。
 */
case class EstablishConceptReq(contextId: String, name: String,
                               kind: Option[String] = None, description: Option[String] = None) {
  def descriptionOrEmpty: String = description.getOrElse("")
}

object EstablishConceptReq {
  implicit val reads: Reads[EstablishConceptReq] = Json.reads[EstablishConceptReq]
}

case class ParkConceptReq(reason: String)

object ParkConceptReq {
  implicit val reads: Reads[ParkConceptReq] = Json.reads[ParkConceptReq]
}

case class DeprecateConceptReq(reason: String)

object DeprecateConceptReq {
  implicit val reads: Reads[DeprecateConceptReq] = Json.reads[DeprecateConceptReq]
}

case class MergeConceptsReq(sourceConceptId: String, targetConceptId: String)

object MergeConceptsReq {
  implicit val reads: Reads[MergeConceptsReq] = Json.reads[MergeConceptsReq]
}

case class RemoveConceptReq(reason: String)

object RemoveConceptReq {
  implicit val reads: Reads[RemoveConceptReq] = Json.reads[RemoveConceptReq]
}

case class ResolveConflictReq(resolution: String)

object ResolveConflictReq {
  implicit val reads: Reads[ResolveConflictReq] = Json.reads[ResolveConflictReq]
}

case class UpdateConceptReq(description: String, kind: String, machineSpec: String, metadata: String)

object UpdateConceptReq {
  implicit val reads: Reads[UpdateConceptReq] = Json.reads[UpdateConceptReq]
}

/** 无载荷命令（unpark 等）：宽容接受任意/空 body，保持既有行为 */
case class EmptyReq()

object EmptyReq {
  implicit val reads: Reads[EmptyReq] = new Reads[EmptyReq] {
    def reads(json: play.api.libs.json.JsValue): play.api.libs.json.JsResult[EmptyReq] =
      play.api.libs.json.JsSuccess(EmptyReq())
  }
}
