package controllers.kb

import javax.inject.{Inject, Singleton}
import net.imadz.common.auth.AuthAction
import net.imadz.metarch.application.kb.{KnReasonService, KnReasonTemplate}
import play.api.libs.json.{JsError, JsSuccess, Json, Reads}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/**
 * M2 推理面 API（批7·kn_kb_storage_plan §5.2/§6.2）——/api/kb/reason 唯一出口。
 *
 *  - GET  /api/kb/reason           在册模板清单（名称/描述/参数 schema——发现面）
 *  - POST /api/kb/reason           {template, params:{...}} → 路径证据行集
 *
 * 白名单纪律：请求体只有 template/params 两键——**无 cypher 字段可传**（模板外 cypher
 * 一律拒绝＝设计即拒绝）；params 值经 driver 参数通道注入，零拼接。
 * 降级纪律：Neo4j 不可用 → degraded 标记（不 5xx）；空行集 → degraded=false＋
 * 「图谱中不存在该关系路径」——负断言是图的诚实答案（特性而非缺陷）。
 * 手写领地声明见 KnReasonStore 头注（挂 EP_S2_48 批1）。
 */
@Singleton
class KnReasonController @Inject() (
                                     authAction: AuthAction,
                                     reason: KnReasonService,
                                     val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  private case class ReasonBody(template: String, params: Map[String, String])

  private implicit val reasonReads: Reads[ReasonBody] = Json.reads[ReasonBody]

  /** 模板清单（发现面·public 元数据无敏感面——仍走鉴权与 journal 同口径） */
  def list(): Action[AnyContent] = authAction.async { _ =>
    reason.templates.map {
      case Some(ts) => Ok(Json.obj(
        "templates" -> ts.map(tplJson),
        "degraded" -> false))
      case None => Ok(Json.obj("templates" -> Json.arr(), "degraded" -> true, "message" -> "推理存储暂不可用（降级）"))
    }
  }

  def run(): Action[AnyContent] = authAction.async { req =>
    req.body.asJson match {
      case None => Future.successful(BadRequest(jsonError("PARAM_INVALID", "body 须为 JSON")))
      case Some(body) =>
        Json.fromJson[ReasonBody](body) match {
          case JsError(_) => Future.successful(BadRequest(jsonError("PARAM_INVALID", "template 必填·params 须为对象")))
          case JsSuccess(b, _) =>
            // P0 tid 硬化（2026-10-08）：tenantBinding=token 的模板强制 params.tenant:=会话
            // tid——body 自报 tenant≠tid 拒绝（403 TENANT_MISMATCH）；会话无租户上下文拒绝
            // （403 TENANT_REQUIRED，TenantRequired 同语义先例）。body（缺省）模板行为不变。
            val binding = reason.tenantBindingOf(b.template.trim)
            val tenantEnforced: Either[(Int, String), Map[String, String]] = binding match {
              case None => Left((400, s"未知模板: ${b.template.trim}"))
              case Some("token") =>
                req.tenantId match {
                  case None => Left((403, jsonError("TENANT_REQUIRED",
                    "session carries no tenant context; re-login to bind a tenant").toString))
                  case Some(tid) =>
                    b.params.get("tenant") match {
                      case Some(declared) if declared != tid =>
                        Left((403, jsonError("TENANT_MISMATCH",
                          s"params.tenant=$declared 不等于会话租户 $tid（tenantBinding=token 模板不允许跨租户自报）").toString))
                      case _ => Right(b.params.updated("tenant", tid))
                    }
                }
              case Some(_) => Right(b.params)
            }
            tenantEnforced match {
              case Left((status, msg)) =>
                Future.successful(if (status == 403) Forbidden(msg) else BadRequest(jsonError("TEMPLATE_REJECTED", msg)))
              case Right(params) =>
                reason.reason(b.template.trim, params) match {
                  case Left(msg) => Future.successful(BadRequest(jsonError("TEMPLATE_REJECTED", msg)))
                  case Right(fut) => fut.map {
                    case Some(rows) => Ok(Json.obj(
                      "template" -> b.template,
                      "params" -> params,
                      "rows" -> rows.map(Json.toJson(_)),
                      "confidence" -> "mechanical",
                      "degraded" -> false))
                    case None => Ok(Json.obj(
                      "template" -> b.template,
                      "params" -> params,
                      "rows" -> Json.arr(),
                      "confidence" -> "mechanical",
                      "degraded" -> true,
                      "message" -> "推理存储暂不可用（降级）"))
                  }
                }
            }
        }
    }
  }

  private def tplJson(t: KnReasonTemplate) = Json.obj(
    "name" -> t.name,
    "description" -> t.description,
    "params" -> t.params.map { case (k, p) => k -> Json.obj(
      "type" -> p.`type`, "required" -> p.required, "desc" -> p.desc) })

  private def jsonError(code: String, msg: String) = Json.obj("errorCode" -> code, "message" -> msg)
}
