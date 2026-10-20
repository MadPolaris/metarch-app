package controllers.kb

import javax.inject.{Inject, Singleton}
import net.imadz.common.auth.AuthAction
import net.imadz.metarch.application.kb.{KnJournalEntry, KnJournalKinds, KnJournalService}
import play.api.libs.json.{JsError, JsSuccess, Json}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/**
 * M1 审计面 API（批6·kn_kb_storage_plan §5.2）——/api/kb/journal 唯一出口。
 *
 *  - GET  /api/kb/journal?kind=&tid=&limit=   近期流水（at 倒序）；存储不可用 → degraded 标记（不 5xx·§5.6）
 *  - POST /api/kb/journal                     落一笔（系统侧事件源入口：伴读会话完成/图 release/评审动作/投影偏移）
 *
 * 双闸防直写（§4.5「禁前端/LLM 直写」）：AuthAction 鉴权＋kind 词汇表白名单；
 * 写路径 fire-and-forget——mongo 挂时响应 recorded=false（伴读主流程零受阻）。
 * 手写领地声明见 KnJournalStore 头注（挂 EP_S2_48 批1）。
 */
@Singleton
class KnJournalController @Inject() (
                                      authAction: AuthAction,
                                      journal: KnJournalService,
                                      val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  private case class RecordBody(kind: String, tid: String, mid: String,
                                 sourceVersion: Option[String], graphReleaseTag: Option[String], payload: Option[String])

  private implicit val recordReads: play.api.libs.json.Reads[RecordBody] = Json.reads[RecordBody]

  def list(kind: String, tid: String, limit: Int): Action[AnyContent] = authAction.async { _ =>
    journal.recent(if (kind.trim.isEmpty) None else Some(kind.trim),
      if (tid.trim.isEmpty) None else Some(tid.trim), limit).map {
      case Some(entries) =>
        Ok(Json.obj(
          "entries" -> entries.map(e => Json.obj(
            "tid" -> e.tid, "mid" -> e.mid, "kind" -> e.kind, "at" -> e.atMillis,
            "sourceVersion" -> e.sourceVersion, "graphReleaseTag" -> e.graphReleaseTag,
            "payload" -> Json.parse(e.payload))),
          "degraded" -> false))
      case None =>
        Ok(Json.obj("entries" -> Json.arr(), "degraded" -> true, "message" -> "journal 存储暂不可用（降级）"))
    }
  }

  /** agg 汇总读面（T2 翻通道补齐——limit 共用封顶·degraded 同律）。 */
  def agg(limit: Int): Action[AnyContent] = authAction.async { _ =>
    journal.aggTop(limit).map {
      case Some(rows) => Ok(Json.obj(
        "agg" -> rows.map(r => Json.obj(
          "_id" -> r.key, "kind" -> r.kind, "tid" -> r.tid, "mid" -> r.mid,
          "count" -> r.count, "lastAt" -> r.lastAt)),
        "degraded" -> false))
      case None =>
        Ok(Json.obj("agg" -> Json.arr(), "degraded" -> true, "message" -> "journal 存储暂不可用（降级）"))
    }
  }

  def record(): Action[AnyContent] = authAction.async { req =>
    req.body.asJson match {
      case None => Future.successful(BadRequest(jsonError("PARAM_INVALID", "body 须为 JSON")))
      case Some(body) =>
        Json.fromJson[RecordBody](body) match {
          case JsError(_) => Future.successful(BadRequest(jsonError("PARAM_INVALID", "kind/tid/mid 必填")))
          case JsSuccess(b, _) =>
            if (!KnJournalKinds.all.contains(b.kind)) {
              Future.successful(BadRequest(jsonError("PARAM_INVALID", s"kind 须在词汇表内: ${KnJournalKinds.all.mkString("/")}")))
            } else {
              journal.record(KnJournalEntry(
                tid = b.tid.trim, mid = b.mid.trim, kind = b.kind,
                atMillis = System.currentTimeMillis(),
                sourceVersion = b.sourceVersion, graphReleaseTag = b.graphReleaseTag,
                payload = b.payload.getOrElse("{}")))
                .map(ok => Ok(Json.obj("recorded" -> ok, "degraded" -> !ok)))
            }
        }
    }
  }

  private def jsonError(code: String, msg: String) = Json.obj("errorCode" -> code, "message" -> msg)
}
