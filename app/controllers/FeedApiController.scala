package controllers

import net.imadz.common.auth.AuthAction
import java.time.Instant
import java.time.format.DateTimeParseException

import javax.inject.{Inject, Singleton}
import net.imadz.metarch.infrastructure.auth.IdentityStore
import net.imadz.metarch.application.feed.{FeedAggregator, FeedItem}
import play.api.libs.json.{JsArray, JsNull, JsObject, JsString, JsValue, Json}
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

@Singleton
class FeedApiController @Inject() (
  authAction: AuthAction,
  aggregator: FeedAggregator,
  identityStore: IdentityStore,
  val cc: ControllerComponents)(implicit ec: ExecutionContext) extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  private def jInst(i: Instant): JsValue = if (i == null) JsNull else JsString(i.toString)

  private def jOptS(o: Option[String]): JsValue = o match {
    case Some(s) => JsString(s)
    case None    => JsNull
  }

  private def paramInvalid(msg: String): Future[Result] =
    Future.successful(BadRequest(Json.obj("errorCode" -> "PARAM_INVALID", "message" -> msg)))

  private def feedItemJson(i: FeedItem): JsObject = Json.obj(
    "type" -> i.itemType,
    "viewKey" -> i.viewKey,
    "entityId" -> i.entityId,
    "title" -> i.title,
    "actors" -> JsArray(i.actors.map(a => Json.obj("userId" -> a.userId, "displayName" -> a.displayName))),
    "project" -> (i.project match {
      case Some(p) => Json.obj("projectId" -> p.projectId, "name" -> p.name)
      case None    => JsNull
    }),
    "occurredAt" -> jInst(i.occurredAt),
    "relevance" -> i.relevance)

  def feed(modulesRaw: String, scope: String, limit: Int, beforeRaw: String): Action[AnyContent] =
    authAction.async { req =>
      parseBefore(beforeRaw) match {
        case Left(msg) => paramInvalid(msg)
        case Right(before) =>
          val modules = Option(modulesRaw).getOrElse("")
            .split(',').map(_.trim).filter(FeedAggregator.Modules.contains).toSet
          val effScope = if (scope == "mine") "mine" else "tenant"
          aggregator.feed(req.userId, modules, effScope, limit, before).map { moduleItems =>
            Ok(Json.obj("modules" -> JsArray(moduleItems.map {
              case (m, items) => Json.obj("module" -> m, "items" -> JsArray(items.map(feedItemJson)))
            })))
          }
      }
    }

  def inbox(): Action[AnyContent] = authAction.async { req =>
    // R3 恢复精化（2026-08-27）：租户由 FeedAggregator.resolveTenantId 解析（登录同源语义）
    aggregator.resolveTenantId(req.userId).flatMap(aggregator.inbox).map { case (total, buckets) =>
      Ok(Json.obj("total" -> total, "buckets" -> JsArray(buckets.map { b =>
        Json.obj("key" -> b.key, "count" -> b.count,
          "items" -> JsArray(b.items.map(i => Json.obj(
            "viewKey" -> i.viewKey, "entityId" -> i.entityId, "summary" -> i.summary))))
      })))
    }
  }

  def usersBatch(ids: String): Action[AnyContent] = authAction.async { _ =>
    val list = Option(ids).getOrElse("").split(',').map(_.trim).filter(_.nonEmpty).toList
    if (Option(ids).forall(_.trim.isEmpty)) paramInvalid("ids required, comma separated")
    else aggregator.usersBatch(list).map { refs =>
      Ok(JsArray(refs.map(r => Json.obj(
        "userId" -> r.userId, "displayName" -> r.displayName, "email" -> jOptS(r.email)))))
    }
  }

  def me(): Action[AnyContent] = authAction.async { req =>
    identityStore.findByUsername(req.userId).flatMap { docOpt =>
      aggregator.me(req.userId, docOpt.map(_.roles).getOrElse(Nil)).map { m =>
        Ok(Json.obj(
          "userId" -> m.userId,
          "displayName" -> jOptS(m.displayName),
          "email" -> jOptS(m.email),
          "tenantId" -> jOptS(m.tenantId),
          "tenantName" -> jOptS(m.tenantName),
          "roles" -> m.roles,
          "status" -> m.status))
      }
    }
  }

  private def parseBefore(raw: String): Either[String, Option[Instant]] =
    Option(raw).map(_.trim).filter(_.nonEmpty) match {
      case None => Right(None)
      case Some(s) =>
        try Right(Some(Instant.parse(s)))
        catch {
          case _: DateTimeParseException => Left("before must be ISO-8601 instant, e.g. 2026-08-24T00:00:00Z")
        }
    }
}
