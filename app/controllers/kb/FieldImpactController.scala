package controllers.kb

import javax.inject.{Inject, Singleton}
import net.imadz.common.auth.AuthAction
import net.imadz.metarch.application.kb.FieldImpactStore
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/**
 * 字段爆破面 API（EP_Field_Impact_Server_Side P1·2026-10-08）——POST /api/kb/field-impact。
 *
 * 三形态锚解析（模糊名不短路纪律·与文件侧 field_impact.mjs 同构）：
 *  - 36 位 UUID → noteId 直取（租户图 Field.noteId=服务端号）
 *  - Container.field（全限定名）→ 宿主+字段名双锚
 *  - 裸名 → 按 name 匹配返回全部同名字段行；唯一→resolved，多→ambiguous
 *
 * 认证（semantic-search 同款链）：login 烧 tid → Bearer → 会话 tenantId 硬门——
 * 无租户上下文 403 TENANT_REQUIRED（TenantRequired 同语义）；知识面零 SQL。
 *
 * 响应契约：field-impact/v1 与文件侧 tools/field_impact.mjs 逐键同名
 * （resolution/mode/nodes/merged/coverage/summary）——MCP 转写层零改动。
 * 负断言纪律：零命中 → mode=missed 空集 200（图的诚实答案，非错误）。
 * 降级纪律：Neo4j 不可用 → degraded=true（不 5xx，KnReasonController 同款）。
 *
 * 手写领地声明（KnReasonStore 同款·挂 EP_Field_Impact_Server_Side）。
 */
@Singleton
class FieldImpactController @Inject() (
                                        authAction: AuthAction,
                                        store: FieldImpactStore,
                                        val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  private case class ImpactBody(anchor: String, split: Option[Boolean])

  private implicit val impactReads: Reads[ImpactBody] = Json.reads[ImpactBody]

  private val UuidRe = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

  def impact(): Action[AnyContent] = authAction.async { req =>
    req.body.asJson match {
      case None => Future.successful(BadRequest(err("PARAM_INVALID", "body 须为 JSON")))
      case Some(body) =>
        Json.fromJson[ImpactBody](body) match {
          case JsError(_) => Future.successful(BadRequest(err("PARAM_INVALID", "anchor 必填")))
          case JsSuccess(b, _) =>
            val anchor = b.anchor.trim
            if (anchor.isEmpty) Future.successful(BadRequest(err("PARAM_INVALID", "anchor 必填")))
            else req.tenantId match {
              case None => Future.successful(Forbidden(err("TENANT_REQUIRED",
                "session carries no tenant context; re-login to bind a tenant")))
              case Some(tid) =>
                val splitMode = b.split.getOrElse(false)
                run(tid, anchor, splitMode).map {
                  case Right(js) => Ok(js)
                  case Left(e) => Ok(degraded(e))
                }.recover { case e: Throwable => Ok(degraded(e)) }
            }
        }
    }
  }

  /** 三形态分流+闭包聚合（Store 查询·本层组装——与文件侧 field_impact_lib 同构） */
  private def run(tid: String, anchor: String, split: Boolean): Future[Either[Throwable, JsObject]] = {
    store.probe().map(_ => Right(())).recover(Left(_)).flatMap {
      case Left(e) => Future.successful(Left(e))
      case Right(_) =>
        if (anchor.matches(UuidRe)) byUuid(tid, anchor)
        else {
          val parts = anchor.split("\\.", 2)
          if (parts.length == 2 && parts(0).nonEmpty && parts(1).nonEmpty) byQualified(tid, parts(0), parts(1), split)
          else byName(tid, anchor, split)
        }
    }
  }

  private def byUuid(tid: String, uuid: String): Future[Either[Throwable, JsObject]] =
    store.resolveByUuid(tid, uuid).flatMap { rows =>
      if (rows.isEmpty) Future.successful(Right(missed(uuid, "uuid")))
      else close(tid, rows, "uuid", anchorOf(uuid, rows)).map(Right(_))
    }

  private def byQualified(tid: String, container: String, name: String, split: Boolean): Future[Either[Throwable, JsObject]] =
    store.resolveByName(tid, Some(container), name).flatMap { rows =>
      if (rows.isEmpty) Future.successful(Right(missed(s"$container.$name", "qualified")))
      else close(tid, rows, "resolved", anchorOf(name, rows)).map(Right(_))
    }

  private def byName(tid: String, name: String, split: Boolean): Future[Either[Throwable, JsObject]] =
    store.resolveByName(tid, None, name).flatMap { rows =>
      if (rows.isEmpty) Future.successful(Right(missed(name, "name")))
      else if (rows.length == 1) close(tid, rows, "resolved", name).map(Right(_))
      else if (split) {
        // split 模式：逐候选全闭包（候选组拆分为独立 impact 段）
        val byCid = rows.map(r => r("conceptId").as[String] -> r).groupBy(_._1).map(_._2.head._2)
        Future.sequence(byCid.map { r =>
          close(tid, Seq(r), "resolved", name).map(js => (r("conceptId").as[String], r("container").as[String], js))
        }.toSeq).map { pairs =>
          val candArr = pairs.map { case (cid, container, js) =>
            Json.obj("conceptId" -> cid, "container" -> container, "impact" -> js)
          }
          Right(Json.obj(
            "schema" -> "field-impact/v1", "query" -> name, "mode" -> "split",
            "resolution" -> Json.obj("kind" -> "ambiguous-split", "candidates" -> rows.length,
              "anchor" -> name),
            "candidates" -> JsArray(candArr),
            "coverage" -> Json.obj("degraded" -> false),
            "summary" -> Json.obj("candidates" -> rows.length)))
        }
      } else close(tid, rows, "ambiguous", name).map(Right(_))
    }

  /** 多候选闭包：逐行 impactClosure → 并集 nodes + 逐面 common/onlyBy 差分（union 模式） */
  private def close(tid: String, rows: Seq[Map[String, JsValue]], mode: String, anchor: String): Future[JsObject] = {
    val closureFuts = rows.map(r => store.impactClosure(tid, r("conceptId").as[String], r("role").as[String]))
    Future.sequence(closureFuts).map { perField =>
      // derivedBy 反向血缘补齐：对 st 面行查下游
      val nodes = perField.flatten
      val stRows = nodes.filter(n => n.get("face").exists(_.as[String] == "st"))
      val derivedByFuts = stRows.map(n => store.derivedBy(tid, n("name").as[String], containerOf(n)))
      Future.sequence(derivedByFuts).map { downs =>
        val downSets = downs.map(_.map(d => (d("fieldId").as[String], d("container").as[String])))
        val nodesWithDown = nodes.map { n =>
          if (n.get("face").exists(_.as[String] == "st")) {
            val idx = stRows.indexOf(n)
            if (idx >= 0 && idx < downSets.size && downSets(idx).nonEmpty)
              n.updated("derivedBy", JsString(downSets(idx).map(_._1).mkString(",")))
            else n
          } else n
        }
        val allFaces = nodesWithDown.map(n => n.get("face").map(_.as[String]).getOrElse("other"))
        val distinctFaces = allFaces.distinct.sorted
        val containers = nodesWithDown.flatMap(n => n.get("container").map(_.as[String])).distinct.sorted
        val anchorName = anchor
        Json.obj(
          "schema" -> "field-impact/v1",
          "query" -> anchorName,
          "mode" -> mode,
          "resolution" -> Json.obj(
            "kind" -> mode,
            "candidates" -> rows.length,
            "anchor" -> anchorName,
            "rows" -> rows.map(r => Json.obj(
              "fieldId" -> r("conceptId"), "face" -> faceOf(r("role").as[String]),
              "container" -> r("container")))),
          "nodes" -> nodesWithDown.map(n => Json.toJson(n)),
          "merged" -> mergeByFace(nodesWithDown),
          "coverage" -> Json.obj(
            "faces" -> distinctFaces,
            "containers" -> containers,
            "anchorsEmpty" -> true,
            "degraded" -> false),
          "summary" -> Json.obj(
            "candidates" -> rows.length,
            "rows" -> nodesWithDown.size,
            "faces" -> distinctFaces.map(f => f -> nodesWithDown.count(n => n.get("face").exists(_.as[String] == f)))),
          "warnings" -> Seq(Json.toJson("codeAnchors 尚未量产（P3 另批）——知识→代码落点暂空")))
      }
    }.flatMap(identity)
  }

  /** 逐面并集差分（文件侧 field_impact_lib counts/common/onlyBy 同构·按容器维度） */
  private def mergeByFace(nodes: Seq[Map[String, JsValue]]): JsObject = {
    val byFace: Map[String, Seq[String]] = nodes.groupBy(n => n.get("face").map(_.as[String]).getOrElse("other"))
      .map { case (f, ns) => f -> ns.map(n => n("fieldId").as[String]).distinct.sorted }
    val containersByFace: Map[String, Set[String]] = nodes.groupBy(n => n.get("face").map(_.as[String]).getOrElse("other"))
      .map { case (f, ns) => f -> ns.flatMap(n => n.get("container").map(_.as[String])).toSet }
    val commonContainers = containersByFace.values.reduceOption(_.intersect(_)).getOrElse(Set.empty)
    val onlyBy: Map[String, Set[String]] = containersByFace.map { case (f, cs) =>
      f -> cs.diff(containersByFace.filterKeys(_ != f).values.foldLeft(Set.empty[String])(_ union _))
    }
    Json.obj(
      "faces" -> JsObject(byFace.map { case (f, ids) => f -> Json.toJson(ids) }.toSeq.sortBy(_._1)),
      "commonContainers" -> Json.toJson(commonContainers.toList.sorted),
      "onlyBy" -> JsObject(onlyBy.map { case (f, cs) => f -> Json.toJson(cs.toList.sorted) }.toSeq.sortBy(_._1)))
  }

  private def faceOf(role: String): String = role match {
    case "Identifier" => "id"
    case "State" => "st"
    case "CommandParam" | "SystemParam" => "pm"
    case "EventPayload" => "pl"
    case "ValueObject.Member" => "vo"
    case "ReadModelField" => "rm"
    case _ => "other"
  }

  private def containerOf(n: Map[String, JsValue]): String =
    n.get("container").map(_.as[String]).getOrElse("")

  private def anchorOf(uuidOrName: String, rows: Seq[Map[String, JsValue]]): String =
    rows.headOption.flatMap(_.get("name")).map(_.as[String]).getOrElse(uuidOrName)

  private def missed(anchor: String, kind: String): JsObject = Json.obj(
    "schema" -> "field-impact/v1",
    "query" -> anchor,
    "mode" -> "missed",
    "resolution" -> Json.obj("kind" -> "missed", "candidates" -> 0, "anchor" -> anchor),
    "nodes" -> Json.arr(),
    "coverage" -> Json.obj("faces" -> Json.arr(), "containers" -> Json.arr(), "degraded" -> false),
    "summary" -> Json.obj("rows" -> 0),
    "warnings" -> Json.arr(),
    "negativeAssertion" -> s"租户图中不存在锚「$anchor」的任何字段面出现（$kind 形态·本图负断言，非系统不存在）")

  private def degraded(e: Throwable): JsObject = Json.obj(
    "schema" -> "field-impact/v1",
    "mode" -> "degraded",
    "degraded" -> true,
    "message" -> s"图存储暂不可用（降级）: ${e.getMessage}")

  private def err(code: String, msg: String): JsObject = Json.obj("errorCode" -> code, "message" -> msg)
}
