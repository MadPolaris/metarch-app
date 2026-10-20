package controllers.kb

import javax.inject.{Inject, Singleton}
import net.imadz.common.auth.AuthAction
import net.imadz.metarch.infrastructure.graph.GraphProjectionRepository
import play.api.libs.json.{JsArray, JsObject, Json}
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * KN 图普查 API（T3②·kn_graph_census.mjs 服务端等价面）——GET /api/kb/census 唯一出口。
 *
 * 三面普查：节点标签×计数 / 边类型×计数 / 边两端标签对×计数——「图现状」一条命令可复跑
 * （agent 断言前先跑它·2026-09-20 判例：凭印象讲图两次看错）。
 *
 * 范围口径：全图普查（元模型 root 租户 Kn*＋业务投影节点）——与文件侧 census 同口径；
 * 租户裁剪面已有 /api/graph/stats（statsScoped）。发射者对账（图上边↔发射器源码字面量）
 * 是文件侧职责（读本仓 tools/ 源码）不进服务端。
 *
 * 纪律同 KnReasonStore：只读 readTransaction·零写入口；普查是诊断面，空图＝合法答案。
 */
@Singleton
class KnCensusController @Inject() (
                                     authAction: AuthAction,
                                     val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  def census(): Action[AnyContent] = authAction.async { _ =>
    Future {
      val labels = GraphProjectionRepository.queryList("CALL db.labels() YIELD label RETURN label").map(_("label").toString)
      val rels = GraphProjectionRepository.queryList("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType").map(_("relationshipType").toString)

      val nodeCounts = labels.map { l =>
        val cnt = GraphProjectionRepository.queryList(s"MATCH (n:`$l`) RETURN count(n) AS n").headOption.flatMap(_.get("n").map(_.toString.toLong)).getOrElse(0L)
        l -> cnt
      }.toMap

      val edgeCounts = scala.collection.mutable.LinkedHashMap[String, Long]()
      val edgeEnds = scala.collection.mutable.LinkedHashMap[String, List[Map[String, Any]]]()
      for (r <- rels) {
        val cnt = GraphProjectionRepository.queryList(s"MATCH ()-[e:`$r`]->() RETURN count(e) AS n").headOption.flatMap(_.get("n").map(_.toString.toLong)).getOrElse(0L)
        edgeCounts(r) = cnt
        edgeEnds(r) = if (cnt == 0) Nil else
          GraphProjectionRepository.queryList(
            s"MATCH (a)-[e:`$r`]->(b) RETURN coalesce(labels(a)[0],'?') AS A, coalesce(labels(b)[0],'?') AS B, count(*) AS n ORDER BY n DESC")
        edgeEnds(r) = edgeEnds(r).map(m => Map("A" -> m("A").toString, "B" -> m("B").toString, "n" -> m("n")))
      }

      Ok(Json.obj(
        "schema" -> "kn-graph-census/v1",
        "nodeLabels" -> labels.length,
        "nodes" -> nodeCounts.values.sum,
        "edgeTypes" -> rels.length,
        "edges" -> edgeCounts.values.sum,
        "nodeCounts" -> JsObject(nodeCounts.map { case (k, v) => k -> Json.toJson(v) }),
        "edgeCounts" -> JsObject(edgeCounts.toMap.map { case (k, v) => k -> Json.toJson(v) }),
        "edgeEnds" -> JsObject(edgeEnds.toMap.map { case (r, rows) =>
          r -> JsArray(rows.map(m => Json.obj("A" -> Json.toJson(m("A").toString), "B" -> Json.toJson(m("B").toString), "n" -> Json.toJson(m("n").toString.toLong))))
        }),
        "degraded" -> false))
    }(ec)
  }
}
