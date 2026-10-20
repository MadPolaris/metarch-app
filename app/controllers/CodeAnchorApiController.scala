// 手写件（方案乙 S5·2026-09-25）——服务端接锚面。
//
// 为什么**手写**：生成器的 `gen_api_layer` 不认「图写」这类命令面（`GraphApiController` 的
// backfill 是 generator 的 B3 特例）。本控制器是**接锚端点**的唯一出口——设计遵既有纪律：
//   ① 鉴权走 `AuthAction`（与 `/api/graph/*` 同闸）；
//   ② 租户由**声明**而来（载荷 `tenantId` 必须 ∈ 调用者可见租户集·BUG-011 同款默认拒绝）；
//   ③ 图标签 `CodeAnchor` 入 `graph.tenant.required-labels` 强制清单（缺戳即 warn/strict 拦）；
//   ④ 写入用 `GraphProjectionRepository.mergeNode`（同 backfill 路径·幂等 MERGE）。
//
// 语义（答「这条知识落在哪些代码点位」）：
//   `(:Concept|:Note {noteId})-[:ANCHORED_AT]->(:CodeAnchor {anchorId})-[:IN_FILE]->(:CodeFile {path})`
//   anchorId = `<知识号>::<角色>::<宿主>`（确定性·可重复 upsert）。
//   · `知识号` 是**服务端权威号**（Note.noteId / Concept.noteId）；
//   · 不做存在性校验（同租户号由投影时序保证·查无即图上一跳为空＝负断言，不臆造）。
//
// 端点：
//   POST /api/knowledge/code-anchor        单条 upsert
//   POST /api/knowledge/code-anchor/batch  批量 upsert（生成器整批回写）
//   GET  /api/knowledge/code-anchor/by-note/:noteId   「这条知识落在哪」（一跳）
package controllers

import javax.inject.{Inject, Singleton}
import net.imadz.common.auth.AuthAction
import net.imadz.metarch.application.projection.repository.{InviteTenantMemberDecisionViewRepository, OnboardTenantDecisionViewRepository}
import net.imadz.metarch.infrastructure.graph.{GraphProjectionRepository, GraphMutationExecutor}
import play.api.libs.json._
import play.api.mvc._
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

@Singleton
class CodeAnchorApiController @Inject() (
    authAction: AuthAction,
    tenantViews: OnboardTenantDecisionViewRepository,
    rosterViews: InviteTenantMemberDecisionViewRepository,
    val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  /** BUG-011 同款（与 GraphApiController.visibleTenantIds 逐字同构）：uid → 可见租户集；查无归属 = 空集 */
  private def visibleTenantIds(userId: String): Future[Set[String]] =
    tenantViews.findAll().flatMap { all =>
      val ownedOrOnboardMember = all.filter(v => v.ownerUserId == userId || net.imadz.metarch.domain.entities.OrganizationMemberIds.isMemberOf(v.tenantId, userId, v.orgMemberIds)).map(_.tenantId).toSet
      rosterViews.findByUserId(userId).map { roster =>
        val joinedRoster = roster.filter(r => r.joinedAt.isDefined && r.removedAt.isEmpty).map(_.tenantId).toSet
        ownedOrOnboardMember.union(joinedRoster)
      }
    }

  private def forTenant(userId: String, tenantId: String)(body: => Result): Future[Result] =
    visibleTenantIds(userId).map { vis =>
      if (tenantId != null && tenantId.trim.nonEmpty && vis.contains(tenantId)) body
      else Forbidden(Json.obj("errorCode" -> "TENANT_FORBIDDEN", "message" -> s"租户 $tenantId 不在调用者可见集内"))
    }

  /** 通用 MERGE 执行（自带 $ 参数绑定）。
    *
    * 为什么不用 `GraphProjectionRepository.mergeNode`：其 `s"$k: $$k"` 生成的占位符是字面
    * `$k`（参数名 `k`）——单键即炸「Expected parameter(s): k」（2026-09-25 实测）。
    * 手写件（无 @generated 头）本批不动它（避免牵连 backfill 路径）；本端点自带执行器。
    */
  private def exec(cypher: String, params: Map[String, AnyRef]): Unit = {
    val s = net.imadz.metarch.infrastructure.graph.GraphConnectionConfig.newDriver().session()
    try {
      val p = new java.util.HashMap[String, AnyRef]()
      params.foreach { case (k, v) => p.put(k, v) }
      s.writeTransaction(tx => tx.run(cypher, p).consume())
    } finally s.close()
  }

  /** 锚点 id 确定性编码——同（知识号, 角色, 宿主）重复 upsert 幂等 */
  private def anchorIdOf(noteId: String, role: String, host: String): String =
    noteId + "::" + role + "::" + host

  private def upsertOne(tenantId: String, noteId: String, role: String, host: String,
                        file: String, symbols: Seq[String], extra: Map[String, String]): Unit = {
    val anchorId = anchorIdOf(noteId, role, host)
    val hasFile = file != null && file.trim.nonEmpty
    // ① CodeAnchor 节点（单键 MERGE·确定性 id ⇒ 幂等）
    exec("MERGE (a:CodeAnchor {anchorId: $anchorId}) SET a.noteId = $noteId, a.role = $role, " +
      "a.host = $host, a.symbols = $symbols, a.tenantId = $tenantId",
      Map("anchorId" -> anchorId, "noteId" -> noteId, "role" -> role, "host" -> host,
          "symbols" -> symbols.mkString(","), "tenantId" -> tenantId))
    // ② CodeFile 节点（同租户同路径幂等）
    if (hasFile)
      exec("MERGE (f:CodeFile {path: $path, tenantId: $tenantId}) SET f.path = $path",
        Map("path" -> file, "tenantId" -> tenantId))
    // ③ 知识节点 → 锚点：**两端 MATCH、只 MERGE 边**。
    //    不用 `MERGE (k)-[:R]->(a {props})` 复合形态：该形态在两端不同时存在时会**新建端点**，
    //    实测留下过「同 anchorId、属性全空」的幽灵节点（2026-09-25）。两端先 MATCH 则不可能多建。
    //    ⚠ 号形态归一：图 Note 节点的 noteId 带 **`ws:` 前缀**（`GraphEventProjector` 侧约定），
    //    应用面（notes 读面/导出物）给的是**裸 UUID**——两边都试，缺一即断链（2026-09-25 实测）。
    val idForms = Seq(noteId, "ws:" + noteId)
    (for { lbl <- Seq("Note", "Concept"); id <- idForms } yield (lbl, id)).foreach { case (lbl, id) =>
      Try(exec("MATCH (k:" + lbl + " {noteId: $id, tenantId: $tenantId}) " +
        "MATCH (a:CodeAnchor {anchorId: $anchorId}) " +
        "MERGE (k)-[:ANCHORED_AT]->(a)",
        Map("id" -> id, "tenantId" -> tenantId, "anchorId" -> anchorId)))
    }
    // ④ 锚点 → 文件（同款：两端 MATCH·只 MERGE 边）
    if (hasFile)
      Try(exec("MATCH (a:CodeAnchor {anchorId: $anchorId}) " +
        "MATCH (f:CodeFile {path: $path, tenantId: $tenantId}) " +
        "MERGE (a)-[:IN_FILE]->(f)",
        Map("anchorId" -> anchorId, "path" -> file, "tenantId" -> tenantId)))
  }

  final case class AnchorReq(noteId: String, role: String, host: String, file: String,
                             symbols: Seq[String] = Nil, tenantId: String,
                             sourceVersion: Option[String] = None, graphReleaseTag: Option[String] = None)
  private implicit val anchorReads: Reads[AnchorReq] = Json.reads[AnchorReq]

  final case class AnchorBatchReq(anchors: Seq[AnchorReq])
  private implicit val batchReads: Reads[AnchorBatchReq] = Json.reads[AnchorBatchReq]

  /** 单条接锚 */
  def record(): Action[AnyContent] = authAction.async { req =>
    req.body.asJson.map(_.validate[AnchorReq]) match {
      case Some(JsSuccess(a, _)) => forTenant(req.userId, a.tenantId) {
        Try(upsertOne(a.tenantId, a.noteId, a.role, a.host, a.file, a.symbols, Map.empty)) match {
          case scala.util.Success(_) => Ok(Json.obj("status" -> "ok", "anchorId" -> anchorIdOf(a.noteId, a.role, a.host)))
          case scala.util.Failure(t) => InternalServerError(Json.obj("errorCode" -> "GRAPH_WRITE_FAILED", "message" -> String.valueOf(t.getMessage)))
        }
      }
      case Some(JsError(e)) => Future.successful(BadRequest(Json.obj("errorCode" -> "PARAM_INVALID", "message" -> JsError.toJson(e).toString)))
      case None             => Future.successful(BadRequest(Json.obj("errorCode" -> "PARAM_INVALID", "message" -> "JSON body required")))
    }
  }

  /** 批量接锚（生成器整批回写） */
  def recordBatch(): Action[AnyContent] = authAction.async { req =>
    req.body.asJson.map(_.validate[AnchorBatchReq]) match {
      case Some(JsSuccess(b, _)) =>
        val distinct = b.anchors.map(_.tenantId).distinct
        if (distinct.size != 1) Future.successful(BadRequest(Json.obj("errorCode" -> "TENANT_INCONSISTENT", "message" -> "批量必须同租户")))
        else forTenant(req.userId, distinct.headOption.getOrElse("")) {
          var ok = 0
          val failures = scala.collection.mutable.ListBuffer[String]()
          b.anchors.foreach { a =>
            Try(upsertOne(a.tenantId, a.noteId, a.role, a.host, a.file, a.symbols, Map.empty)) match {
              case scala.util.Success(_) => ok += 1
              case scala.util.Failure(t) => failures += s"${a.noteId}/${a.host}: ${t.getMessage}"
            }
          }
          Ok(Json.obj("status" -> "ok", "recorded" -> ok, "failed" -> failures.size,
            "failures" -> failures.take(20)))
        }
      case Some(JsError(e)) => Future.successful(BadRequest(Json.obj("errorCode" -> "PARAM_INVALID", "message" -> JsError.toJson(e).toString)))
      case None             => Future.successful(BadRequest(Json.obj("errorCode" -> "PARAM_INVALID", "message" -> "JSON body required")))
    }
  }

  /** 「这条知识落在哪」——一跳：noteId → 锚（**两个面都查**）→ 代码点位
    *
    * ★ 两面之辨（EP_Anchor_Bridge_Landing 阶段 E·2026-09-26 实证）：
    *   - **面甲** `(:CodeAnchor {noteId})`——S5 旁路写入的独立锚节点（`:ANCHORED_AT` 形态）；
    *   - **面乙** `(:Concept).anchors` 属性——**正规通路**写入（KnowledgePatch → 策略 →
    *     RecordConceptLandings → 投影 handler），形态 `noteId|role|host|file|symbols;;…`。
    *   只查面甲会漏掉正规通路的全部锚（实测：1630 个正规锚在图上、端点却答 count=0）。
    *   两面**并集**返回；同一 noteId 在两面都有时去重（按 anchorId）。
    */
  def byNote(noteId: String): Action[AnyContent] = authAction.async { req =>
    visibleTenantIds(req.userId).map { vis =>
      if (vis.isEmpty) Ok(Json.obj("noteId" -> noteId, "count" -> 0, "anchors" -> JsArray()))
      else {
        val params = new java.util.HashMap[String, AnyRef]()
        params.put("noteId", noteId)
        params.put("tenants", vis.toList.asJava)

        // 面甲：独立 CodeAnchor 节点
        val cypherA =
          """MATCH (a:CodeAnchor {noteId: $noteId})
            |WHERE a.tenantId IN $tenants
            |OPTIONAL MATCH (a)-[:IN_FILE]->(f:CodeFile)
            |RETURN a.anchorId AS anchorId, a.role AS role, a.host AS host,
            |       a.symbols AS symbols, f.path AS file""".stripMargin
        val rowsA = Try(GraphProjectionRepository.queryList(cypherA, params)).getOrElse(Nil).map { r =>
          Json.obj(
            "anchorId" -> r.get("anchorId").map(String.valueOf),
            "role" -> r.get("role").map(String.valueOf),
            "host" -> r.get("host").map(String.valueOf),
            "symbols" -> r.get("symbols").map(String.valueOf),
            "file" -> r.get("file").map(String.valueOf))
        }

        // 面乙：Concept.anchors 属性（正规通路）——形态 `noteId|role|host|file|symbols;;…`
        val cypherB =
          """MATCH (c:Concept {tenantId: $tenantId})
            |WHERE c.anchors CONTAINS $noteId
            |RETURN c.name AS conceptName, c.anchors AS anchors""".stripMargin
        val rowsB = vis.toList.flatMap { tid =>
          val p2 = new java.util.HashMap[String, AnyRef]()
          p2.put("tenantId", tid)
          p2.put("noteId", noteId)
          Try(GraphProjectionRepository.queryList(cypherB, p2)).getOrElse(Nil).flatMap { r =>
            val cname = r.get("conceptName").map(String.valueOf).getOrElse("")
            r.get("anchors").map(String.valueOf).getOrElse("").split(";;").toList
              .filter(_.startsWith(noteId + "|"))
              .map { seg =>
                val parts = (seg.split("\\|", -1) ++ Array("", "", "", "")).take(5)
                Json.obj(
                  "anchorId" -> s"$noteId::${parts(1)}::${parts(2)}",
                  "role" -> parts(1),
                  "host" -> parts(2),
                  "symbols" -> parts(4),
                  "file" -> parts(3),
                  "via" -> "concept.anchors",
                  "concept" -> cname)
              }
          }
        }

        val all = (rowsA ++ rowsB).groupBy(r => (r \ "anchorId").asOpt[String].getOrElse(""))
          .values.map(_.head).toList
          .sortBy(r => ((r \ "role").asOpt[String].getOrElse(""), (r \ "host").asOpt[String].getOrElse("")))
        Ok(Json.obj("noteId" -> noteId, "count" -> all.size, "anchors" -> all))
      }
    }
  }
}
