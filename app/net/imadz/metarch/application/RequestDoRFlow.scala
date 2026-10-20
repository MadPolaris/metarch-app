package net.imadz.metarch.application

import javax.inject.{Inject, Singleton}
import net.imadz.metarch.application.aggregates.DoRProtocol._
import net.imadz.metarch.application.aggregates.repository.DoRRepository
import net.imadz.metarch.infrastructure.api.OkReceipt
import net.imadz.common.auth.{ApiError, TenantCtx}
import play.api.libs.json.{JsObject, JsString, Json}
import scalikejdbc._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** RequestDoRFlow：DoR 快照服务端装配 one-call（批1-B item 7·2026-09-06·MANUAL 实写面——RegisterUserFlow 同款领地）。
 *
 *  病灶（MF5·Act 13 知识管线半程负路径）：DoRState.knowledgeSnapshot/scopeSnapshot 原无任何 fold 写入
 *  → IR-DOR-AGG-003 notNull 门 400 DOR_SNAPSHOT_REQUIRED → evaluate/findings/verdict/conclude 四步全挡死。
 *
 *  用户裁决（2026-09-06）：「DoR 从 DB 获取数据完成 Evaluation」——本 Flow 从租户 DB 装配两快照：
 *    knowledgeSnapshot ＝ kn_aggregate_instance 按聚合类型计数 JSON（便签/概念/关系/问题…本体知识体量）
 *    scopeSnapshot     ＝ 租户 workshop/session/project/patch 实例清单 JSON（评审范围声明）
 *  快照经命令参数进 DoRRequested 事件载荷进 fold 写 state——event sourcing 面随事件持久化（重启可重建）。
 *
 *  B3·用户拍板#1（2026-09-06·EP_S2_59_S2_60）：workshopId 单入口——客户端只供给 workshopId
 *  （RequestDoRReq 唯一 body 字段），其余范围键由本 Flow 从 workshop 命名空间解析：
 *    projectId/patchId  ＝ scheduleworkshopdecisionview_table（B4a 起含 patchId 列·2026-09-07 切真）
 *    targetSessionIds ＝ opensessiondecisionview_table WHERE workshopId（B4c 起有写者——零目标
 *                        会话仍如实空集）
 *  IR-DOR-CMD-010（原四键命令门）随之退役为 Flow 服务端语义（truth 源已删）。
 *
 *  参数序＝协议 RequestDoR 声明序（生成器 flow 委托——错序即换值，禁手改）。
 */
@Singleton
class RequestDoRFlow @Inject() (
    dorRepository: DoRRepository,
)(implicit ec: ExecutionContext) {

  private implicit val timeout: akka.util.Timeout = akka.util.Timeout(5.seconds)

  private val NilUuid = "00000000-0000-0000-0000-000000000000" // 范围键未解析回退（空租户首评语义·经 recover 兜底不越 5xx）

  /** 租户实例面查询行：agg_type → (instance_id, updated_at_millis)。 */
  private def tenantInstances(tenantId: String): List[(String, String, Long)] =
    DB.readOnly { implicit s =>
      sql"SELECT agg_type, instance_id, updated_at_millis FROM kn_aggregate_instance WHERE tenant_id = $tenantId"
        .map(rs => (rs.string("agg_type"), rs.string("instance_id"), rs.long("updated_at_millis")))
        .list.apply()
    }

  /** workshop 命名空间解析：projectId/patchId ← scheduleworkshopdecisionview_table（B4a 起含 patchId 列；Workshop 命名空间唯一 project·1:1:1 patch）。 */
  private def workshopScopeOf(tenantId: String, workshopId: String): (String, String) =
    DB.readOnly { implicit s =>
      sql"SELECT projectId, patchId FROM scheduleworkshopdecisionview_table WHERE tenantId = $tenantId AND workshopId = $workshopId"
        .map(rs => (rs.string("projectId"), rs.string("patchId"))).single.apply()
    }.getOrElse((NilUuid, NilUuid))

  /** workshop 命名空间解析：targetSessionIds ← opensessiondecisionview_table WHERE workshopId（B4 前空集如实）。 */
  private def sessionIdsOf(tenantId: String, workshopId: String): List[String] =
    DB.readOnly { implicit s =>
      sql"SELECT sessionId FROM opensessiondecisionview_table WHERE tenantId = $tenantId AND workshopId = $workshopId"
        .map(rs => rs.string("sessionId")).list.apply()
    }.sorted

  def request(dorId: String, workshopId: String, ts: TenantCtx): Future[Either[ApiError, OkReceipt]] = {
    Future {
      // 单次取数：计数快照与范围解析共用知识体量切面（快照＝发起时刻的一致切面）
      val instances = tenantInstances(ts.tenantId)
      val counts = instances.groupBy(_._1).view.mapValues(_.size).toMap
      val knowledgeSnapshot: String = Json.toJson(Json.obj(
        "source" -> "kn_aggregate_instance",
        "aggregateCounts" -> Json.toJson(counts.map { case (k, v) => k -> JsString(v.toString) }),
        "totalInstances" -> JsString(instances.size.toString),
        "capturedAt" -> JsString(java.time.Instant.now().toString),
      )).toString()
      val (projectId, patchId) = workshopScopeOf(ts.tenantId, workshopId) // B4a 切真:patchId 不再 NilUuid 过渡
      val targetSessionIds = sessionIdsOf(ts.tenantId, workshopId)
      val scopeSnapshot: String = Json.toJson(Json.obj(
        "tenantId" -> ts.tenantId,
        "workshopId" -> workshopId,
        "patchId" -> patchId,
        "projectId" -> projectId,
        "targetSessionIds" -> Json.toJson(targetSessionIds),
      )).toString()
      (knowledgeSnapshot, scopeSnapshot, projectId, patchId, targetSessionIds)
    }.flatMap { case (knowledgeSnapshot, scopeSnapshot, projectId, patchId, targetSessionIds) =>
      dorRepository.entityRef(dorId).ask {
        (replyTo: akka.actor.typed.ActorRef[RequestDoRReply]) =>
          RequestDoR(
            net.imadz.common.Id.of(dorId),
            net.imadz.common.Id.of(ts.tenantId),
            net.imadz.common.Id.of(workshopId),
            net.imadz.common.Id.of(patchId),
            net.imadz.common.Id.of(projectId),
            targetSessionIds.map(net.imadz.common.Id.of),
            knowledgeSnapshot,
            scopeSnapshot,
            ts.userId,
            replyTo)
      }.map { reply =>
        reply.error match {
          case Some(e) => Left(ApiError.badRequest(e.code, e.message))
          case None    => Right(OkReceipt(dorId, JsString("dor-verdicts")))
        }
      }
    }.recover { case NonFatal(t) =>
      play.api.Logger(getClass).error("requestDoR snapshot assembly failure", t)
      Left(ApiError(500, "INTERNAL_ERROR", "internal error, please retry later"))
    }
  }
}
