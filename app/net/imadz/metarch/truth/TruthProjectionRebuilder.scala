// MANUAL (S2-82-B4b/B4c · 2026-09-11 · s282 §11-12)：TruthProjectionSource 生产装配——
// 投影 DecisionViewRepository 直查（scalikejdbc 走 projection.conf 连接池·零新方法）。
// 真源租户解析：TSP 行（per sourceKey）的 tenantId——tenantId 系服务端发号不可从真源推导，
// provenance 行是重建的必要原料（缺位 → failed，不猜）。B4c：metarch 键＋skeleton/三兄弟件
// 四 payload（metarch-skeleton / metarch-{fieldmodel,behaviormodel,flowmodel}·P12 灌入）。
package net.imadz.metarch.truth

import net.imadz.metarch.application.projection.repository.CaptureNoteDecisionViewRepository
import net.imadz.metarch.application.projection.repository.CemEdgeDecisionViewRepository
import net.imadz.metarch.application.projection.repository.FieldModelDefinitionDecisionViewRepository
import net.imadz.metarch.application.projection.repository.ReadModelDefinitionDecisionViewRepository
import net.imadz.metarch.application.projection.repository.RoleCommandBindingDecisionViewRepository
import net.imadz.metarch.application.projection.repository.TruthSourceProvenanceDecisionViewRepository
import net.imadz.metarch.application.projection.repository.ValueObjectDefinitionDecisionViewRepository

import com.google.inject.{Inject, Singleton}
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}

/** 生产装配：投影 DecisionViewRepository 直查（scalikejdbc 走 projection.conf 连接池·零新方法）。 */
@Singleton
class TruthProjectionRebuilder @Inject() (
    fmdRepo: FieldModelDefinitionDecisionViewRepository,
    rmdRepo: ReadModelDefinitionDecisionViewRepository,
    cemRepo: CemEdgeDecisionViewRepository,
    rcbRepo: RoleCommandBindingDecisionViewRepository,
    vodRepo: ValueObjectDefinitionDecisionViewRepository,
    noteRepo: CaptureNoteDecisionViewRepository,
    tspRepo: TruthSourceProvenanceDecisionViewRepository)(implicit ec: ExecutionContext)
    extends TruthProjectionSource {

  private val SupportedKeys = Set("fields-template", "readmodels", "cem", "role-command-map", "metarch")

  /** 键 → 该键重建所需的 TSP sourceKey（真源租户反查行）。 */
  private val TenantKeyOf: Map[String, String] = Map(
    "fields-template" -> "fields-template",
    "readmodels" -> "readmodels",
    "cem" -> "cem",
    "role-command-map" -> "role-command-map",
    "metarch" -> "metarch-skeleton",
  )

  override def rebuild(key: String): Future[JsValue] =
    if (!SupportedKeys.contains(key))
      Future.failed(new IllegalArgumentException(s"truth projection rebuild: unsupported key '$key'"))
    else
      for {
        tsp <- tspRepo.findAll()
        row = tsp.find(_.sourceKey == TenantKeyOf(key))
          .getOrElse(throw new IllegalStateException(s"truth projection rebuild: TSP 行缺位 sourceKey=${TenantKeyOf(key)}（provenance 是重建必要原料——重灌 full-load P12）"))
        tid = row.tenantId
        rebuilt <- key match {
          case "fields-template" =>
            val payload = parsePayload(row.payloadJson)
            for {
              aggs <- fmdRepo.findByTenantId(tid)
              vos  <- vodRepo.findByTenantId(tid)
            } yield FieldsTemplateRebuild(aggs, vos, (payload \ "meta").getOrElse(JsNull))
          case "readmodels" =>
            val payload = parsePayload(row.payloadJson)
            rmdRepo.findByTenantId(tid).map(views =>
              ReadmodelsRebuild(views, (payload \ "meta").getOrElse(JsNull), (payload \ "stats").getOrElse(JsNull)))
          case "cem" =>
            val payload = parsePayload(row.payloadJson)
            cemRepo.findByTenantId(tid).map(edges => CemRebuild(edges, wrapperOf(payload)))
          case "role-command-map" =>
            val payload = parsePayload(row.payloadJson)
            rcbRepo.findByTenantId(tid).map(bindings => RoleCommandMapRebuild(bindings, payload))
          case "metarch" =>
            for {
              skeleton <- payloadOf(tsp, tid, "metarch-skeleton")
              fieldModel <- payloadOf(tsp, tid, "metarch-fieldmodel")
              behaviorModel <- payloadOf(tsp, tid, "metarch-behaviormodel")
              flowModel <- payloadOf(tsp, tid, "metarch-flowmodel")
              notes <- noteRepo.findByTenantId(tid)
            } yield MetarchRebuild(notes, skeleton, fieldModel, behaviorModel, flowModel)
        }
      } yield rebuilt

  /** TSP payloadJson 串 → JsObject（P12 灌入恒为对象；非对象＝数据不一致·failed）。 */
  private def parsePayload(s: String): JsObject = Json.parse(s) match {
    case o: JsObject => o
    case other       => throw new IllegalStateException(s"truth projection rebuild: TSP payload 非对象（${other.getClass.getSimpleName}）")
  }

  /** 按租户＋sourceKey 取 TSP payload（缺位 → failed——三兄弟件/skeleton 是 metarch 重建必要原料）。 */
  private def payloadOf(tsp: List[net.imadz.metarch.application.projection.repository.TruthSourceProvenanceDecisionViewTable.TruthSourceProvenanceDecisionView],
                        tid: String, sourceKey: String): Future[JsObject] = {
    val row = tsp.find(r => r.sourceKey == sourceKey && r.tenantId == tid)
      .getOrElse(throw new IllegalStateException(s"truth projection rebuild: TSP 行缺位 sourceKey=$sourceKey tid=$tid（重灌 full-load P12）"))
    Future.successful(parsePayload(row.payloadJson))
  }

  /** cem wrapper 六件（schema/generatedAt/batch/generatedFrom/unresolved/crossValidation/stats）verbatim。 */
  private def wrapperOf(payload: JsObject): JsObject =
    JsObject(payload.fields)
}
