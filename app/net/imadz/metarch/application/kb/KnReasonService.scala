package net.imadz.metarch.application.kb

import javax.inject.{Inject, Singleton}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * M2 推理面 service（批7·kn_kb_storage_plan §6.2）——/api/kb/reason 门面。
 *
 * 降级纪律（§5.6 分层隔离）：Neo4j 不可用 → None（controller 出 degraded 标记，不 5xx）；
 * 白名单/参数校验失败 → Left（controller 出 400）——两者严格区分：一个是「请求错」，
 * 一个是「存储面降级」，绝不混同。
 *
 * 负断言纪律（§6.2）：查询成功且行集为空 → Some(Seq.empty)——「图谱中不存在该关系路径」
 * 是合法且优先的回答，degraded=false（这不是降级，是图的诚实答案）。
 */
@Singleton
class KnReasonService @Inject() (store: KnReasonStore)(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  /** 在册模板清单（发现面透传） */
  def templates: Future[Option[Seq[KnReasonTemplate]]] =
    Future.successful(Some(store.templates))

  /** 模板租户绑定口径透传（P0 tid 硬化）：None=模板未注册；Some("token")=租户图通道硬门 */
  def tenantBindingOf(template: String): Option[String] = store.tenantBindingOf(template)

  /**
   * 白名单查询。
   * @return Left(原因)   模板未注册/缺必填参数（400 语义）
   *         Right(None)  存储不可用（degraded）
   *         Right(Some(rows)) 查询成功（空集＝合法的负断言答案）
   */
  def reason(template: String, params: Map[String, String]): Either[String, Future[Option[Seq[Map[String, play.api.libs.json.JsValue]]]]] =
    store.reason(template, params).map { fut =>
      fut.map(rows => Some(rows)).recover {
        case e: Throwable =>
          log.warn(s"kb_reason 查询失败（degraded）: 模板=${template} — ${e.getMessage}")
          None
      }
    }
}
