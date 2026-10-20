package net.imadz.metarch.application.kb

import javax.inject.{Inject, Singleton}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * M1 kn-journal service（批6·kn_kb_storage_plan §4.5/§5.2）——审计面唯一写者门面。
 *
 * 写纪律（「伴读会话完成 / 图 release 钩子 / 评审动作 / 实例投影偏移」四事件源经此落库；
 * 前端/LLM 禁直写——controller 侧 AuthAction＋kind 白名单双闸，本层再加一道词汇表闸）：
 *  - record ＝ raw insert ＋ agg upsert（同一笔的两侧）；**fire-and-forget**——
 *    任何失败 recover 落日志返回 false（伴读降级可用·§5.6「Mongo 挂→纯浏览」），
 *    绝不阻塞/炸掉调用方主流程；
 *  - recent ＝ 读路径；失败翻译为 None（controller 出 degraded 标记，不 5xx）。
 */
@Singleton
class KnJournalService @Inject() (store: KnJournalStore)(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  /** 落一笔审计；@return true=两侧全成；false=任一侧失败（已 log·伴读不受阻） */
  def record(entry: KnJournalEntry): Future[Boolean] = {
    if (!KnJournalKinds.all.contains(entry.kind)) {
      log.warn(s"kn_journal 拒记未知 kind: ${entry.kind}（词汇表白名单闸）")
      Future.successful(false)
    } else {
      Future.sequence(Seq(
        store.insertRaw(entry).map(_ => true),
        store.upsertAgg(entry).map(_ => true)))
        .map(_.forall(identity))
        .recover {
          case e: Throwable =>
            log.warn(s"kn_journal 写入失败（fire-and-forget 落日志·主流程不受阻）: ${e.getMessage}")
            false
        }
    }
  }

  /** 近期流水；Some(entries)=健康；None=存储不可用（degraded——调用方降级呈现） */
  def recent(kind: Option[String], tid: Option[String], limit: Int): Future[Option[Seq[KnJournalEntry]]] =
    store.recent(kind, tid, limit)
      .map(Some(_))
      .recover {
        case e: Throwable =>
          log.warn(s"kn_journal 读取失败（degraded）: ${e.getMessage}")
          None
      }

  /** agg 汇总读面（T2 翻通道补齐·degraded 同律） */
  def aggTop(limit: Int): Future[Option[Seq[KnJournalAggRow]]] =
    store.aggTop(limit)
      .map(Some(_))
      .recover {
        case e: Throwable =>
          log.warn(s"kn_journal agg 读取失败（degraded）: ${e.getMessage}")
          None
      }
}
