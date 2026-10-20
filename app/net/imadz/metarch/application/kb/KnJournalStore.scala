package net.imadz.metarch.application.kb

import javax.inject.Inject
import org.mongodb.scala.{Document, MongoCollection, MongoClient, MongoDatabase}
import org.mongodb.scala.model.{IndexOptions, Indexes, Updates}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * M1 Mongo 审计面存储（批6·kn_kb_storage_plan §4.5）——kn_journal 集合族唯一写者。
 *
 * 手写领地声明（挂 EP_S2_48 批1·writeTerritorial）：本文件族
 * （KnJournalStore/KnJournalService/controllers/kb/KnJournalController/modules/KnJournalModule/
 * conf/routes 尾巴块 kb journal 段）为行为侧手写代码，不在 CodegenMain 生成产物清单内
 * （新文件名无碰撞面），禁止未来把 KnJournal* 加入生成器输出集。
 *
 * 集合纪律（§4.5）：
 *  - kn_journal（raw）：TTL 180 天（at 时间戳索引）——原始审计流水；compound 索引 (kind, at desc) 供查询；
 *  - kn_journal_agg（汇总）：永久——agg 键 (kind, tid, mid) upsert，count 自增＋lastAt 刷新。
 * 文档形状：{_id, tid, mid, kind, at, sourceVersion, graphReleaseTag, payload}。
 * Document 手动映射（IdentityStore 同款惯例·避免宏 codec 注册依赖）。
 *
 * 降级纪律（§5.6「Mongo 挂→纯浏览」）：本层不 throw——写失败 log-and-drop（伴读主流程不受阻），
 * 读失败返回 None 语义由 Service 翻译为 degraded 标记。
 */
class KnJournalStore @Inject() (conf: play.api.Configuration)(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val client: MongoClient =
    MongoClient(conf.getOptional[String]("kb.mongo.uri").getOrElse("mongodb://127.0.0.1:27017"))

  private val db: MongoDatabase =
    client.getDatabase(conf.getOptional[String]("kb.mongo.db").getOrElse("metarch-kb"))

  private val raw: MongoCollection[Document] = db.getCollection("kn_journal")
  private val agg: MongoCollection[Document] = db.getCollection("kn_journal_agg")

  /** spec 清场用（包内可见·生产代码禁用直连集合） */
  private[kb] def rawCollection: MongoCollection[Document] = raw
  private[kb] def aggCollection: MongoCollection[Document] = agg

  /** 建索引幂等（TTL 180d＋查询序）；eager 模块启动时调一次。失败不抛（降级——索引可后补）。 */
  def ensureIndexes(): Future[Unit] = {
    val ttl = raw.createIndex(
      Indexes.ascending("at"),
      IndexOptions().name("ttl_at").expireAfter(180L * 24 * 3600, java.util.concurrent.TimeUnit.SECONDS))
      .toFuture().map(_ => ())
    val query = raw.createIndex(
      Indexes.compoundIndex(Indexes.ascending("kind"), Indexes.descending("at")),
      IndexOptions().name("kind_at_desc"))
      .toFuture().map(_ => ())
    Future.sequence(Seq(ttl, query)).map(_ => ()).recover { case e => log.warn(s"kn_journal 索引建立失败（可后补）: ${e.getMessage}") }
  }

  /** 落一笔 raw 流水（fire-and-forget 语义由 Service 收口；本层失败上抛由其 recover）。 */
  def insertRaw(entry: KnJournalEntry): Future[Unit] =
    raw.insertOne(toDoc(entry)).toFuture().map(_ => ())

  /** agg upsert（永久汇总）：键 (kind, tid, mid)——count 自增＋lastAt/lastSourceVersion 刷新。 */
  def upsertAgg(entry: KnJournalEntry): Future[Unit] = {
    val id = s"${entry.kind}|${entry.tid}|${entry.mid}"
    val sets = Seq(
      Updates.set("kind", entry.kind),
      Updates.set("tid", entry.tid),
      Updates.set("mid", entry.mid),
      Updates.inc("count", 1L),
      Updates.set("lastAt", new java.util.Date(entry.atMillis))) ++
      entry.sourceVersion.map(v => Updates.set("lastSourceVersion", v)) ++
      entry.graphReleaseTag.map(v => Updates.set("graphReleaseTag", v))
    agg.updateOne(
      Document("_id" -> id),
      Updates.combine(sets: _*),
      new org.mongodb.scala.model.UpdateOptions().upsert(true))
      .toFuture().map(_ => ())
  }

  /** 近期流水查询（kind/tid 可选过滤·at 倒序·limit 封顶）。失败上抛（Service 翻译 degraded）。 */
  def recent(kind: Option[String], tid: Option[String], limit: Int): Future[Seq[KnJournalEntry]] = {
    import org.mongodb.scala.model.Filters._
    import org.mongodb.scala.model.Sorts._
    val filters = Seq(kind.map(k => equal("kind", k)), tid.map(t => equal("tid", t))).flatten
    val filter = if (filters.isEmpty) Document() else and(filters: _*)
    raw.find(filter).sort(descending("at")).limit(math.max(1, math.min(limit, 500))).toFuture()
      .map(_.map(fromDoc).toSeq)
  }

  /** agg 汇总读面（lastAt 倒序·limit 封顶·T2 kb_journal 翻通道补齐——mongosh 直连时代 CLI 同时返回两面，翻通道不降级）。 */
  def aggTop(limit: Int): Future[Seq[KnJournalAggRow]] = {
    import org.mongodb.scala.model.Sorts._
    agg.find().sort(descending("lastAt")).limit(math.max(1, math.min(limit, 500))).toFuture()
      .map(_.map { d =>
        KnJournalAggRow(
          key = Option(d.get("_id")).map(_.toString).getOrElse(""),
          kind = Option(d.getString("kind")).getOrElse(""),
          tid = Option(d.getString("tid")).getOrElse(""),
          mid = Option(d.getString("mid")).getOrElse(""),
          count = Option(d.get("count")).map(_.toString.toLong).getOrElse(0L),
          lastAt = Option(d.getDate("lastAt")).map(_.getTime).getOrElse(0L))
      })
  }

  private def toDoc(e: KnJournalEntry): Document = {
    val d = Document(
      "_id" -> new org.bson.types.ObjectId(),
      "tid" -> e.tid,
      "mid" -> e.mid,
      "kind" -> e.kind,
      "at" -> new java.util.Date(e.atMillis),
      "payload" -> e.payload)
    // 可空字段条件性写入（Document 构造磁铁拒 null）
    e.sourceVersion.foreach(v => d.put("sourceVersion", v))
    e.graphReleaseTag.foreach(v => d.put("graphReleaseTag", v))
    d
  }

  private def fromDoc(d: Document): KnJournalEntry = KnJournalEntry(
    tid = Option(d.getString("tid")).getOrElse(""),
    mid = Option(d.getString("mid")).getOrElse(""),
    kind = Option(d.getString("kind")).getOrElse(""),
    atMillis = Option(d.getDate("at")).map(_.getTime).getOrElse(0L),
    sourceVersion = Option(d.getString("sourceVersion")),
    graphReleaseTag = Option(d.getString("graphReleaseTag")),
    payload = d.get("payload").map(_.toString).getOrElse("{}"))
}

/** agg 汇总读面行（typed——Json.obj 隐式推导对 Map[String, Any] 会 diverging·判例） */
final case class KnJournalAggRow(key: String, kind: String, tid: String, mid: String, count: Long, lastAt: Long)

/**
 * 一笔审计流水（§4.5 文档形状的强类型载体）。
 * @param tid   租户 id
 * @param mid   模块/会话 id（伴读会话 id·评审 id·投影 key 等）
 * @param kind  词汇表：companion_session / graph_release / review_action / projection_offset
 * @param payload JSON 字符串（原样保存·消费方自解）
 */
final case class KnJournalEntry(
    tid: String,
    mid: String,
    kind: String,
    atMillis: Long,
    sourceVersion: Option[String] = None,
    graphReleaseTag: Option[String] = None,
    payload: String = "{}")

/** kind 词汇表（白名单闸——禁前端/LLM 直写自由 kind；扩展须改这里＋裁决留痕）。 */
object KnJournalKinds {
  val all: Set[String] = Set("companion_session", "graph_release", "review_action", "projection_offset")
}
