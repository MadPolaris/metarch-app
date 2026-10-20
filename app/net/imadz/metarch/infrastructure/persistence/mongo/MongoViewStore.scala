package net.imadz.metarch.infrastructure.persistence.mongo

import javax.inject.{Inject, Singleton}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.model.{Filters, IndexOptions, Indexes, ReplaceOptions}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * MongoViewStore —— 决策视图 Mongo 文档读模型手写基建（视图存储重设计 2026-10-08·KP 试点）。
 *
 * 手写领地声明：本文件为行为侧手写基建（挂 handwritten-manifest·mongo-view-store 领地），
 * 禁止加入生成器输出集——生成 Impl（*MongoRepositoryImpl）机械委托本 store（PgVectorStore 同款分工：
 * 连接/索引/分页逻辑在手写基建，模板只机械发射调用）。
 *
 * 设计判据（EP 决策记录）：
 *  - 文档整取整写：视图行整体＝一个文档，**视图字段平铺文档顶层**（Document(json) 单点解析）——
 *    TEXT 列 JSON blob 同病族的退路（schema 演进免疫 guards 型全表 500 事故）；tenantId/键字段
 *    天然可查（无需 JSON 内寻址）。
 *  - decode 单行失败＝单行披露不炸集合：读面逐行 try（log.warn＋跳过）——对照 MySQL 路全表
 *    反序列化一炸全表的 CRASH_GUARD。
 *  - 阻塞语义：org.mongodb.scala 观察 Future（非 ReactiveMongo）——生成 Impl 在 JdbcProjection
 *    handler 内以 Await 风格阻塞消费（offset 仍留 MySQL·exactlyOnce 不动）。
 *  - 集合纪律：一个 DecisionView 一个集合（view_* 前缀）；键唯一索引＋tenantId 查询索引
 *    （ensureCollections 幂等·失败不抛——索引可后补）。
 */
@Singleton
class MongoViewStore @Inject() (conf: play.api.Configuration)(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val client: MongoClient =
    MongoClient(conf.getOptional[String]("viewstore.mongo.uri").getOrElse("mongodb://127.0.0.1:27017"))

  private val db: MongoDatabase =
    client.getDatabase(conf.getOptional[String]("viewstore.mongo.db").getOrElse("cqrs-demo-journal"))

  private def coll(name: String): MongoCollection[Document] = db.getCollection(name)

  /** 建索引幂等（specs: (collection, keyField)·Bootstrap eager 调一次）。 */
  def ensureCollections(specs: Seq[(String, String)]): Future[Unit] = {
    val idxFuts = specs.flatMap { case (name, keyField) =>
      Seq(
        coll(name).createIndex(
          Indexes.ascending(keyField),
          IndexOptions().name(s"ux_$keyField").unique(true)).toFuture().map(_ => ()),
        coll(name).createIndex(
          Indexes.ascending("tenantId"),
          IndexOptions().name("ix_tenantId")).toFuture().map(_ => ()))
    }
    Future.sequence(idxFuts).map(_ => ())
      .recover { case e => log.warn(s"[MongoViewStore] 索引建立失败（可后补）: ${e.getMessage}"); () }
  }

  /** 文档整写（replaceOne upsert·按 keyField 判等）——幂等承接 at-least-once 投影窗口。 */
  def putDocument(collection: String, keyField: String, keyValue: String, json: String): Unit = {
    val doc = Document(json)
    coll(collection).replaceOne(
      Filters.eq(keyField, keyValue), doc, new ReplaceOptions().upsert(true))
      .toFuture().map(_ => ())
      .recover { case e =>
        log.error(s"[MongoViewStore] putDocument $collection $keyField=$keyValue 失败: ${e.getMessage}")
        throw e
      }
  }

  /** 单键取（Document→JSON 串；缺失 None）。 */
  def findByKey(collection: String, keyField: String, keyValue: String): Future[Option[String]] =
    coll(collection).find(Filters.eq(keyField, keyValue))
      .toFuture().map(_.headOption.map(docToJson))

  /** 租户内分页（_id 倒序·limit/offset SQL 语义同形）。decode 单行失败＝单行披露跳过。
    * [Phase 3·6合1] conds 附加等值过滤（默认空＝8 旧族零波及）——kind 判别式族传 Map("kind"->kv) 防串族。 */
  def findByTenantPaged(collection: String, tenantId: String, limit: Int, offset: Long,
      conds: Map[String, String] = Map.empty): Future[Seq[String]] = {
    val filter = Filters.and((Seq(Filters.eq("tenantId", tenantId)) ++
      conds.map { case (k, v) => Filters.eq(k, v) }.toSeq): _*)
    coll(collection).find(filter)
      .sort(Document("_id" -> -1))
      .skip(offset.toInt)
      .limit(limit)
      .toFuture().map(docsToJsons)
  }

  /** 租户计数（分页 total）。 */
  def countByTenant(collection: String, tenantId: String): Future[Long] =
    coll(collection).countDocuments(Filters.eq("tenantId", tenantId)).toFuture()

  /** 全表（进程内可信消费者·与 MySQL findAll 同语义）。 */
  def findAll(collection: String): Future[Seq[String]] =
    coll(collection).find().toFuture().map(docsToJsons)

  /** 按声明字段等值过滤（findBy<CatalogId> 等维度查询通用形）。 */
  def findByFields(collection: String, conds: Map[String, String]): Future[Seq[String]] = {
    val filter = Filters.and(conds.map { case (k, v) => Filters.eq(k, v) }.toSeq: _*)
    coll(collection).find(filter).toFuture().map(docsToJsons)
  }

  /** Document → JSON 串（ExtendedJson——BSON 日期/Long 保真·play-json 侧 Id/Option 由生成 Impl decode）。 */
  private def docToJson(d: Document): String =
    try d.toJson()
    catch { case e: Exception => log.warn(s"[MongoViewStore] toJson 失败: ${e.getMessage}"); "" }

  /** 逐行 decode：失败行 log.warn＋跳过（毒行隔离·绝不 fail 整个集合）。 */
  private def docsToJsons(docs: Seq[Document]): Seq[String] =
    docs.flatMap { d =>
      val j = docToJson(d)
      if (j.isEmpty) None else Some(j)
    }

  // ── Phase 2 Document 类型化接口（Q8 补丁·B 组装落地 2026-10-20） ──
  // 依据 ~/workspace/phase2-mongoviewstore-document-api.md §2 逐字落地；旧 String 接口全保留。

  /** 文档整写（Document 版·replaceOne upsert·按 keyField 判等）——与 putDocument 同语义，
    * 免去 JSON 串往返。生成 Impl 的 viewToDocument 直接落此口。 */
  def putDocumentDoc(collection: String, keyField: String, keyValue: String, doc: Document): Unit = {
    coll(collection).replaceOne(
      Filters.eq(keyField, keyValue), doc, new ReplaceOptions().upsert(true))
      .toFuture().map(_ => ())
      .recover { case e =>
        log.error(s"[MongoViewStore] putDocumentDoc $collection $keyField=$keyValue 失败: ${e.getMessage}")
        throw e
      }
  }

  /** 单键取（Document 版；缺失 None）。 */
  def findByKeyDoc(collection: String, keyField: String, keyValue: String): Future[Option[Document]] =
    coll(collection).find(Filters.eq(keyField, keyValue)).toFuture().map(_.headOption)

  /** 租户内分页（Document 版·_id 倒序·conds 附加等值过滤，与 String 版同语义）。 */
  def findByTenantPagedDoc(collection: String, tenantId: String, limit: Int, offset: Long,
      conds: Map[String, String] = Map.empty): Future[Seq[Document]] = {
    val filter = Filters.and((Seq(Filters.eq("tenantId", tenantId)) ++
      conds.map { case (k, v) => Filters.eq(k, v) }.toSeq): _*)
    coll(collection).find(filter)
      .sort(Document("_id" -> -1))
      .skip(offset.toInt)
      .limit(limit)
      .toFuture()
  }

  /** 租户计数（带 conds 重载·目录族 kind 判别计数专用）。
    * 与既有 countByTenant(collection, tenantId) 并存（旧签名不动）；
    * conds 为空时语义与旧签名一致。走 countDocuments，不拉行。 */
  def countByTenant(collection: String, tenantId: String, conds: Map[String, String]): Future[Long] = {
    val filter = Filters.and((Seq(Filters.eq("tenantId", tenantId)) ++
      conds.map { case (k, v) => Filters.eq(k, v) }.toSeq): _*)
    coll(collection).countDocuments(filter).toFuture()
  }

  /** 全表（Document 版·进程内可信消费者，与 findAll 同语义）。 */
  def findAllDoc(collection: String): Future[Seq[Document]] =
    coll(collection).find().toFuture()

  /** 按声明字段等值过滤（Document 版）。 */
  def findByFieldsDoc(collection: String, conds: Map[String, String]): Future[Seq[Document]] = {
    val filter = Filters.and(conds.map { case (k, v) => Filters.eq(k, v) }.toSeq: _*)
    coll(collection).find(filter).toFuture()
  }

  /** 字段索引补建（消费生成物 MongoViewIndexSpec.fieldIndexes·幂等·失败不抛，
    * 与 ensureCollections 同纪律：索引可后补）。生成 Impl 的 finder 字段索引走此口，
    * 不再靠人工在 mongo shell 里补。 */
  def ensureFieldIndexes(specs: Seq[(String, String)]): Future[Unit] = {
    val idxFuts = specs.map { case (name, field) =>
      coll(name).createIndex(
        Indexes.ascending(field),
        IndexOptions().name(s"ix_$field")).toFuture().map(_ => ())
    }
    Future.sequence(idxFuts).map(_ => ())
      .recover { case e => log.warn(s"[MongoViewStore] 字段索引建立失败（可后补）: ${e.getMessage}"); () }
  }
}
