package net.imadz.metarch.infrastructure.auth

import javax.inject.{Inject, Singleton}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.{IndexOptions, Indexes, Updates}
import org.mongodb.scala.{Document, MongoCollection, MongoClient, MongoDatabase}

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** 身份用户文档 */
case class UserDoc(_id: String, username: String, passwordHash: String,
                   roles: List[String], tokenVersion: Int, createdAt: Long)

/**
 * 身份存储（v1：Mongo 单集合 CRUD；v2 评估升级 IdentityContext 事件溯源聚合）。
 * username 唯一索引启动时创建。Document 手动映射（避免宏 codec 注册依赖）。
 */
@Singleton
class IdentityStore @Inject() (conf: play.api.Configuration)(implicit ec: ExecutionContext) {

  private val client: MongoClient =
    MongoClient(conf.getOptional[String]("identity.mongo.uri")
      .getOrElse("mongodb://127.0.0.1:27017"))

  private val db: MongoDatabase =
    client.getDatabase(conf.getOptional[String]("identity.mongo.db").getOrElse("metarch-identity"))

  private val users: MongoCollection[Document] = db.getCollection("users")

  // 启动建唯一索引（fire-and-forget）
  users.createIndex(Indexes.ascending("username"), IndexOptions().unique(true)).subscribe(_ => (), _ => ())

  private def toDoc(u: UserDoc): Document =
    Document("_id" -> u._id, "username" -> u.username, "passwordHash" -> u.passwordHash,
      "roles" -> u.roles.mkString(","), "tokenVersion" -> u.tokenVersion, "createdAt" -> u.createdAt)

  private def fromDoc(d: Document): UserDoc =
    UserDoc(
      _id = Option(d.getString("_id")).getOrElse(""),
      username = Option(d.getString("username")).getOrElse(""),
      passwordHash = d.getString("passwordHash"),
      roles = Option(d.getString("roles")).getOrElse("").split(',').filter(_.nonEmpty).toList,
      tokenVersion = d.getInteger("tokenVersion", 1),
      createdAt = d.getLong("createdAt", 0L)
    )

  def findByUsername(username: String): Future[Option[UserDoc]] =
    users.find(equal("username", username)).toFuture().map(_.headOption.map(fromDoc))

  def insert(user: UserDoc): Future[Unit] =
    users.insertOne(toDoc(user)).toFuture().map(_ => ())

  def updatePassword(username: String, newPasswordHash: String, newTokenVersion: Int): Future[Unit] =
    users.updateOne(equal("username", username),
      Updates.combine(Updates.set("passwordHash", newPasswordHash), Updates.set("tokenVersion", newTokenVersion)))
      .toFuture().map(_ => ())
}
