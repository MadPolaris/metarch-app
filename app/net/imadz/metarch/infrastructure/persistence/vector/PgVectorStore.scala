package net.imadz.metarch.infrastructure.persistence.vector

import java.sql.{ Connection, DriverManager }
import javax.inject.{ Inject, Singleton }
import play.api.Configuration

/**
 * PgVectorStore（M3 向量读模型批E·2026-09-07）：pgvector 手写基建——HTTP/连接/池逻辑全在此，
 * 生成侧只机械发射调用（范式裁决：投影内同步嵌入＝bge-m3 1024d·HNSW cosine）。
 *
 * 生成侧契约（ProjectionRepositoryImplBuilder.buildVector 发射的四个机械调用·命名参数严格对齐）：
 *   upsert(table, columns, embeddingColumn, embedding, keyColumns)
 *   delete(table, sourceType, sourceId)                      // view.delete 折叠·sourceId 全局唯一
 *   findByKeys(table, keys, columns) → Option[Map[String, Any]]
 *   search(table, tenantId, queryEmbedding, k, sourceType, columns) → List[Map[String, Any]]
 *
 * 连接策略＝每次调用短连接（DriverManager）：检索 QPS 低、嵌入才是瓶颈；避免引入第二套池。
 * 租户门＝search 恒等 `tenant_id = ?` 硬条件（tid 由服务端 token 解析，客户端不可伪造范围）。
 * 表名/列名均来自生成侧 snake_case 白名单标识符（无拼接注入面）；值一律 PreparedStatement 绑定。
 *
 * ⚠️ 手写领地（writeTerritorial 之外）：生成器不覆写本文件；DDL 部署面＝conf/sql/5-pgvector.sql
 * （gen_projection_ddl --vector-out 产物·幂等 IF NOT EXISTS）；ensureSchema 为程序化等价通道（Spec/冒烟用）。
 */
@Singleton
class PgVectorStore @Inject() (config: Configuration) {

  private val pgConf = config.get[Configuration]("vector.pg")
  private val url = pgConf.get[String]("url")
  private val user = pgConf.get[String]("user")
  private val password = pgConf.get[String]("password")
  private val embeddingColumn = pgConf.get[String]("embedding-column")

  // 驱动装载（幂等）：容器内 PG 16 + pgvector 0.8（compose metarch-pg·宿主 5433）
  locally {
    Class.forName(pgConf.get[String]("driver"))
  }

  private def withConnection[A](f: Connection => A): A = {
    val conn = DriverManager.getConnection(url, user, password)
    try f(conn)
    finally conn.close()
  }

  private def vecLiteral(v: Array[Float]): String = v.mkString("[", ",", "]")

  // ── 生成侧契约四方法 ────────────────────────────────────────────────────────

  /** 行 upsert（INSERT ... ON CONFLICT (keyColumns) DO UPDATE）：嵌入由 Impl 侧先行计算传入。
    * columns＝(snake 列名 → 值) 全字段；keyColumns＝复合键列（与 DDL PRIMARY KEY 同序同集）。 */
  def upsert(
      table: String,
      columns: Seq[(String, Any)],
      embeddingColumn: String,
      embedding: Array[Float],
      keyColumns: Seq[String]): Unit = withConnection { conn =>
    val nonKeyCols = columns.map(_._1).filterNot(keyColumns.contains)
    val colList = (columns.map(_._1) :+ embeddingColumn).mkString(", ")
    val placeholders = (columns.map(_ => "?") :+ "?::vector").mkString(", ")
    val updates = (nonKeyCols :+ embeddingColumn).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    val sql =
      s"INSERT INTO $table ($colList) VALUES ($placeholders) ON CONFLICT (${keyColumns.mkString(", ")}) DO UPDATE SET $updates"
    val ps = conn.prepareStatement(sql)
    try {
      columns.zipWithIndex.foreach { case ((_, v), i) => ps.setObject(i + 1, v.asInstanceOf[AnyRef]) }
      ps.setString(columns.size + 1, vecLiteral(embedding))
      ps.executeUpdate()
    } finally ps.close()
  }

  /** 删除事件折叠（view.delete）：按 (source_type, source_id) 删行——sourceId 全局唯一，
    * 租户门由行键承载（upsert 时写入的 tenant_id 键列）。 */
  def delete(table: String, sourceType: String, sourceId: String): Unit = withConnection { conn =>
    val ps = conn.prepareStatement(s"DELETE FROM $table WHERE source_type = ? AND source_id = ?")
    try {
      ps.setString(1, sourceType)
      ps.setString(2, sourceId)
      ps.executeUpdate()
    } finally ps.close()
  }

  /** 复合键等值查询：键/列均为 snake_case 列名；行值以 rs.getObject 透出（生成侧 rowToView 负责类型落位）。 */
  def findByKeys(table: String, keys: Map[String, Any], columns: Seq[String]): Option[Map[String, Any]] =
    withConnection { conn =>
      val where = keys.keys.map(k => s"$k = ?").mkString(" AND ")
      val ps = conn.prepareStatement(s"SELECT ${columns.mkString(", ")} FROM $table WHERE $where")
      try {
        keys.values.zipWithIndex.foreach { case (v, i) => ps.setObject(i + 1, v.asInstanceOf[AnyRef]) }
        val rs = ps.executeQuery()
        try if (rs.next()) Some(rowMap(rs, columns)) else None
        finally rs.close()
      } finally ps.close()
    }

  /** 语义近邻检索：tenant_id 硬门（恒等条件·不可省）＋可选 sourceType 过滤；
    * 距离算子 <=>（cosine）走 HNSW 索引（DDL vector_cosine_ops 同源）；k 由生成侧钳位 1..50。 */
  def search(
      table: String,
      tenantId: String,
      queryEmbedding: Array[Float],
      k: Int,
      sourceType: Option[String],
      columns: Seq[String]): List[Map[String, Any]] = withConnection { conn =>
    val stCond = sourceType.filter(_.nonEmpty).map(_ => " AND source_type = ?").getOrElse("")
    val sql = s"SELECT ${columns.mkString(", ")} FROM $table WHERE tenant_id = ?$stCond " +
      s"ORDER BY $embeddingColumn <=> ?::vector LIMIT ?"
    val ps = conn.prepareStatement(sql)
    try {
      var idx = 0
      ps.setString({ idx += 1; idx }, tenantId)
      if (stCond.nonEmpty) ps.setString({ idx += 1; idx }, sourceType.get)
      ps.setString({ idx += 1; idx }, vecLiteral(queryEmbedding))
      ps.setInt({ idx += 1; idx }, k)
      val rs = ps.executeQuery()
      try {
        val buf = List.newBuilder[Map[String, Any]]
        while (rs.next()) buf += rowMap(rs, columns)
        buf.result()
      } finally rs.close()
    } finally ps.close()
  }

  // ── 程序化建表（部署面等价通道；DDL 权威＝conf/sql/5-pgvector.sql） ──────────

  /** 幂等建表/建索引：columns＝(列名 → SQL 类型)；distance ∈ cosine|l2|ip。Spec/冒烟用；
    * 生产部署走 5-pgvector.sql（同源生成）。 */
  def ensureSchema(
      table: String,
      columns: Seq[(String, String)],
      keyColumns: Seq[String],
      embeddingColumn: String,
      dims: Int,
      distance: String): Unit = withConnection { conn =>
    def exec(sql: String): Unit = {
      val st = conn.createStatement()
      try st.executeUpdate(sql)
      finally st.close()
    }
    exec("CREATE EXTENSION IF NOT EXISTS vector")
    val colDefs = columns.map { case (c, t) => s"$c $t" } :+ s"$embeddingColumn vector($dims) NOT NULL"
    val pk = if (keyColumns.nonEmpty) Seq(s"PRIMARY KEY (${keyColumns.mkString(", ")})") else Nil
    exec(s"CREATE TABLE IF NOT EXISTS $table (${(colDefs ++ pk).mkString(", ")})")
    exec(s"CREATE INDEX IF NOT EXISTS idx_${table}_tenant ON $table (${keyColumns.headOption.getOrElse("tenant_id")})")
    exec(s"CREATE INDEX IF NOT EXISTS idx_${table}_$embeddingColumn ON $table USING hnsw " +
      s"($embeddingColumn ${HnswOps.of(distance)})")
  }

  private def rowMap(rs: java.sql.ResultSet, columns: Seq[String]): Map[String, Any] =
    columns.map(c => c -> rs.getObject(c)).toMap
}

/** 距离 → HNSW 算子类（与 gen_projection_ddl pgvector 方言同源映射） */
private[vector] object HnswOps {
  private val map = Map("cosine" -> "vector_cosine_ops", "l2" -> "vector_l2_ops", "ip" -> "vector_ip_ops")
  def of(distance: String): String = map.getOrElse(distance.toLowerCase,
    throw new IllegalArgumentException(s"不支持的向量距离度量：$distance（词表 cosine|l2|ip）"))
}
