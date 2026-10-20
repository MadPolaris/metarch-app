package net.imadz.metarch.infrastructure.graph

import scala.util.Try

/**
 * GraphConnectionConfig（S2-33·2026-09-03）：
 * Neo4j 连接配置单点——口令不入代码、不入库（git 历史泄露教训：硬编码 18 天可见）。
 * 取值优先级：conf `graph.neo4j.*` > 环境变量 NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD > uri/user 合理缺省。
 * password **无缺省**：缺失即 fail-fast（lazy driver 首次初始化时炸出，带修复指引）——
 * 历史口令已入 git 历史，轮换前本机可临时设 NEO4J_PASSWORD=<当前容器口令>。
 */
object GraphConnectionConfig {

  private lazy val cfg = com.typesafe.config.ConfigFactory.load()

  private def confOrEnv(confKey: String, envKey: String, default: Option[String]): String = {
    val fromConf = Try(cfg.getString(confKey)).toOption.filter(v => v != null && v.trim.nonEmpty)
    val fromEnv = sys.env.get(envKey).filter(v => v.trim.nonEmpty)
    fromConf.orElse(fromEnv).orElse(default).getOrElse {
      throw new IllegalArgumentException(
        s"[GraphConnectionConfig] Neo4j [$confKey] 未配置——请在 conf/application.conf 的 graph.neo4j 段配置，" +
          s"或设置环境变量 $envKey（S2-33：口令已从代码退役；历史泄露口令请轮换）"
      )
    }
  }

  lazy val uri: String = confOrEnv("graph.neo4j.uri", "NEO4J_URI", Some("bolt://127.0.0.1:7687"))
  lazy val user: String = confOrEnv("graph.neo4j.user", "NEO4J_USER", Some("neo4j"))
  lazy val password: String = confOrEnv("graph.neo4j.password", "NEO4J_PASSWORD", None)

  /** 统一 driver 构造（两处消费方共用同一池参数——抽取自原两处同构代码） */
  def newDriver(): org.neo4j.driver.Driver = org.neo4j.driver.GraphDatabase.driver(
    uri,
    org.neo4j.driver.AuthTokens.basic(user, password),
    org.neo4j.driver.Config.builder().withMaxConnectionPoolSize(5).build()
  )
}
