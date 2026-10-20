package net.imadz.metarch.infrastructure.graph

import net.imadz.metarch.infrastructure.graph.GraphMutationExecutor.ensureConstraints
import net.imadz.metarch.infrastructure.repositories.projection.ConceptEdgeGraphViewRepositoryImpl
import net.imadz.metarch.infrastructure.repositories.projection.ConceptGraphViewRepositoryImpl
import net.imadz.metarch.infrastructure.repositories.projection.ContextGraphViewRepositoryImpl
import net.imadz.metarch.infrastructure.repositories.projection.NoteGraphViewRepositoryImpl
import net.imadz.metarch.infrastructure.repositories.projection.QuestionGraphViewRepositoryImpl

import scala.jdk.CollectionConverters._

/**
 * 图投影链路隔离冒烟（EP_Neo4j_Graph_Projection 验证门的工具化，2026-08-24；
 * S2-65 批E 2026-09-07 改道）：真实事件载荷 → 生成范式 ④Impl（GraphMutationExecutor
 * 静态委托·结构烧录）→ 真 Neo4j。旧手写映射件（DomainEventGraphMappings）已随
 * S2-65 原子退役——本工具改走生成件，顺带冒烟 ③接口面值绑定→④结构烧录全链。
 * 不经 Akka/journal——eventsByTag 管线由生成 ⑤Bootstrap（ShardedDaemonProcess）承载，
 * 此处只验最险的「值绑定→结构烧录→PO→Cypher→图」段。
 *
 * 用法：
 *   sbt "runMain net.imadz.metarch.infrastructure.graph.GraphSmokeMain"            // 写入 smoke-* 数据并打印统计
 *   sbt "runMain net.imadz.metarch.infrastructure.graph.GraphSmokeMain --cleanup"  // 清除全部 smoke-* 节点
 */
object GraphSmokeMain {

  private val Prefix = "smoke-"

  def main(args: Array[String]): Unit = {
    if (args.contains("--cleanup")) { cleanup(); println("[smoke] cleanup done"); return }

    ensureConstraints()

    // ④Impl 零构造器注入（object 单例静态委托·S2-65 批C 裁决）——直接 new 即全链
    val conceptRepo = new ConceptGraphViewRepositoryImpl
    val edgeRepo = new ConceptEdgeGraphViewRepositoryImpl
    val contextRepo = new ContextGraphViewRepositoryImpl
    val noteRepo = new NoteGraphViewRepositoryImpl
    val questionRepo = new QuestionGraphViewRepositoryImpl

    // 与旧手写映射件 DomainEventGraphMappings 同语义（退役件 L44-68 翻译·kn:/ws: 前缀烧在 Impl）
    val batches: Seq[(String, () => Unit)] = Seq(
      "ConceptCreated c1" -> (() => conceptRepo.upsertNode(s"${Prefix}c1", Map(
        "name" -> "SmokeA", "kind" -> "AGGREGATE", "description" -> "smoke test A",
        "contextId" -> s"${Prefix}ctx", "sourceStage" -> "smoke", "machineSpec" -> "", "metadata" -> "",
        "tenantId" -> s"${Prefix}tenant", "status" -> "ESTABLISHED"))),
      "ConceptCreated c2" -> (() => conceptRepo.upsertNode(s"${Prefix}c2", Map(
        "name" -> "SmokeB", "kind" -> "ENTITY", "description" -> "smoke test B",
        "contextId" -> s"${Prefix}ctx", "sourceStage" -> "smoke", "machineSpec" -> "", "metadata" -> "",
        "tenantId" -> s"${Prefix}tenant", "status" -> "ESTABLISHED"))),
      "RelationCreated e1" -> (() => {
        edgeRepo.upsertNode(s"${Prefix}e1", Map(
          "relationType" -> "causedBy", "condition" -> "", "mapping" -> "", "order" -> "1",
          "metadata" -> "", "tenantId" -> s"${Prefix}tenant"))
        edgeRepo.mergeEdge("FROM", s"${Prefix}e1", s"${Prefix}c1", s"${Prefix}tenant")
        edgeRepo.mergeEdge("TARGET", s"${Prefix}e1", s"${Prefix}c2", s"${Prefix}tenant")
      }),
      "ContextDiscovered ctx" -> (() => contextRepo.upsertNode(s"${Prefix}ctx", Map(
        "name" -> "SmokeContext", "description" -> "smoke ctx", "tenantId" -> s"${Prefix}tenant"))),
      "NoteCaptured n1" -> (() => noteRepo.upsertNode(s"${Prefix}n1", Map(
        "type" -> "EVENT", "name" -> "smoke", "content" -> "smoke note", "sessionId" -> s"${Prefix}s1",
        "boardId" -> "", "externalId" -> "", "timeline" -> "1", "pivotal" -> "true",
        "tenantId" -> s"${Prefix}tenant"))),
      "QuestionRaised q1" -> (() => questionRepo.upsertNode(s"${Prefix}q1", Map(
        "status" -> "raised", "content" -> "smoke?", "sessionId" -> s"${Prefix}s1",
        "tenantId" -> s"${Prefix}tenant")))
    )

    batches.foreach { case (label, run) =>
      run()
      println(s"[smoke] applied: $label")
    }
    printStats()
  }

  private def param(v: String): java.util.Map[String, AnyRef] = {
    val m = new java.util.HashMap[String, AnyRef]()
    m.put("p", v); m
  }

  private def printStats(): Unit = {
    val s = GraphMutationExecutor.sessionForStats()
    try {
      val r = s.readTransaction(tx => tx.run(
        "MATCH (n) WHERE n.uid CONTAINS $p RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC",
        param(Prefix)).list().asScala)
      r.foreach(rec => println(f"[smoke] node ${rec.get("label").asString}%-20s x${rec.get("cnt").asInt}"))
      val edges = s.readTransaction(tx => tx.run(
        """MATCH (a)-[r]->(b) WHERE a.uid CONTAINS $p AND b.uid CONTAINS $p
          |RETURN type(r) AS t, count(*) AS cnt""".stripMargin,
        param(Prefix)).list().asScala)
      edges.foreach(rec => println(f"[smoke] edge  ${rec.get("t").asString}%-20s x${rec.get("cnt").asInt}"))
    } finally s.close()
  }

  private def cleanup(): Unit = {
    ensureConstraints()
    val s = GraphMutationExecutor.sessionForStats()
    try {
      val deleted = s.writeTransaction(tx => tx.run(
        "MATCH (n) WHERE n.uid CONTAINS $p DETACH DELETE n RETURN count(*) AS c",
        param(Prefix)).single().get("c").asInt())
      println(s"[smoke] deleted $deleted nodes")
    } finally s.close()
  }
}
