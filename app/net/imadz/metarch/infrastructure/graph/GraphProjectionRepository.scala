package net.imadz.metarch.infrastructure.graph

import org.neo4j.driver.Session

import scala.jdk.CollectionConverters._

/**
 * GraphProjectionRepository（EP_Neo4j_Graph_Projection B1，2026-08-24）：
 * 图投影写侧——通用节点/边 MERGE（mergeNode/mergeEdge，B2 handler 双写底层）。
 * 幂等（MERGE），与 at-least-once 投影语义兼容。
 * MySQL DecisionView 为权威读侧；本仓库为派生图视图（可全量重建）。
 * object 单例（B2 决策）：handler 构造器零改动接入；driver 生命周期=JVM。
 * 2026-08-27：移除 5 个固定形态死代码 helper（mergeConcept/Context/Subdomain/
 * ContextRelation/ConceptEdge，无调用方），统一走 mergeNode/mergeEdge 复合键。
 */
object GraphProjectionRepository {

  /** 懒加载：与 GraphMutationExecutor 一致，纯查询辅助单测不触发 driver 初始化。
   * 2026-09-03 S2-33：连接参数收敛到 GraphConnectionConfig（口令退役出代码）。 */
  private lazy val driver = GraphConnectionConfig.newDriver()

  private def write(cypherRaw: String, params: java.util.Map[String, AnyRef]): Unit = {
    // $$ 占位 → $（规避 Scala s-interpolator 对字面 $ 的转义限制）
    val cypher = cypherRaw.replaceAll("\\$\\$", "\\$")
    val session: Session = driver.session()
    try session.writeTransaction(tx => tx.run(cypher, params).consume())
    finally session.close()
  }

  // ── B2a：继承补戳（与执行器同口径；backfill/DecisionView 双写路径） ──────────

  /** 图内既有节点 tenantId 读取（多键精确匹配；查无/缺属性 ⇒ None） */
  private def inheritTenantStamp(label: String, keys: Map[String, AnyRef]): Option[String] = {
    val ex = GraphMutationExecutor
    if (ex.enforcement == ex.StampEnforcement.Off || !ex.requiredLabels.contains(label)) return None
    val keyPattern = keys.keys.map(k => s"`$k`: $$inh_$k").mkString(", ")
    val q = s"MATCH (n:`$label` {$keyPattern}) RETURN n.${ex.TenantKey} AS t LIMIT 1"
    val p = new java.util.HashMap[String, AnyRef]()
    keys.foreach { case (k, v) => p.put(s"inh_$k", v.asInstanceOf[AnyRef]) }
    val s = driver.session()
    try {
      val rec = s.readTransaction { tx =>
        val cur = tx.run(q, p)
        if (cur.hasNext()) cur.next() else null
      }
      if (rec == null) None else {
        val v = rec.get("t")
        if (v == null || v.isNull) None else Option(v.asString()).filter(_.trim.nonEmpty)
      }
    } finally s.close()
  }

  /** 缺戳则继承，返回最终写入 props */
  private def withInheritedStamp(label: String, keys: Map[String, AnyRef], props: Map[String, AnyRef]): Map[String, AnyRef] = {
    if (!GraphMutationExecutor.inheritanceEligible(label, props.map { case (k, v) => k -> String.valueOf(v) }))
      return props
    inheritTenantStamp(label, keys)
      .map(t => props.updated(GraphMutationExecutor.TenantKey, t))
      .getOrElse(props)
  }

  /**
   * 通用节点 MERGE（B2 handler 双写底层）：keys 组成匹配键，props 全量 SET（EP_Neo4j §二.2）；
   * B2a 继承补戳＋B2c 空戳剥除。形态2：props 带有效 tenantId 时并入匹配键（复合键，同 uuid 跨租户分节点）。
   */
  def mergeNode(label: String, keys: Map[String, AnyRef], props0: Map[String, AnyRef]): Unit = {
    val stripped = if (props0.get(GraphMutationExecutor.TenantKey)
        .exists(v => v == null || v.toString.trim.isEmpty)) props0 - GraphMutationExecutor.TenantKey
      else props0
    val props = withInheritedStamp(label, keys, stripped)
    val tid = props.get(GraphMutationExecutor.TenantKey)
      .flatMap(v => Option(v).map(_.toString).filter(_.trim.nonEmpty))
    val mergeKeys = tid.fold(keys)(t => keys + (GraphMutationExecutor.TenantKey -> t))
    val keyPattern = mergeKeys.keys.map(k => s"$k: $$k").mkString(", ")
    val cypher = "MERGE (n:" + label + " {" + keyPattern + "}) SET n += $graphProps"
    val params = new java.util.HashMap[String, AnyRef]()
    (mergeKeys ++ props).foreach { case (k, v) => params.put(k, v: AnyRef) }
    params.put("graphProps", props.asJava)
    write(cypher, params)
  }

  /** 通用边 MERGE：两端节点自动补建（ON CREATE 占位），关系属性 SET；形态2：边带租户时端点限定 */
  def mergeEdge(relType: String, relId: String, fromLabel: String, fromKeyId: String, fromId: String,
                toLabel: String, toKeyId: String, toId: String,
                props: Map[String, AnyRef]): Unit = {
    val tid = props.get(GraphMutationExecutor.TenantKey)
      .flatMap(v => Option(v).map(_.toString).filter(_.trim.nonEmpty))
    val tenantClause = tid.fold("")(_ => " AND n.tenantId = $tid")
    val cypher =
      "MERGE (f:" + fromLabel + " {" + fromKeyId + ": $fromId}) WHERE 1=1" + tenantClause +
      " MERGE (t:" + toLabel + " {" + toKeyId + ": $toId}) WHERE 1=1" + tenantClause +
      " MERGE (f)-[r:" + relType + " {relationId: $relationId}]->(t)" +
      " SET r += $edgeProps"
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("fromId", fromId)
    params.put("toId", toId)
    params.put("relationId", relId)
    tid.foreach(t => params.put("tid", t))
    params.put("edgeProps", props.asJava)
    write(cypher, params)
  }

  /** 边属性更新（端点不可变语义：Defined 定死端点，Updated 只改属性） */
  def updateEdgeProps(relType: String, relId: String, props: Map[String, AnyRef]): Unit = {
    val cypher = "MATCH ()-[r:" + relType + " {relationId: $relationId}]->() SET r += $edgeProps"
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("relationId", relId)
    params.put("edgeProps", props.asJava)
    write(cypher, params)
  }

  /** 节点删除（Removed 类事件）：DETACH DELETE */
  def deleteNode(label: String, keyName: String, keyValue: String): Unit =
    write("MATCH (n:" + label + " {" + keyName + ": $keyValue}) DETACH DELETE n",
      java.util.Map.of[String, AnyRef]("keyValue", keyValue))

  /** 只读查询（B3 查询端点/回填校验）：返回 node 属性 map 列表 */
  def queryList(cypher: String, params: java.util.Map[String, AnyRef] = new java.util.HashMap()): List[Map[String, AnyRef]] = {
    val session: Session = driver.session()
    try session.readTransaction(tx => tx.run(cypher, params).list().asScala.toList.map(rec =>
      rec.asMap().asScala.toMap.map { case (k, v) => k -> deepToScala(v).asInstanceOf[AnyRef] }))
    finally session.close()
  }

  /**
   * BUG-009（2026-08-25）：driver 4.4 Record.asMap() 只解包顶层，嵌套值
   * （properties(n)/collect(...) 等 MAP/LIST 型）原样是 java.util.LinkedHashMap/List，
   * 控制器侧 asInstanceOf[Map[List[...]]] 会 ClassCastException ⇒ /api/graph/export 500。
   * 递归落成 Scala 形态；标量/Temporal/Spatial 原样透传。
   */
  private def deepToScala(v: Any): Any = v match {
    case m: java.util.Map[_, _] =>
      m.asScala.toMap.map { case (k, vv) => String.valueOf(k) -> deepToScala(vv) }
    case l: java.util.List[_] =>
      l.asScala.toList.map(deepToScala)
    case other => other
  }

  /** 图统计（冒烟断言用）：各 label 节点计数 */
  def stats(): Map[String, Long] =
    queryList("MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt")
      .flatMap(m => m.get("label").map(l => l.toString -> m.get("cnt").map(_.asInstanceOf[Long]).getOrElse(0L)))
      .toMap

  /**
   * BUG-011（2026-08-26 Squad-B）：graph 读端点租户隔离——可见性规则（保守口径，待产品最终确认）：
   * 节点/关系带 tenantId 属性 ⇒ 仅当其值 ∈ 调用者可见租户集才返回；
   * 无 tenantId 属性的节点/关系（如 Ws* 工作坊过程元数据）视为全局共享，对所有人可见。
   * 空租户集 ⇒ 默认拒绝（仅全局节点可见）。JWT 无租户声明，归属由服务端解析
   * （uid → OnboardTenantDecisionView：ownerUserId=uid 或 orgMemberIds 含 orgMemberIdOf(tenantId, uid)）。
   */
  def tenantScopeParams(visibleTenants: Set[String]): java.util.Map[String, AnyRef] =
    java.util.Map.of[String, AnyRef]("tenants", visibleTenants.asJava)

  /** 单别名可见性谓词（B1 过渡版：tenantId NULL=全局共享）。S2-22 拍板后读面已切 strict——本谓词保留备查（元模型独立访问功能另批），勿再用于租户读端点 */
  def tenantScopePred(alias: String): String =
    "(" + alias + ".tenantId IS NULL OR " + alias + ".tenantId IN $tenants)"

  /** strict 谓词（S2-22 收权·2026-08-30 用户拍板）：tenantId 缺失＝元模型（Ws* 来自真源的板书投影，属独立只读访问功能域，暂缓实施）——对租户 token 一律不可见（default-deny）。
    * 业务节点 100% 带 tenantId（Neo4j 实证 24/24 等），strict 不误伤。多别名（边双端）由调用方 AND 组合。
    * 边（relationship）不携带 tenantId 属性（评审 B-1·2026-08-30 D4 读侧放宽）：隔离由端点别名承载，勿对边别名用本谓词。 */
  def tenantScopePredStrict(alias: String): String =
    "(" + alias + ".tenantId IS NOT NULL AND " + alias + ".tenantId IN $tenants)"

  /** kn 空间节点键（§6.8 双空间本体：知识资产=kn:，工作坊过程=ws:）——投影写侧 DomainEventGraphMappings.kn 同源。
    * 2026-08-30 对齐修复：读端点按 id 查询必须用本前缀（裸 uuid 恒 MISS → VIEW_ITEM_NOT_FOUND），backfill 同前缀写。 */
  def knKey(id: String): String = "kn:" + id

  /** 租户作用域图统计：各 label 节点计数（strict 谓词——元模型（无 tenantId）对租户 token 不可见·S2-22）；缺戳审计仅余元模型自有 NULL（设计态，非缺戳） */
  def statsScoped(visibleTenants: Set[String]): Map[String, Long] = {
    auditNullTenantStamps()
    queryList(
      "MATCH (n) WHERE " + tenantScopePredStrict("n") + " RETURN labels(n)[0] AS label, count(*) AS cnt",
      tenantScopeParams(visibleTenants))
      .flatMap(m => m.get("label").map(l => l.toString -> m.get("cnt").map(_.asInstanceOf[Long]).getOrElse(0L)))
      .toMap
  }

  // ── B1：缺租户戳存量审计（过渡期只告警不改行为；B2 补齐后据此切 default-deny） ──

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  /** 业务 label 清单：这些 label 出现无 tenantId 节点即视为待补戳存量（Ws* 过程元数据暂不在列） */
  private val AuditedLabels = Seq("Concept", "Catalog", "Context", "Subdomain",
    "ConceptEdge", "ContextRelationNode")

  /** 各业务 label 下无 tenantId 戳的节点计数；非空则 WARN（GRAPH_TENANT_AUDIT） */
  private[graph] def auditNullTenantStamps(): Map[String, Long] = {
    val missing = queryList(
      "MATCH (n) WHERE labels(n)[0] IN $labels AND (n.tenantId IS NULL OR n.tenantId = '') " +
        "RETURN labels(n)[0] AS label, count(*) AS cnt",
      java.util.Map.of[String, AnyRef]("labels", AuditedLabels.asJava))
      .flatMap(m => m.get("label").map(l => l.toString -> m.get("cnt").map(_.asInstanceOf[Long]).getOrElse(0L)))
      .toMap
    if (missing.nonEmpty)
      log.warn("GRAPH_TENANT_AUDIT business nodes without tenantId stamp: {}", missing)
    missing
  }
}
