package net.imadz.metarch.infrastructure.graph

import com.typesafe.config.ConfigException
import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/**
 * GraphMutation 通用执行层（EP_Neo4j_Graph_Projection §6.5 Cypher 契约，2026-08-24）。
 *
 * 与算法内核（backend-codegen evtgraph 包）的 GraphMutation DSL 同构；内核为纯 Scala
 * 参考实现并承载幂等门测试，本文件为 Bolt 翻译器。两侧语义契约锚定于内核
 * InMemoryGraphState scaladoc：Upsert 合并保留未提及属性 / MergeEdge 前置端点校验 /
 * DeleteNode = DETACH DELETE / UpdateProps 仅覆盖给出的键（目标缺失 Strict=no-op，
 * CollectConflicts=记冲突）。
 *
 * 注入安全边界：label/relType/keyName 一律来自映射注册表白名单常量且经 [[ident]] 校验；
 * 仅值走参数化。
 *
 * 统一节点戳（§6.5）：UpsertNode 自动补 uid/label；stage/stageIdx 由调用方（投影器按
 * StageVocabulary 折算）放入 props 一并写入。位点推进须由调用方与本调用同事务包裹（§6.5）。
 */
object GraphMutationExecutor {

  // ── 模型（与算法内核同构） ──────────────────────────────────────────────────

  sealed trait GraphMutation

  /** 变更来源戳（EP_KnowledgePatch_Tenant_Isolation §3.6 防环：沿链单调传递） */
  object Origin {
    final val Workshop = "WORKSHOP"
    final val Patch    = "PATCH"
    final val Rebuild  = "REBUILD"
  }

  final case class UpsertNode(label: String, keyName: String, keyValue: String,
                              props: Map[String, Any] = Map.empty,
                              origin: String = Origin.Workshop) extends GraphMutation

  final case class MergeEdge(relType: String,
                             fromLabel: String, fromKeyName: String, fromKeyValue: String,
                             toLabel: String, toKeyName: String, toKeyValue: String,
                             props: Map[String, Any] = Map.empty,
                             origin: String = Origin.Workshop) extends GraphMutation

  /**
   * expectedBaseVersion 定义时走乐观锁 CAS（EP §3.6）：WHERE coalesce(n._v,0)=$baseV；
   * touched=0 ⇒ Strict 抛 VersionConflictException / CollectConflicts 记 VERSION_CONFLICT。
   * 未定义时保持既有 UPDATE_TARGET_MISSING 语义。
   */
  final case class UpdateNodeProps(label: String, keyName: String, keyValue: String,
                                   props: Map[String, Any] = Map.empty,
                                   removeKeys: Seq[String] = Nil,
                                   expectedBaseVersion: Option[Long] = None,
                                   origin: String = Origin.Workshop) extends GraphMutation

  /** tenantId 可选：形态2 复合键下删除/定位需租户限定（缺省=存量过渡回落单键） */
  final case class DeleteNode(label: String, keyName: String, keyValue: String,
                              tenantId: Option[String] = None) extends GraphMutation

  sealed trait ExecutionMode
  object ExecutionMode {
    case object Strict extends ExecutionMode           // 悬空抛 DanglingEndpointException → 停车场重试（时序问题）
    case object CollectConflicts extends ExecutionMode // 悬空聚合为 Conflict（语义交集，需人修）
  }

  final case class GraphConflict(criterion: String, severity: String,
                                 description: String, refs: Map[String, String])

  final case class DanglingEndpointException(msg: String) extends RuntimeException(msg)

  // ── 租户验戳与版本冲突（EP_KnowledgePatch_Tenant_Isolation B1，2026-08-26） ──

  final case class MissingTenantStampException(msg: String) extends RuntimeException(msg)

  final case class CrossTenantEdgeException(msg: String) extends RuntimeException(msg)

  final case class VersionConflictException(msg: String) extends RuntimeException(msg)

  /**
   * 验戳模式（graph.tenant.enforcement，默认 warn 过渡期）：
   * off=关闭；warn=缺戳仅告警；strict=缺戳 fail-fast（B2 载荷补齐后由配置切换，不改代码）。
   * 强制范围 graph.tenant.required-labels 默认 Concept/Catalog——两者事件载荷已带 tenantId，
   * 其余 label 等 B2 补齐后再入列。
   */
  sealed trait StampEnforcement extends Product with Serializable
  object StampEnforcement {
    case object Off    extends StampEnforcement
    case object Warn   extends StampEnforcement
    case object Strict extends StampEnforcement
    def of(raw: String): StampEnforcement =
      Option(raw).map(_.trim.toLowerCase) match {
        case Some("off")    => Off
        case Some("strict") => Strict
        case _              => Warn
      }
  }

  final val TenantKey = "tenantId"

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val cfg = com.typesafe.config.ConfigFactory.load()
  private[graph] lazy val enforcement: StampEnforcement =
    try StampEnforcement.of(cfg.getString("graph.tenant.enforcement"))
    catch { case _: ConfigException.Missing | _: ConfigException.WrongType => StampEnforcement.Warn }
  private[graph] lazy val requiredLabels: Set[String] =
    try cfg.getStringList("graph.tenant.required-labels").asScala.toSet
    catch {
      case _: ConfigException.Missing | _: ConfigException.WrongType =>
        // B2b：七个创建事件 PO 均已携带 tenantId（mappings 已消费）⇒ 全部节点 label 入强制清单；
        // ConceptEdge 例外——边不落租户戳，可见性由端点谓词推导（EP §3.3 决策）
        Set("Concept", "Catalog", "Context", "Subdomain",
            "ContextRelationNode", "Note", "Question")
    }

  /** 纯函数：required label 的 Upsert/Update 是否缺 tenantId 戳（null/空白视同缺失） */
  private[graph] def stampMissing(label: String, props: Map[String, Any]): Boolean =
    requiredLabels.contains(label) &&
      props.get(TenantKey).forall(v => v == null || strOf(v).trim.isEmpty)

  /**
   * 纯函数：跨租户连线判定（EP §3.3-③）。边声明租户 t 且任一可判定端点归属 ≠ t ⇒ 违规描述。
   * 端点或边未携带租户（存量过渡期）⇒ None 不判定。
   */
  private[graph] def crossTenantViolation(declared: Option[String],
                                          fromTenant: Option[String],
                                          toTenant: Option[String]): Option[String] =
    declared.filter(v => v != null && v.nonEmpty) match {
      case None => None
      case Some(t) =>
        val foreign = Seq(fromTenant, toTenant).flatten.filter(v => v != null && v.nonEmpty && v != t).distinct
        if (foreign.isEmpty) None
        else Some(s"edge declares tenant=$t but endpoint(s) carry ${foreign.mkString(",")}")
    }

  private def enforceStamp(label: String, props: Map[String, Any]): Unit =
    if (enforcement != StampEnforcement.Off && stampMissing(label, props)) {
      val msg = s"$label mutation without $TenantKey stamp"
      if (enforcement == StampEnforcement.Strict) throw MissingTenantStampException(msg)
      log.warn("GRAPH_TENANT_STAMP_MISSING {} mode=warn", msg)
    }

  /**
   * B2a 继承补戳判定（EP §3.7）：required label 且缺戳且验戳未关闭 ⇒ 尝试从图内既有节点继承。
   * 查无（全新节点）⇒ 保持缺失交回验戳口径。
   */
  private[graph] def inheritanceEligible(label: String, props: Map[String, Any]): Boolean =
    enforcement != StampEnforcement.Off && stampMissing(label, props)

  /**
   * B2c 空戳防护：tenantId 为 null/空白 ⇒ 从 props 剥除（视同缺失）。
   * 背景：A 案刀1 落了 proto 字段而刀2/3（发射点/converter）未填充前，po.tenantId=""；
   * 无条件消费会写出 tenantId="" 属性——读侧谓词下节点既非 NULL 也非命中 ⇒ 隐形。
   * 剥除后落入继承补戳/验戳的正常口径。
   */
  private[graph] def sanitizeTenantStamp(props: Map[String, Any]): Map[String, Any] =
    props.get(TenantKey).forall(v => v == null || strOf(v).trim.isEmpty) match {
      case true if props.contains(TenantKey) => props - TenantKey
      case _                                 => props
    }

  /** 图内既有节点 tenantId 读取（只读；节点不存在或属性缺失 ⇒ None） */
  private def inheritTenantStamp(label: String, keyName: String, keyValue: String): Option[String] = {
    val l = ident("label", label); val k = ident("keyName", keyName)
    val s = session()
    try {
      val rec = s.readTransaction { tx =>
        val cur = tx.run(s"MATCH (n:`$l` {$k: $$keyValue}) RETURN n.$TenantKey AS t LIMIT 1",
          map1("keyValue", keyValue))
        if (cur.hasNext()) cur.next() else null
      }
      if (rec == null) None else {
        val v = rec.get("t")
        if (v == null || v.isNull) None else Option(v.asString()).filter(_.trim.nonEmpty)
      }
    } finally s.close()
  }

  /** 缺戳则继承，返回最终应写入的 props */
  private def withInheritedStamp(label: String, keyName: String, keyValue: String,
                                 props: Map[String, Any]): Map[String, Any] =
    if (inheritanceEligible(label, props))
      inheritTenantStamp(label, keyName, keyValue)
        .map(t => props.updated(TenantKey, t))
        .getOrElse(props)
    else props

  // ── 标识符白名单校验 ────────────────────────────────────────────────────────

  private val Ident = "^[A-Za-z_][A-Za-z0-9_]*$"
  private def ident(what: String, s: String): String = {
    require(s != null && s.matches(Ident), s"illegal $what identifier: $s")
    s
  }

  // ── Phase 2 Any 通道（B 组装补丁 2026-10-20）：props 放宽 Map[String, Any] ──
  // 值转换口径：String 原样；Boolean/数值装箱为 Neo4j 原生标量（R9 类型化意图）；
  // Id(UUID) 与 Instant 按 Phase 1 字符串通道同形落串（Id 与节点键同为字符串形、
  // Instant 存 ISO-8601 串——Neo4j 属性无 Instant/UUID 原生型）；Seq 递归转 java List。
  private def strOf(v: Any): String = if (v == null) null else v.toString

  private def tenantOf(props: Map[String, Any]): Option[String] =
    props.get(TenantKey).map(strOf).filter(v => v != null && v.trim.nonEmpty)

  private def neoValue(v: Any): AnyRef = v match {
    case null             => null
    case s: String        => s
    case b: Boolean       => Boolean.box(b)
    case i: Int           => Int.box(i)
    case l: Long          => Long.box(l)
    case d: Double        => Double.box(d)
    case f: Float         => Double.box(f.toDouble)
    case bd: BigDecimal   => Double.box(bd.toDouble)
    case u: java.util.UUID => u.toString
    case t: java.time.Instant => t.toString
    case seq: Seq[_]      => seq.map(neoValue).asJava
    case other            => other.toString
  }

  private def strParams(props: Map[String, Any]): java.util.Map[String, AnyRef] =
    props.view.mapValues(v => neoValue(v)).toMap.asJava

  private def map1(k: String, v: AnyRef): java.util.Map[String, AnyRef] = {
    val m = new java.util.HashMap[String, AnyRef]()
    m.put(k, v); m
  }

  // ── Cypher 构造（纯函数，可单测） ───────────────────────────────────────────

  /**
   * UpsertNode：SET n += $props 保留未提及属性；自动盖 uid/label 戳；
   * 并盖版本戳 _v（单调递增）/ _origin（变更来源，EP §3.6 防环）。
   * 形态2（EP 风险0）：props 带有效 tenantId 时 MERGE 复合键 {key, tenantId}——同 uuid 跨租户
   * 成双节点，租户隔离进存储层；缺戳节点（存量过渡/非 required label）回落单键 MERGE。
   */
  def upsertNodeCypher(m: UpsertNode): (String, java.util.Map[String, AnyRef]) = {
    val l = ident("label", m.label); val k = ident("keyName", m.keyName)
    val tid = tenantOf(m.props)
    val keyPattern = tid match {
      case Some(_) => s"$k: $$keyValue, $TenantKey: $$tid"
      case None    => s"$k: $$keyValue"
    }
    val cypher =
      s"""MERGE (n:`$l` {$keyPattern})
         |  SET n += $$props, n.uid = $$keyValue, n.label = '$l',
         |      n._v = coalesce(n._v, 0) + 1, n._origin = $$origin""".stripMargin
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("keyValue", m.keyValue)
    if (tid.isDefined) params.put("tid", tid.get)
    params.put("props", strParams(m.props))
    params.put("origin", m.origin)
    (cypher, params)
  }

  /**
   * UpdateNodeProps：RETURN count(*) 让执行层感知「目标缺失」（Strict no-op / CollectConflicts 记冲突）；
   * expectedBaseVersion 存在时追加乐观锁守卫，touched=0 ⇒ 版本冲突（EP §3.6 CAS 模板）。
   * 形态2：props 带有效 tenantId 时 MATCH 加租户限定——复合键下同 uuid 双节点只命中本租户行。
   */
  def updateNodePropsCypher(m: UpdateNodeProps): (String, java.util.Map[String, AnyRef]) = {
    val l = ident("label", m.label); val k = ident("keyName", m.keyName)
    m.removeKeys.foreach(ident("removeKey", _))
    val removeClause = if (m.removeKeys.isEmpty) "" else " REMOVE " + m.removeKeys.map(kk => s"n.`$kk`").mkString(", ")
    val casGuard = m.expectedBaseVersion.fold("")(v => s" WHERE coalesce(n._v, 0) = $v")
    val tid = tenantOf(m.props)
    val tenantClause = tid match {
      case Some(_) => " AND " + s"n.$TenantKey = $$tid"
      case None    => ""
    }
    val whereClause = (casGuard + tenantClause) match {
      case ""  => ""
      case w   => " WHERE " + w.stripPrefix(" AND ")
    }
    val cypher =
      s"""MATCH (n:`$l` {$k: $$keyValue})$whereClause
         |  SET n += $$props$removeClause,
         |      n._v = coalesce(n._v, 0) + 1, n._origin = $$origin
         |RETURN count(n) AS touched""".stripMargin
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("keyValue", m.keyValue)
    if (tid.isDefined) params.put("tid", tid.get)
    params.put("props", strParams(m.props))
    params.put("origin", m.origin)
    (cypher, params)
  }

  /**
   * DeleteNode → DETACH DELETE。形态2：带租户限定（复合键下按业务键会命中多行），
   * tenantId 由调用方（mappings）从事件 PO 传入；缺失时回落单键删除（存量过渡）。
   */
  def deleteNodeCypher(m: DeleteNode): (String, java.util.Map[String, AnyRef]) = {
    val l = ident("label", m.label); val k = ident("keyName", m.keyName)
    val tenantClause = m.tenantId.filter(v => v != null && v.trim.nonEmpty)
      .fold("")(t => s" AND n.$TenantKey = $$tid")
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("keyValue", m.keyValue)
    m.tenantId.filter(v => v != null && v.trim.nonEmpty).foreach(t => params.put("tid", t))
    (s"MATCH (n:`$l` {$k: $$keyValue}) WHERE 1=1$tenantClause DETACH DELETE n", params)
  }

  /**
   * MergeEdge 端点预检（悬空即拒绝，镜像算法内核 requireNode）。
   * 同时带回两端点 tenantId 供跨租户连线判定（EP §3.3-③）；OPTIONAL MATCH 恒单行。
   * 形态2：边带有效租户时端点 MATCH 加租户限定——复合键下同 uuid 双节点只命中本租户端点。
   */
  def mergeEdgeCheckCypher(m: MergeEdge): (String, java.util.Map[String, AnyRef]) = {
    val fl = ident("fromLabel", m.fromLabel); val fk = ident("fromKeyName", m.fromKeyName)
    val tl = ident("toLabel", m.toLabel);     val tk = ident("toKeyName", m.toKeyName)
    val tid = tenantOf(m.props)
    // BUG-020（2026-09-06 批1-A）：原实现租户限定子句写 n.tenantId——端点别名是 a/b，
    // 带租户戳的 MergeEdge 一执行即 Neo4j「Variable n not defined」；该路径此前零执行
    // （存量边调用全不带 props），纯函数 Spec 同款断言掩盖。别名分置修复。
    val fromTenantClause = tid.fold("")( _ => s" AND a.$TenantKey = $$tid")
    val toTenantClause = tid.fold("")( _ => s" AND b.$TenantKey = $$tid")
    val cypher =
      s"""OPTIONAL MATCH (a:`$fl` {$fk: $$fromKeyValue}) WHERE 1=1$fromTenantClause
         |OPTIONAL MATCH (b:`$tl` {$tk: $$toKeyValue}) WHERE 1=1$toTenantClause
         |RETURN count(a) + count(b) AS endpoints, a.$TenantKey AS atenant, b.$TenantKey AS btenant""".stripMargin
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("fromKeyValue", m.fromKeyValue)
    params.put("toKeyValue", m.toKeyValue)
    if (tid.isDefined) params.put("tid", tid.get)
    (cypher, params)
  }

  /** MergeEdge 实写（预检通过后调用）；形态2：端点匹配同加租户限定 */
  def mergeEdgeCypher(m: MergeEdge): (String, java.util.Map[String, AnyRef]) = {
    val rel = ident("relType", m.relType)
    val fl = ident("fromLabel", m.fromLabel); val fk = ident("fromKeyName", m.fromKeyName)
    val tl = ident("toLabel", m.toLabel);     val tk = ident("toKeyName", m.toKeyName)
    val tid = tenantOf(m.props)
    // BUG-020：同上——实写侧别名分置
    val fromTenantClause = tid.fold("")( _ => s" AND a.$TenantKey = $$tid")
    val toTenantClause = tid.fold("")( _ => s" AND b.$TenantKey = $$tid")
    val cypher =
      s"""MATCH (a:`$fl` {$fk: $$fromKeyValue}) WHERE 1=1$fromTenantClause
         |MATCH (b:`$tl` {$tk: $$toKeyValue}) WHERE 1=1$toTenantClause
         |MERGE (a)-[r:`$rel`]->(b)
         |  SET r += $$props""".stripMargin
    val params = new java.util.HashMap[String, AnyRef]()
    params.put("fromKeyValue", m.fromKeyValue)
    params.put("toKeyValue", m.toKeyValue)
    if (tid.isDefined) params.put("tid", tid.get)
    params.put("props", strParams(m.props))
    (cypher, params)
  }

  // ── 执行层 ─────────────────────────────────────────────────────────────────

  /** 懒加载：纯 Cypher/验戳单测不触发网络栈初始化。
   * 2026-09-03 S2-33：连接参数收敛到 GraphConnectionConfig（口令退役出代码）。 */
  private lazy val driver = GraphConnectionConfig.newDriver()

  /**
   * 复合节点键约束幂等建立（EP 风险0·形态2）：(业务键, tenantId) IS NODE KEY——
   * 同 uuid 跨租户成双节点，租户隔离进存储层。uid 降级为普通属性（不再唯一约束）。
   * ⚠️ 前置：存量节点需已完成租户戳补齐（B1/B2+B2a 继承），NODE KEY 要求全分量非空；
   * 存量缺戳节点存在时本调用会失败——strict 排练批次先跑 rebuild 再建约束。
   */
  def ensureConstraints(): Seq[String] = {
    val keyFields = Map(
      "Concept" -> "conceptId", "ConceptEdge" -> "edgeId", "Context" -> "contextId",
      "Subdomain" -> "subdomainId", "ContextRelationNode" -> "relationId",
      "Note" -> "noteId", "Question" -> "questionId")
    val drops = keyFields.keys.map { l =>
      // 形态2：uid 降级普通属性，废弃单键 UNIQUE（否则跨租户同 uuid 双节点被旧约束拦截）
      s"DROP CONSTRAINT ${l}_uid IF EXISTS"
    }
    val creates = keyFields.map { case (l, kf) =>
      // 复合 UNIQUENESS（非 NODE KEY）：NODE KEY 为 Enterprise 专属，Community 实例无法创建；
      // UNIQUE 允许 null 分量（存量缺戳过渡友好），跨租户同 uuid 隔离由复合唯一性保证，
      // 缺戳强制由应用层验戳（strict 模式 MissingTenantStampException）承担
      s"CREATE CONSTRAINT ${l}_tenant_key IF NOT EXISTS FOR (n:`$l`) REQUIRE (n.`$kf`, n.$TenantKey) IS UNIQUE"
    }
    (drops ++ creates).map { cypher =>
      val s = session()
      try s.writeTransaction(tx => tx.run(cypher).consume())
      finally s.close()
      cypher
    }.toSeq
  }

  private def session(): Session = driver.session()

  /** 工具/冒烟入口用的只读会话（包内可见；业务代码请走 apply） */
  private[graph] def sessionForStats(): Session = driver.session()

  private def write(cypher: String, params: java.util.Map[String, AnyRef]): Unit = {
    val s = session()
    try s.writeTransaction(tx => tx.run(cypher, params).consume())
    finally s.close()
  }

  private def writeCount(cypher: String, params: java.util.Map[String, AnyRef], field: String): Int = {
    val s = session()
    try s.writeTransaction(tx => tx.run(cypher, params).single().get(field).asInt())
    finally s.close()
  }

  private def readCount(cypher: String, params: java.util.Map[String, AnyRef], field: String): Int = {
    val s = session()
    try s.readTransaction(tx => tx.run(cypher, params).single().get(field).asInt())
    finally s.close()
  }

  private def endpointMissing(label: String, keyName: String, keyValue: String): Boolean = {
    val l = ident("label", label); val k = ident("keyName", keyName)
    readCount(s"MATCH (n:`$l` {$k: $$v}) RETURN count(n) AS c", map1("v", keyValue), "c") == 0
  }

  /** 边预检结果：端点计数 + 两端 tenantId（OPTIONAL MATCH 恒单行，缺失为 NULL） */
  private def readEdgeCheck(cy: String, pa: java.util.Map[String, AnyRef]): (Int, Option[String], Option[String]) = {
    def optStr(v: org.neo4j.driver.Value): Option[String] =
      if (v == null || v.isNull) None else Option(v.asString()).filter(_.trim.nonEmpty)
    val s = session()
    try {
      val rec = s.readTransaction(tx => tx.run(cy, pa).single())
      (rec.get("endpoints").asInt(), optStr(rec.get("atenant")), optStr(rec.get("btenant")))
    } finally s.close()
  }

  /**
   * 应用单个变更，返回产生的冲突清单（CollectConflicts 模式下可能非空；Strict 恒空——
   * 悬空直接抛 [[DanglingEndpointException]]，由调用方进停车场有界重试）。
   * B1：写前强制租户验戳（模式可配，默认 warn）；边预检含跨租户连线判定（结构性污染恒拒绝）；
   * UpdateNodeProps 带 expectedBaseVersion 时 touched=0 ⇒ 版本冲突（EP_KnowledgePatch_Tenant_Isolation）。
   */
  def apply(m: GraphMutation, mode: ExecutionMode): Seq[GraphConflict] = m match {
    case up: UpsertNode =>
      val base = sanitizeTenantStamp(up.props)
      val stamped = withInheritedStamp(up.label, up.keyName, up.keyValue, base)
      enforceStamp(up.label, stamped)
      val (cy, pa) = upsertNodeCypher(if (stamped eq base) up else up.copy(props = stamped))
      write(cy, pa); Nil

    case upd: UpdateNodeProps =>
      val base = sanitizeTenantStamp(upd.props)
      val stamped = withInheritedStamp(upd.label, upd.keyName, upd.keyValue, base)
      enforceStamp(upd.label, stamped)
      val (cy, pa) = updateNodePropsCypher(
        if (stamped eq base) upd else upd.copy(props = stamped))
      if (writeCount(cy, pa, "touched") == 0) upd.expectedBaseVersion match {
        case Some(baseV) => mode match {
          case ExecutionMode.Strict =>
            throw VersionConflictException(
              s"CAS failed (${upd.label}{${upd.keyName}=${upd.keyValue}}) expected _v=$baseV")
          case ExecutionMode.CollectConflicts =>
            Seq(GraphConflict("VERSION_CONFLICT", "BLOCKER",
              s"乐观锁冲突 (${upd.label}{${upd.keyName}=${upd.keyValue}}) 期望 _v=$baseV",
              Map("label" -> upd.label, "keyName" -> upd.keyName,
                  "keyValue" -> upd.keyValue, "expectedVersion" -> baseV.toString)))
        }
        case None =>
          if (mode == ExecutionMode.CollectConflicts)
            Seq(GraphConflict("UPDATE_TARGET_MISSING", "BLOCKER",
              s"属性更新目标不存在 (${upd.label}{${upd.keyName}=${upd.keyValue}})",
              Map("label" -> upd.label, "keyName" -> upd.keyName, "keyValue" -> upd.keyValue)))
          else Nil
      } else Nil

    case del: DeleteNode =>
      val (cy, pa) = deleteNodeCypher(del); write(cy, pa); Nil

    case me @ MergeEdge(_, fl, fk, fv, tl, tk, tv, _, _) =>
      val (checkCy, checkPa) = mergeEdgeCheckCypher(me)
      val (endpoints, at, bt) = readEdgeCheck(checkCy, checkPa)
      // 跨租户连线判定：边声明租户而端点归属相悖 ⇒ 结构性污染，两种模式均拒绝
      crossTenantViolation(tenantOf(me.props), at, bt).foreach { why =>
        throw CrossTenantEdgeException(
          s"MergeEdge ($fl{$fk=$fv})→($tl{$tk=$tv}): $why")
      }
      if (endpoints >= 2) {
        val (cy, pa) = mergeEdgeCypher(me); write(cy, pa); Nil
      } else mode match {
        case ExecutionMode.Strict =>
          throw DanglingEndpointException(s"MergeEdge endpoints missing: ($fl{$fk=$fv})→($tl{$tk=$tv})")
        case ExecutionMode.CollectConflicts =>
          Seq((fl, fk, fv), (tl, tk, tv))
            .filterNot { case (l, k, v) => !endpointMissing(l, k, v) }
            .map { case (l, k, v) =>
              GraphConflict("REVERT_INTERSECTION", "BLOCKER",
                s"边端点缺失 ($l{$k=$v})",
                Map("label" -> l, "keyName" -> k, "keyValue" -> v))
            }
      }
  }
}
