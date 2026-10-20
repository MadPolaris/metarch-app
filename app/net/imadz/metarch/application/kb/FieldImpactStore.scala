package net.imadz.metarch.application.kb

import javax.inject.{Inject, Singleton}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/**
 * 字段爆破面存储（EP_Field_Impact_Server_Side P1·2026-10-08）——/api/kb/field-impact 唯一执行层。
 *
 * 手写领地声明（KnReasonStore 同款）：本文件族（FieldImpactStore/controllers/kb/FieldImpactController/
 * conf/routes 尾巴块 field-impact 段）为行为侧手写代码，不在 CodegenMain 生成产物清单内，
 * 禁止未来把 FieldImpact* 加入生成器输出集。
 *
 * 数据面铁律（用户令）：知识面 **零 SQL**——MySQL 知识表（concept 内容 text 直写）不参与任何
 * 查询路径；本类只读 Neo4j 租户图（Field/Concept/HAS_FIELD/WRITES/EMITS/EMITS_PAYLOAD），
 * 谓词恒带 tenantId=$tid 构造性隔离（两图号空间混查雷的防线），全程 readTransaction 零写入口。
 *
 * 契约镜像（与 backend-codegen tools/field_impact.mjs 的 field-impact/v1 逐键同名）：
 * resolution/nodes/edges/merged{common,onlyBy}/coverage/summary——MCP 转写层零改动。
 *
 * 三形态锚解析（模糊名不短路纪律）：唯一命中→resolved；多候选→ambiguous（并集+逐面差分，
 * split=true 改逐候选全闭包）；零命中→missed（负断言，非错误）。
 *
 * 降级纪律：Neo4j 不可用 → degraded=true（不 5xx，KnReasonService 同款）。
 */
@Singleton
class FieldImpactStore @Inject() (implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  /** Field.role → face 六面映射（租户图 role 词汇 2026-10-08 实测七种恰好覆盖六面） */
  private val RoleToFace =
    "MATCH (f:Field {tenantId:$tid}) WHERE f.name = $anchor " +
      "RETURN f"

  // ---------- 锚解析（三形态） ----------

  /** 模糊名/全限定名共用：按 name 精确匹配（不短路——返回全部同名字段行）
    * 全限定名 Container.field 拆两段后加宿主过滤 */
  def resolveByName(tid: String, container: Option[String], name: String): Future[Seq[Map[String, JsValue]]] = {
    val cypher = container match {
      case Some(c) =>
        """MATCH (host:Concept {tenantId:$tid, name:$container})-[:HAS_FIELD]->(f:Field)
          |WHERE f.name = $name
          |RETURN f.conceptId AS conceptId, f.name AS name, f.role AS role,
          |       f.noteId AS noteId, host.name AS container, host.kind AS containerKind
          |ORDER BY role, conceptId""".stripMargin
      case None =>
        """MATCH (host:Concept {tenantId:$tid})-[:HAS_FIELD]->(f:Field)
          |WHERE f.name = $name
          |RETURN f.conceptId AS conceptId, f.name AS name, f.role AS role,
          |       f.noteId AS noteId, host.name AS container, host.kind AS containerKind
          |ORDER BY role, conceptId""".stripMargin
    }
    val params: Map[String, String] = Map("tid" -> tid, "name" -> name) ++
      container.map(c => Map("container" -> c)).getOrElse(Map.empty)
    run(cypher, params)
  }

  /** UUID 形态：36 位 noteId 直取（noteId 即服务端号=回锚键） */
  def resolveByUuid(tid: String, uuid: String): Future[Seq[Map[String, JsValue]]] =
    run(
      """MATCH (host:Concept {tenantId:$tid})-[:HAS_FIELD]->(f:Field {noteId:$uuid})
        |RETURN f.conceptId AS conceptId, f.name AS name, f.role AS role,
        |       f.noteId AS noteId, host.name AS container, host.kind AS containerKind
        |ORDER BY role, conceptId""".stripMargin,
      Map("tid" -> tid, "uuid" -> uuid))

  // ---------- 单字段影响闭包（六面行+owner 归约+双向血缘） ----------

  /**
   * 单字段节点的影响闭包行（field-impact/v1 的 nodes 元素同形）：
   * face/fieldId/container/owners/derivesFrom/derivedBy/writtenBy/slot+expr。
   *
   * owner 归约（实测图形状·2026-10-08 二次修正）：
   *  - 宿主=AGGREGATE 概念（id/st 面·**pm 面实测也挂聚合**·pl 面部分 TruthSourceProvenance
   *    型聚合载体）：HAS_FIELD 直接归属；
   *  - 宿主=EVENT 概念（pl 面）：经 事件-WRITES→状态字段←HAS_FIELD←聚合 归约；
   *  - 宿主=COMMAND 概念（兜底·万一有命令概念直挂参数）：经 命令-EMITS→事件-WRITES→
   *    状态字段←HAS_FIELD←聚合；推导不出（纯参数无落状态）→ owners 空=诚实空；
   *  - vo 面：宿主=ValueObject 概念，owner 空（VO 无聚合承载）；
   *  - rm 面：宿主=READ_MODEL 概念，owner 空（读模型跨族承载·多源视图多 owner 诚实空）。
   *
   * 血缘推导（writtenBy 字符串解析·最高危判据）：状态字段 S 的 derivesFrom=以 S 为
   * WRITES 目标的事件经 EMITS_PAYLOAD 发出的载荷字段；载荷字段 P 的 derivedBy=WRITES
   * 到同名状态字段且 writtenBy 集合精确含 P 宿主事件名的下游字段。事件名匹配必须
   * split(';') 集合相等——禁 CONTAINS（前缀冲突误配判据）。
   */
  def impactClosure(tid: String, conceptId: String, role: String): Future[Seq[Map[String, JsValue]]] = {
    val cypher =
      """MATCH (f:Field {tenantId:$tid, conceptId:$cid})
        |MATCH (host:Concept {tenantId:$tid})-[:HAS_FIELD]->(f)
        |OPTIONAL MATCH (ag:Concept {tenantId:$tid, kind:'AGGREGATE'})-[:HAS_FIELD]->(f)
        |WITH f, host, collect(DISTINCT ag.name) AS aggs
        |WITH f, host,
        |  CASE
        |    WHEN host.kind = 'AGGREGATE' THEN aggs
        |    WHEN host.kind = 'EVENT' THEN [(host)-[:WRITES]->(wf:Field)<-[:HAS_FIELD]-(oag:Concept {kind:'AGGREGATE', tenantId:$tid}) | oag.name]
        |    WHEN host.kind = 'COMMAND' THEN [(host)-[:EMITS]->(ev:Concept {kind:'EVENT', tenantId:$tid})-[:WRITES]->(sf:Field)<-[:HAS_FIELD]-(cag:Concept {kind:'AGGREGATE', tenantId:$tid}) | cag.name]
        |    ELSE []
        |  END AS owners
        |OPTIONAL MATCH (evt:Concept {tenantId:$tid, kind:'EVENT'})-[w:WRITES]->(f)
        |OPTIONAL MATCH (evt)-[:EMITS_PAYLOAD]->(pay:Field {tenantId:$tid})
        |WITH f, host, owners, w,
        |     collect(DISTINCT evt.name) AS writtenByEvts,
        |     collect(DISTINCT pay.conceptId) AS deriveFromIds
        |RETURN f.conceptId AS fieldId, f.name AS name,
        |  CASE f.role
        |    WHEN 'Identifier' THEN 'id' WHEN 'State' THEN 'st'
        |    WHEN 'CommandParam' THEN 'pm' WHEN 'SystemParam' THEN 'pm'
        |    WHEN 'EventPayload' THEN 'pl' WHEN 'ValueObject.Member' THEN 'vo'
        |    WHEN 'ReadModelField' THEN 'rm' ELSE 'other' END AS face,
        |  host.name AS container, host.kind AS containerKind,
        |  reduce(s='', x IN owners | s + CASE WHEN s='' THEN '' ELSE ',' END + x) AS owners,
        |  reduce(s='', x IN deriveFromIds | s + CASE WHEN s='' THEN '' ELSE ',' END + x) AS derivesFrom,
        |  reduce(s='', x IN writtenByEvts | s + CASE WHEN s='' THEN '' ELSE ';' END + x) AS writtenBy,
        |  coalesce(w.slot, '') AS slot, coalesce(w.expr, '') AS expr
        |ORDER BY face, fieldId""".stripMargin
    run(cypher, Map("tid" -> tid, "cid" -> conceptId))
  }

  /** derivedBy 反向血缘（下游状态字段集）：writtenBy 集合精确含本字段宿主事件名 */
  def derivedBy(tid: String, name: String, hostName: String): Future[Seq[Map[String, JsValue]]] =
    run(
      """MATCH (down:Field {tenantId:$tid})
        |WHERE $evtName IN split(down.writtenBy, ';') AND down.name = $name AND down.role = 'State'
        |MATCH (dh:Concept {tenantId:$tid})-[:HAS_FIELD]->(down)
        |RETURN down.conceptId AS fieldId, dh.name AS container
        |ORDER BY fieldId""".stripMargin,
      Map("tid" -> tid, "evtName" -> hostName, "name" -> name))

  /** 前缀冲突哨兵对：golden 防回归用（split 集合相等 vs CONTAINS 的分辨用例） */
  def probe(): Future[Seq[Map[String, JsValue]]] = run("RETURN 1 AS ok", Map.empty)

  /** 只读执行（KnReasonStore.runCypher 同款：readTransaction·参数化注入零拼接） */
  private[kb] def run(cypher: String, params: Map[String, String]): Future[Seq[Map[String, JsValue]]] = Future {
    val session = FieldImpactStore.driver.session()
    try {
      session.readTransaction { tx =>
        val rs = tx.run(cypher, params.asInstanceOf[Map[String, AnyRef]].asJava)
        rs.list().asScala.toSeq.map { rec =>
          rec.keys().asScala.map(k => k -> FieldImpactStore.toJs(rec.get(k))).toMap
        }
      }
    } finally session.close()
  }
}

object FieldImpactStore {
  import net.imadz.metarch.infrastructure.graph.GraphConnectionConfig

  private lazy val driver = GraphConnectionConfig.newDriver()

  private def toJs(v: org.neo4j.driver.Value): JsValue = fromObject(v.asObject())

  private def fromObject(o: AnyRef): JsValue = o match {
    case null => JsNull
    case s: String => JsString(s)
    case l: java.lang.Long => JsNumber(BigDecimal(l))
    case i: java.lang.Integer => JsNumber(BigDecimal(i))
    case d: java.lang.Double => JsNumber(BigDecimal(d))
    case b: java.lang.Boolean => JsBoolean(b)
    case list: java.util.List[_] => JsArray(list.asScala.map(x => fromObject(x.asInstanceOf[AnyRef])).toSeq)
    case other => JsString(String.valueOf(other))
  }
}
