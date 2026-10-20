package net.imadz.metarch.application.kb

import java.io.File
import javax.inject.{Inject, Singleton}
import org.neo4j.driver.Value
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * M2 推理面存储＋模板注册表（批7·kn_kb_storage_plan §6.2/§6.6）——/api/kb/reason 唯一执行层。
 *
 * 手写领地声明（挂 EP_S2_48 批1·writeTerritorial）：本文件族
 * （KnReasonStore/controllers/kb/KnReasonController/conf/kb/reasoning-templates/
 * conf/routes 尾巴块 kb reason 段）为行为侧手写代码，不在 CodegenMain 生成产物清单内
 * （新文件名无碰撞面），禁止未来把 KnReason* 加入生成器输出集。
 *
 * 白名单纪律（§6.2「模板外 cypher 一律拒绝」）：执行输入＝注册表内模板名＋参数 map——
 * 本类**没有任何接受调用方 cypher 的入口**（设计即拒绝，非运行时校验）；注册表从
 * conf/kb/reasoning-templates/ 装载（template.json＋query.cypher 同构同步自
 * backend-codegen tools/kn_reasoning_templates/——单一真源在彼侧，模板变更须双向同步）。
 *
 * 证据契约：返回行集（每行含路径列），confidence=mechanical；空集＝「图谱中不存在该
 * 关系路径」的合法答案（负断言纪律——controller 原样透传，不填充虚构行）。
 *
 * 降级纪律（§5.6 分层隔离）：Neo4j 不可用 → 查询 Future 失败上抛，Service 翻译 degraded
 * （不 5xx）；模板目录缺失/损坏 → 启动期 fail-fast（注册表空＝服务形态错，静默空注册
 * 会让所有查询变「未知模板」的误导性 400）。
 */
@Singleton
class KnReasonStore @Inject() (conf: play.api.Configuration)(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val templatesDir: File =
    new File(conf.getOptional[String]("kb.reason.templatesDir").getOrElse("conf/kb/reasoning-templates"))

  // ---------- 模板注册表（启动期装载·白名单） ----------
  private val registry: Map[String, KnReasonTemplate] = loadRegistry()

  private def loadRegistry(): Map[String, KnReasonTemplate] = {
    if (!templatesDir.isDirectory)
      throw new IllegalStateException(s"[KnReasonStore] 模板目录不存在: ${templatesDir.getAbsolutePath}（kb.reason.templatesDir）")
    val loaded = Option(templatesDir.listFiles()).getOrElse(Array.empty).filter(_.isDirectory).flatMap { d =>
      val tplFile = new File(d, "template.json")
      if (!tplFile.isFile) {
        log.warn(s"kn_reason 模板目录缺 template.json（跳过）: ${d.getName}")
        None
      } else {
        Try {
          val tpl = Json.parse(scala.io.Source.fromFile(tplFile, "UTF-8").mkString)
          val name = (tpl \ "name").as[String]
          val description = (tpl \ "description").asOpt[String].getOrElse("")
          val params = (tpl \ "params").as[Map[String, KnReasonParam]]
          val cypherFile = new File(d, (tpl \ "cypher").as[String])
          val cypher = scala.io.Source.fromFile(cypherFile, "UTF-8").mkString
          val tenantBinding = (tpl \ "tenantBinding").asOpt[String].getOrElse("body")
          KnReasonTemplate(name, description, params, cypher, tenantBinding)
        } match {
          case scala.util.Success(t) => Some(t)
          case scala.util.Failure(e) =>
            log.warn(s"kn_reason 模板装载失败（跳过）: ${d.getName} — ${e.getMessage}"); None
        }
      }
    }
    if (loaded.isEmpty)
      throw new IllegalStateException(s"[KnReasonStore] 模板注册表为空: ${templatesDir.getAbsolutePath}——推理服务形态错（fail-fast）")
    loaded.map(t => t.name -> t).toMap
  }

  /** 在册模板清单（发现面：GET /api/kb/reason） */
  def templates: Seq[KnReasonTemplate] = registry.values.toSeq.sortBy(_.name)

  /** 模板租户绑定口径（P0 tid 硬化·2026-10-08）：body=沿用调用方参数（存量 7 模板缺省，
    * root 图文件流水线通道不变）；token=服务端强制 params.tenant:=会话 tid（租户图通道，
    * body 自报≠tid 拒绝）。binding 判定在 service/controller 层执法，本方法只暴露声明。 */
  def tenantBindingOf(template: String): Option[String] = registry.get(template).map(_.tenantBinding)

  /** 白名单查询：模板名必须注册在案；必填参数缺即 Left（400 语义） */
  def reason(template: String, params: Map[String, String]): Either[String, Future[Seq[Map[String, JsValue]]]] =
    registry.get(template) match {
      case None => Left(s"未知模板: $template（在册: ${registry.keys.toSeq.sorted.mkString(", ")}）")
      case Some(tpl) =>
        // 缺省参数补 template.json default（与文件侧 CLI kn_graph_query.mjs 同口径——防两通道分叉判例）
        val effective = params ++ tpl.params.collect { case (k, p) if !params.contains(k) && p.default.nonEmpty => k -> p.default.get }
        val missing = tpl.params.collect { case (k, p) if p.required && !effective.contains(k) => k }
        if (missing.nonEmpty) Left(s"缺必填参数: ${missing.toSeq.sorted.mkString(", ")}")
        else Right(runCypher(tpl.cypher, effective))
    }

  /** 只读执行（readTransaction·参数化注入——注册表 cypher 原样执行，值经 driver 参数通道无拼接） */
  private[kb] def runCypher(cypher: String, params: Map[String, String]): Future[Seq[Map[String, JsValue]]] = Future {
    val session = KnReasonStore.driver.session()
    try {
      session.readTransaction { tx =>
        val rs = tx.run(cypher, params.asInstanceOf[Map[String, AnyRef]].asJava)
        rs.list().asScala.toSeq.map { rec =>
          rec.keys().asScala.map(k => k -> KnReasonStore.toJs(rec.get(k))).toMap
        }
      }
    } finally session.close()
  }

  /** spec 探针：连通性验证（live round-trip 的 probe-or-cancel 用） */
  private[kb] def probe(): Future[Seq[Map[String, JsValue]]] = runCypher("RETURN 1 AS ok", Map.empty)
}

object KnReasonStore {
  import net.imadz.metarch.infrastructure.graph.GraphConnectionConfig

  // 与 GraphMutationExecutor/GraphProjectionRepository 同款：lazy 单例 driver（口令经
  // GraphConnectionConfig 单点取值——conf > env > fail-fast）
  private lazy val driver = GraphConnectionConfig.newDriver()

  /** Neo4j Value → JsValue（模板列形态：字符串/整数/布尔/列表/路径序列化） */
  private def toJs(v: Value): JsValue = fromObject(v.asObject())

  private def fromObject(o: AnyRef): JsValue = o match {
    case null => JsNull
    case s: String => JsString(s)
    case l: java.lang.Long => JsNumber(BigDecimal(l))
    case i: java.lang.Integer => JsNumber(BigDecimal(i))
    case d: java.lang.Double => JsNumber(BigDecimal(d))
    case b: java.lang.Boolean => JsBoolean(b)
    case list: java.util.List[_] => JsArray(list.asScala.map(x => fromObject(x.asInstanceOf[AnyRef])) .toSeq)
    case other => JsString(String.valueOf(other))
  }
}

/** 白名单模板（conf/kb/reasoning-templates/<name>/template.json 的强类型载体）
  * tenantBinding：body（缺省·调用方参数直传——root 图文件流水线通道）｜
  * token（租户图通道·服务端强制 params.tenant:=会话 tid，调用方自报 tenant 须与 tid 相等） */
final case class KnReasonTemplate(
    name: String,
    description: String,
    params: Map[String, KnReasonParam],
    cypher: String,
    tenantBinding: String = "body")

final case class KnReasonParam(`type`: String, required: Boolean, desc: Option[String], default: Option[String] = None)

object KnReasonParam {
  implicit val reads: Reads[KnReasonParam] = Json.reads[KnReasonParam]
}
