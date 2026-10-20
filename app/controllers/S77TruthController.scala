// MANUAL (S2-81 批5 · 2026-09-10)：s77 真源数据面服务——设计知识库设计文档 s281_s77_9000_dataplane.md
// 与 gen_api_layer 生成的 CRUD 面不同域：本控制器只做「持有真源文件并按 sha256 指纹服务」，
// 不做派生计算（派生层在 9077 evidence_model/viewmodel 纯函数）。勿被生成器收编时抹掉指纹语义。
// B4-A（S2-82 · 2026-09-11 · s282 §9）：manifest 协议 v2——每源披露 origin（出身）＋contentSource（字节来源）。
// B4b（S2-82 · 2026-09-11 · s282 §11）：contentSource 逐键翻 projection——配置列出的键（缺省空集=全 file）
//   字节改由投影重建（TruthProjectionRebuilder·canonical JSON），manifest/:source 指纹随重建字节实算；
//   重建失败 → manifest 条目 sha256="PROJECTION_UNAVAILABLE" / :source 503 TRUTH_PROJECTION_UNAVAILABLE
//   ——不静默回退文件（审计语义先例：缓存缺位 throw 不本地）。文件仍是权威直至 B5 写环闭环。
package controllers

import net.imadz.common.auth.AuthAction
import net.imadz.metarch.truth.TruthCanonicalJson
import net.imadz.metarch.truth.TruthProjectionSource

import javax.inject.{Inject, Singleton}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import play.api.libs.json.{JsArray, JsNull, JsNumber, JsObject, JsString, JsValue, Json}
import play.api.mvc._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** 真源键 provenance 分类（B4-A · s282 §9.1 W2）——纯函数供测试门直测。
  *
  * origin＝文件出身（谁产出这个文件）：
  *   - "file"    真源本体（手写/land_apply 落盘/静态输入）：metarch/cem/role-command-map/fields-template/roadshow-script
  *   - "derived" node 派生管线产物（§1.2 #5 口径）：fields-full/generate_lineage、generated-rules/generate_rules、
  *               dor-report/run_loop、readmodels/generate_readmodels（SM10 反向推导——派生后 canonical 化入库，与
  *               B4b 投影读切源不冲突：origin 说出身，contentSource 说服务字节从哪来）
  *   - "run"     服务运行回执（客户端观察·by design 不可服务重建）：roadshow-events/roadshow-summary
  *
  * contentSource＝服务字节来源：缺省全 "file"；B4b 对账门（tools/truth_recon.mjs drift=0）逐键翻 "projection"
  *   ——翻键集合由 conf s77.truth.projection-keys / env S77_TRUTH_PROJECTION_KEYS 声明（缺省空集）。
  */
object TruthSourceOrigin {
  val Derived: Set[String] = Set("fields-full", "generated-rules", "dor-report", "readmodels")
  val Run: Set[String] = Set("roadshow-events", "roadshow-summary")
  def of(key: String): String =
    if (Derived.contains(key)) "derived" else if (Run.contains(key)) "run" else "file"
}

/** S77TruthController（S2-81 批5）：12 真源输入的指纹服务。
  *
  * GET /api/truth/manifest          指纹表（sha256+bytes+rel；roadshow-events/summary 指向当前最新 run 束）
  * GET /api/truth/:source           原始字节（ETag=sha256 指纹；If-None-Match → 304）
  * GET /api/truth/status            健康披露（不走 authAction——只报源在不在，不透内容）
  *
  * 鉴权：manifest/:source 走 authAction（同 /api/meta 一族先例；真源含工作坊内部数据）；
  *       status 与 /healthz 同待遇（纯探针）。
  * 语义基座（协议 §2）：指纹优先——客户端先 manifest 比指纹，未变用本地缓存，变了再拉。
  * 缺失披露优于崩溃：任何源文件缺失 → manifest 该条 sha256="MISSING"，不 500。
  */
@Singleton
class S77TruthController @Inject() (
                                     authAction: AuthAction,
                                     conf: play.api.Configuration,
                                     truthProjectionSource: TruthProjectionSource,
                                     val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {

  override def controllerComponents: ControllerComponents = cc

  // 真源根：conf s77.truth.root > 环境变量 S77_TRUTH_ROOT > 缺省 ../backend-codegen（相对 ingested-app 工作目录）
  private val truthRoot: Path = {
    val confVal = conf.getOptional[String]("s77.truth.root")
    val root = confVal.orElse(sys.env.get("S77_TRUTH_ROOT")).getOrElse("../backend-codegen")
    Paths.get(root).toAbsolutePath.normalize()
  }

  // B4b/B4c：投影读键集合（conf > env > 缺省空集=关——回滚＝清配置重启）。
  // 合法域＝已翻键（fields-template/readmodels/cem/role-command-map〔B4b〕＋metarch〔B4c〕
  // ＋fields-full/generated-rules/dor-report 预留）；roadshow run 出身永不可翻。
  // 非法键启动即失败（防拼写静默——配置写了但不生效比报错更危险）。
  private val projectionKeys: Set[String] = {
    val confKeys: Seq[String] = conf.getOptional[Seq[String]]("s77.truth.projection-keys")
      .orElse(sys.env.get("S77_TRUTH_PROJECTION_KEYS").filter(_.nonEmpty).map(_.split(',').toSeq))
      .getOrElse(Seq.empty)
    val allowed = Set("fields-template", "readmodels", "cem", "role-command-map", "metarch", "fields-full", "generated-rules", "dor-report")
    val bad = confKeys.filterNot(allowed.contains)
    if (bad.nonEmpty)
      throw new IllegalArgumentException(s"s77.truth.projection-keys 含不合法键：${bad.mkString(",")}（合法域：B4b 四键＋B4c metarch＋derived 三键预留；roadshow run 出身永不可翻）")
    confKeys.toSet
  }

  /** 9 个硬源（key → 仓库相对路径）。roadshow 三键运行时解析（束随最新 run 变动）。 */
  private val HARD_SOURCES: Seq[(String, String)] = Seq(
    "metarch" -> ".ingest/metarch.json",
    "cem" -> ".ingest/command-event-map.json",
    "role-command-map" -> "knowledge_base/design_docs/field_template/story/role_command_map.json",
    "fields-template" -> "knowledge_base/design_docs/field_template/metarch.fields.template.json",
    "fields-full" -> "knowledge_base/design_docs/field_template/fields-full.json",
    "readmodels" -> "knowledge_base/design_docs/field_template/story/readmodels.json",
    "generated-rules" -> "knowledge_base/design_docs/field_template/generated_rules.json",
    "dor-report" -> "knowledge_base/design_docs/usecase_ledger/stage-dor-report.md",
    "roadshow-script" -> "visualization/postman/roadshow/script.v4-real.json",
  )

  private val ROADSHOW_DIR = "visualization/postman/roadshow/auto"

  /** 当前最新 run 束（run-s275r\d+ 含 events.jsonl 者取最新——与 9077 evidence_model 同口径）。 */
  private def latestRun: Option[String] = {
    val dir = truthRoot.resolve(ROADSHOW_DIR)
    if (!Files.isDirectory(dir)) None
    else
      Files.list(dir).iterator().asScala.toSeq
        .filter(p => Files.isDirectory(p) && p.getFileName.toString.matches("run-s275r\\d+")
          && Files.exists(p.resolve("events.jsonl")))
        .map(_.getFileName.toString)
        .sorted
        .lastOption
  }

  private def roadshowRels: Seq[(String, String)] = latestRun match {
    case Some(run) => Seq(
      "roadshow-events" -> s"$ROADSHOW_DIR/$run/events.jsonl",
      "roadshow-summary" -> s"$ROADSHOW_DIR/$run/summary.json",
    )
    case None => Seq(
      // 束缺位也登记键（MISSING 披露），客户端指纹恒变驱动重试可见
      "roadshow-events" -> s"$ROADSHOW_DIR/-/events.jsonl",
      "roadshow-summary" -> s"$ROADSHOW_DIR/-/summary.json",
    )
  }

  private def sourceRel(key: String): Option[String] =
    (HARD_SOURCES ++ roadshowRels).toMap.get(key)

  private def resolve(rel: String): Option[Path] = {
    val p = truthRoot.resolve(rel).normalize()
    // 越界防护：解析后必须仍在真源根下（Windows 反斜杠统一）
    if (p.startsWith(truthRoot) && Files.isRegularFile(p)) Some(p) else None
  }

  private def sha256Hex(p: Path): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val is = Files.newInputStream(p)
    try {
      val buf = new Array[Byte](8192)
      var n = is.read(buf)
      while (n >= 0) { if (n > 0) md.update(buf, 0, n); n = is.read(buf) }
      md.digest.map(b => String.format("%02x", java.lang.Byte.valueOf(b))).mkString
    } finally is.close()
  }

  private def contentTypeFor(key: String): String = key match {
    case "dor-report" => "text/markdown; charset=utf-8"
    case "roadshow-events" => "application/x-ndjson"
    case _ => "application/json"
  }

  /** manifest：全部源 sha256+bytes+rel＋origin/contentSource（协议 v2·B4-A）＋投影读键（B4b）；
    * MISSING/PROJECTION_UNAVAILABLE 披露不 500（协议 §2.1）。 */
  def manifest(): Action[AnyContent] = authAction.async { _ =>
    val entries: Future[Seq[(String, JsObject)]] = Future.sequence {
      (HARD_SOURCES ++ roadshowRels).map { case (key, rel) =>
        if (projectionKeys.contains(key))
          truthProjectionSource.rebuild(key).map { rebuilt =>
            val bytes = TruthCanonicalJson.canonicalBytes(rebuilt)
            key -> Json.obj(
              "rel" -> rel,
              "sha256" -> TruthCanonicalJson.sha256Hex(bytes),
              "bytes" -> bytes.length,
              // v2 披露：origin＝文件出身（不变）；contentSource＝服务字节来源（B4b 翻 projection）
              "origin" -> TruthSourceOrigin.of(key),
              "contentSource" -> "projection",
            )
          }.recover { case _ =>
            // 重建失败：缺失披露优于崩溃（不 500 整表）——客户端指纹恒不匹配 → 重拉 :source → 503 → 按消费侧既有降级路径
            key -> Json.obj(
              "rel" -> rel,
              "sha256" -> "PROJECTION_UNAVAILABLE",
              "bytes" -> 0,
              "origin" -> TruthSourceOrigin.of(key),
              "contentSource" -> "projection",
            )
          }
        else
          Future.successful {
            val entry = resolve(rel) match {
              case Some(p) => Json.obj("rel" -> rel, "sha256" -> sha256Hex(p), "bytes" -> Files.size(p))
              case None => Json.obj("rel" -> rel, "sha256" -> "MISSING", "bytes" -> 0)
            }
            key -> (entry ++ Json.obj("origin" -> TruthSourceOrigin.of(key), "contentSource" -> "file"))
          }
      }
    }
    entries.map { es =>
      Ok(Json.obj(
        "schemaVersion" -> 2,
        "generatedAt" -> java.time.Instant.now().toString,
        "root" -> String.valueOf(truthRoot),
        "sources" -> JsObject(es),
        "latestRun" -> latestRun.fold[JsValue](JsNull)(n => JsString(n)),
      ))
    }
  }

  /** :source 原始字节（翻键＝投影重建 canonical 字节）。ETag=sha256（指纹语义——客户端 If-None-Match 命中即 304）。 */
  def source(source: String): Action[AnyContent] = authAction.async { req =>
    if (projectionKeys.contains(source)) {
      // 投影读（B4b）：字节来源＝投影重建，无文件回退（contentSource 声明 projection 就必须真是 projection）
      truthProjectionSource.rebuild(source).map { rebuilt =>
        val bytes = TruthCanonicalJson.canonicalBytes(rebuilt)
        val tag = "\"" + TruthCanonicalJson.sha256Hex(bytes) + "\""
        if (req.headers.get("If-None-Match").contains(tag)) {
          Status(304).withHeaders("ETag" -> tag)
        } else {
          Ok(bytes).as("application/json")
            .withHeaders(
              "ETag" -> tag,
              "X-Truth-Rel" -> sourceRel(source).getOrElse(""),
              "X-Truth-Run" -> latestRun.getOrElse(""),
            )
        }
      }.recover { case _ =>
        ServiceUnavailable(Json.obj(
          "errorCode" -> "TRUTH_PROJECTION_UNAVAILABLE",
          "message" -> s"truth source '$source' projection rebuild failed（DB 缺位或不一致——重灌 full-load）",
        ))
      }
    } else {
      Future.successful {
        sourceRel(source) match {
          case None =>
            BadRequest(Json.obj("errorCode" -> "TRUTH_SOURCE_INVALID", "message" -> s"unknown truth source '$source'"))
          case Some(rel) =>
            resolve(rel) match {
              case None =>
                NotFound(Json.obj("errorCode" -> "TRUTH_SOURCE_NOT_FOUND", "message" -> s"truth source '$source' file missing at $rel"))
              case Some(p) =>
                val tag = "\"" + sha256Hex(p) + "\""
                if (req.headers.get("If-None-Match").contains(tag)) {
                  Status(304).withHeaders("ETag" -> tag)
                } else {
                  val bytes = Files.readAllBytes(p)
                  Ok(bytes).as(contentTypeFor(source))
                    .withHeaders(
                      "ETag" -> tag,
                      "X-Truth-Rel" -> rel,
                      // roadshow 源带当次 run 目录名（束切换可观测）
                      "X-Truth-Run" -> latestRun.getOrElse(""),
                    )
                }
            }
        }
      }
    }
  }

  /** 健康披露（无鉴权探针——与 /healthz 同待遇）：只报源在不在，不透内容。 */
  def status(): Action[AnyContent] = Action { _ =>
    val all = HARD_SOURCES ++ roadshowRels
    val missing = all.collect { case (k, rel) if resolve(rel).isEmpty => k }
    Ok(Json.obj(
      "status" -> (if (missing.isEmpty) "ok" else "degraded"),
      "root" -> String.valueOf(truthRoot),
      "sourcesOk" -> (all.size - missing.size),
      "sourcesTotal" -> all.size,
      "sourcesMissing" -> JsArray(missing.map(JsString(_))),
    ))
  }
}
