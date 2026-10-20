package controllers.filter

import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import akka.stream.Materializer
import net.imadz.common.auth.TokenService
import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment, Logging}
import play.api.mvc.{Filter, RequestHeader, Result}
import play.api.routing.Router
import scalikejdbc.DB
import scalikejdbc.interpolation.Implicits._

import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * R0.5 因果审计——命令侧落账（批3 · Option A 设计，2026-09-06）。
 *
 * 背景：事件侧 metadata 通道实证不可用——akka-persistence-mongo（3.0.8 与 3.1.2 同病）的
 * `Event.apply(repr)` 在写入与读取两侧都丢弃 `PersistentRepr.metadata`；typed
 * `Effect.persist` 也无挂点（thenRun 仅存在于 EffectBuilder）。因此因果边改为两层重建：
 *   ① 类型级：KN 图 EMITS（命令→事件）静态边已在图 v24；
 *   ② 实例级：本 filter 把每个变更请求（POST/PUT/PATCH/DELETE）落 MySQL `command_audit`
 *      ——(command, entity_id, occurred_at) 与事件日志按实体+序号/时间窗对齐，
 *      命令→事件边可重建（v4r10 世界事件无时间戳，以实体内序号邻接为主锚）。
 *
 * 设计约束：
 *   - 零生成面触碰（手写行为侧·不进 CodegenMain 领地）；
 *   - fire-and-forget——审计失败只记日志，绝不回压业务请求；
 *   - insert-only + request_id 唯一键（幂等，重放安全）；
 *   - 单写线程（顺序落账，daemon，随 JVM 退出）。
 */
final case class CommandAuditEntry(
    requestId: String,
    occurredAtMillis: Long,
    method: String,
    routePattern: Option[String],
    path: String,
    controller: Option[String],
    command: Option[String],
    entityId: Option[String],
    userId: Option[String],
    tenantId: Option[String],
    status: Int,
    durationMs: Long
)

/** 落账出口（spec 以内存实现替换；生产绑定 ScalikeJdbcCommandAuditStore）。 */
trait CommandAuditStore {
  /** fire-and-forget 契约：实现方自吞异常、异步执行，调用方零等待。 */
  def record(entry: CommandAuditEntry): Unit
}

class CommandAuditFilter @Inject() (
    tokenService: TokenService,
    store: CommandAuditStore
)(implicit val mat: Materializer, val ec: ExecutionContext)
    extends Filter
    with Logging {

  import CommandAuditFilter._

  def apply(nextFilter: RequestHeader => Future[Result])(rh: RequestHeader): Future[Result] = {
    // 只审计变更请求；OPTIONS（CORS 预检）/GET/HEAD 不产生命令
    if (!MutatingMethods.contains(rh.method)) nextFilter(rh)
    else {
      val start = System.currentTimeMillis()
      nextFilter(rh).map { result =>
        try {
          val handlerDef = rh.attrs.get(Router.Attrs.HandlerDef)
          val verified = bearerToken(rh.headers.get("Authorization")).flatMap(tokenService.verifyAccess)
          store.record(
            CommandAuditEntry(
              requestId = UUID.randomUUID().toString,
              occurredAtMillis = start,
              method = rh.method,
              routePattern = handlerDef.map(_.path),
              path = rh.path,
              controller = handlerDef.map(_.controller),
              // route action 名 == camelCase 命令名（parkConcept ↔ KN ParkConcept，查询侧规范化）
              command = handlerDef.map(_.method),
              entityId = handlerDef.flatMap(hd => firstPathParam(hd.path, rh.path)),
              userId = verified.map(_.userId),
              tenantId = verified.flatMap(_.tenant.map(_.tenantId)),
              status = result.header.status,
              durationMs = System.currentTimeMillis() - start
            )
          )
        } catch {
          case NonFatal(e) => logger.warn(s"command audit skipped for ${rh.method} ${rh.path}: ${e.getMessage}")
        }
        result
      }
    }
  }
}

object CommandAuditFilter {

  /** 变更动词：这些方法产生命令，其余（GET/HEAD/OPTIONS）只读不审计。 */
  val MutatingMethods: Set[String] = Set("POST", "PUT", "PATCH", "DELETE")

  /** `Authorization: Bearer <token>` → token；其余形态（含 Basic/裸串）返回 None。 */
  def bearerToken(authHeader: Option[String]): Option[String] =
    authHeader.flatMap {
      case h if h.length > 7 && h.substring(0, 7).equalsIgnoreCase("Bearer ") => Some(h.substring(7).trim)
      case _                                                           => None
    }

  /**
   * 从 route 模式串抽第一个路径参数值（实体 id best-effort）。
   * 模式串形如 `/api/concept/$conceptId<[^/]+>/park-concept`（动态段 `$name<regex>`）。
   */
  def firstPathParam(routePattern: String, actualPath: String): Option[String] = {
    val pat = routePattern.split('/').toVector
    val act = actualPath.split('/').toVector
    pat.indices
      .find(i => i < act.length && act(i).nonEmpty && pat(i).startsWith("$"))
      .map(i => act(i))
  }
}

/** 生产落账实现：scalikejdbc default 池（ScalikeJdbcSetup 于启动期初始化）。 */
@Singleton
class ScalikeJdbcCommandAuditStore @Inject() () extends CommandAuditStore with Logging {

  private val auditEc: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor { r =>
      val t = new Thread(r, "command-audit-writer")
      t.setDaemon(true)
      t
    })

  private val ensured = new AtomicBoolean(false)

  def record(entry: CommandAuditEntry): Unit =
    auditEc.execute { () =>
      try {
        ensureTable()
        DB.autoCommit { implicit s =>
          sql"""INSERT INTO command_audit
                (request_id, occurred_at_millis, method, route_pattern, path,
                 controller, command, entity_id, user_id, tenant_id, status, duration_ms)
                VALUES (
                  ${entry.requestId}, ${entry.occurredAtMillis}, ${entry.method},
                  ${entry.routePattern}, ${entry.path},
                  ${entry.controller}, ${entry.command}, ${entry.entityId},
                  ${entry.userId}, ${entry.tenantId}, ${entry.status}, ${entry.durationMs})""".update
            .apply()
        }
      } catch {
        case NonFatal(e) => logger.warn(s"command audit row dropped (${entry.method} ${entry.path}): ${e.getMessage}")
      }
    }

  /** 建表幂等（表已存在零副作用）；与 conf/sql/3.sql 同源——改这里必同步那边。 */
  private def ensureTable(): Unit = this.synchronized {
    if (ensured.get) return
    DB.autoCommit { implicit s =>
      sql"""CREATE TABLE IF NOT EXISTS command_audit (
            id BIGINT NOT NULL AUTO_INCREMENT,
            request_id VARCHAR(64) NOT NULL,
            occurred_at_millis BIGINT NOT NULL,
            method VARCHAR(8) NOT NULL,
            route_pattern VARCHAR(255),
            path VARCHAR(512) NOT NULL,
            controller VARCHAR(255),
            command VARCHAR(128),
            entity_id VARCHAR(128),
            user_id VARCHAR(128),
            tenant_id VARCHAR(128),
            status INT NOT NULL,
            duration_ms BIGINT NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT uq_command_audit_request_id UNIQUE (request_id)
          )""".update.apply()
    }
    ensured.set(true)
  }
}

/** DI 装配（独立小模块，不碰共享热点 BootstrapModule）。 */
class CommandAuditModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(bind[CommandAuditStore].to[ScalikeJdbcCommandAuditStore])
}
