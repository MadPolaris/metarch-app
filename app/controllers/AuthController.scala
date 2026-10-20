package controllers

import net.imadz.common.auth.{AuthAction, LoginRateLimiter, PasswordHashing, TenantIdentity, TokenService}

import javax.inject.{Inject, Singleton}
import net.imadz.metarch.application.projection.repository.{InviteTenantMemberDecisionViewRepository, OnboardTenantDecisionViewRepository}
import net.imadz.metarch.infrastructure.auth.{IdentityStore, UserDoc}
import net.imadz.common.auth.Roles
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.{ExecutionContext, Future}

/** 认证端点（EP：Authentication_Design §三，gen_api_layer 直出批次 B1） */
@Singleton
class AuthController @Inject() (
                                 identityStore: IdentityStore,
                                 tokens: TokenService,
                                 rateLimiter: LoginRateLimiter,
                                 roster: InviteTenantMemberDecisionViewRepository,
                                 onboardView: OnboardTenantDecisionViewRepository,
                                 authAction: AuthAction,
                                 val cc: ControllerComponents)(implicit ec: ExecutionContext)
  extends BaseController {
  override def controllerComponents: ControllerComponents = cc

  private val logger = play.api.Logger(this.getClass)
  private val Login500MarkerContext =
    play.api.MarkerContext(org.slf4j.MarkerFactory.getMarker("LOGIN_500"))

  private def extractString(body: JsValue, field: String): Option[String] =
    (body \ field).asOpt[String]

  /** D5-A：登录时经名册解析活跃归属并烧入 token。
    * 判定=joinedAt 非空且 removedAt 为空（与 Feed 映射表同源时间戳语义，不猜 status 字符串）；
    * 0 归属→无 tid（建租户前用户可登录）；≥2→无 tid＋tenantIds 清单（L7·Q3 裁决 2026-09-07：
    * 多组织是领域事实，拒登退役——登录发无租户 token，客户端经 /api/auth/tenants 挑选后
    * POST /api/auth/tenant-switch 重签发；token 模型不变〔tid 本就可选〕）。
    * 2026-08-27 引导链修复：合并 OnboardTenant 视图 ownerUserId——D5-A 捷径 A 只认 invite 名册，
    * 但 OnboardTenant 命令只发 TenantOnboarded（无 invite/join 事件），owner 名册永无行 → 引导后重登仍无 tid。
    * owner 即租户身份（§九 命令侧反查链同源语义）；归档/挂起租户不计（对偶 removedAt==null）。
    * S2-87 WP-C（2026-09-13）：保 owner/roster 两源判别——mrole 烧值按归属性质定档
    * （TenantOwner/TenantMember·域模型拍板：Tenant 生命周期域归 Owner 管理）。 */
  private def activeTenants(userId: String): Future[(List[String], List[String])] =
    roster.findByUserId(userId).flatMap { rows =>
      val rosterIds = rows.filter(r => r.joinedAt.isDefined && r.removedAt.isEmpty).map(_.tenantId) // Option[Instant] 语义（4d6108d9 涟漪·None==null 恒 false 惯用法退役）
      onboardView.findByOwnerUserId(userId).map { owned =>
        val ownerIds = owned.filter(v => v.archivedAt.isEmpty && v.suspendedAt.isEmpty).map(_.tenantId)
        (ownerIds, rosterIds)
      }
    }

  private def activeTenantIds(userId: String): Future[List[String]] =
    activeTenants(userId).map { case (ownerIds, rosterIds) => (rosterIds ++ ownerIds).distinct.sorted }

  /** S2-87 WP-C：mrole 烧值——owner 源命中=TenantOwner（生命周期域 Owner 至上），否则名册成员=TenantMember */
  private def mroleFor(ownerIds: List[String], tenantId: String): String =
    if (ownerIds.contains(tenantId)) Roles.TenantOwner else Roles.TenantMember

  /** 单归属直烧 tid＋mrole（S2-87 WP-C：A3「role 置空串省略烧录」退役）；零/多归属发无租户 token（多归属附 tenantIds 供客户端选） */
  private def membershipPayload(username: String, roles: List[String], userId: String): Future[JsObject] =
    activeTenants(userId).map { case (ownerIds, rosterIds) =>
      (rosterIds ++ ownerIds).distinct.sorted match {
        case single :: Nil => authResponse(username, roles, userId, Some(TenantIdentity(single, mroleFor(ownerIds, single))))
        case tids          => authResponse(username, roles, userId, None) ++ Json.obj("tenantIds" -> tids)
      }
    }

  private def tokenPair(username: String, roles: List[String], userId: String,
                        tenant: Option[TenantIdentity]): (String, String) = {
    val (access, _) = tokens.issueAccessToken(userId, 1, tenant)
    val refreshExp = System.currentTimeMillis() + 2592000000L
    val refresh = java.util.Base64.getEncoder.encodeToString(
      s"$username:$refreshExp".getBytes("UTF-8"))
    (access, refresh)
  }

  private def authResponse(username: String, roles: List[String], userId: String,
                           tenant: Option[TenantIdentity] = None): JsObject = {
        val (access, refresh) = tokenPair(username, roles, userId, tenant)
    // 2026-08-27：expiresIn 用真实 TTL（原硬编码 900 与 config 脱节——accessTtlSeconds=4h 后误导前端）
    val expiresIn = tokens.accessTtlSeconds
    Json.obj("status" -> "ok", "accessToken" -> access, "refreshToken" -> refresh, "expiresIn" -> expiresIn)
  }

  /** POST /api/auth/register */
  def register(): Action[JsValue] = Action.async(parse.json) { request =>
    val username = extractString(request.body, "username").getOrElse("")
    val password = extractString(request.body, "password").getOrElse("")
    if (username.isEmpty || password.length < 10 ||
      !password.matches(".*[A-Za-z].*") || !password.matches(".*[0-9].*")) {
      Future.successful(BadRequest(Json.obj("errorCode" -> "PASSWORD_WEAK",
        "message" -> "username required; password >=10 chars with letters and digits")))
    } else {
      identityStore.findByUsername(username).flatMap {
        case Some(_) => Future.successful(Conflict(Json.obj("errorCode" -> "USERNAME_TAKEN")))
        case None =>
          // R-REG（S2-86 端点表裁决·2026-09-13 邀请入租路径首触实证）：_id＝后端发号 UUID，
          // 与 username 分离——InviteTenantMember 等命令面 IdGuard 按 UUID 执法，
          // _id=username 的账号永远无法被邀入租户（协作面候选池断裂）。
          val doc = UserDoc(_id = java.util.UUID.randomUUID().toString, username = username,
            passwordHash = PasswordHashing.hash(password), roles = List("tenantOwner"),
            tokenVersion = 1, createdAt = System.currentTimeMillis())
          identityStore.insert(doc).flatMap { _ =>
            membershipPayload(username, doc.roles, doc._id).map(Created(_))
          }
      }
    }
  }

  /** POST /api/auth/login */
  def login(): Action[JsValue] = Action.async(parse.json) { request =>
    val ip = request.remoteAddress
    val usernameKey = extractString(request.body, "username").getOrElse("-")
    if (rateLimiter.isBlocked(usernameKey, ip)) {
      Future.successful(TooManyRequests(Json.obj("errorCode" -> "RATE_LIMITED", "message" -> "try later")))
    } else {
      val username = extractString(request.body, "username").getOrElse("-")
      val password = extractString(request.body, "password").getOrElse("")
      identityStore.findByUsername(username).flatMap {
        case Some(user) if !PasswordHashing.verify(password, user.passwordHash) =>
          failLogin(username, ip)
        case Some(user) =>
          rateLimiter.recordSuccess(username, ip)
          // R-REG（2026-08-30 WP-6 硬门修复）：token uid＝UserDoc._id（后端发号 UUID），非 username——
          // one-call 注册后 _id=UUID 与 username 分离；tid 解析亦须按 _id 对齐名册 ownerUserId。
          membershipPayload(user.username, user.roles, user._id).map(Ok(_))
        case None =>
          failLogin(username, ip)
      }.recover { case scala.util.control.NonFatal(t) =>
        logger.error(s"LOGIN_500 unexpected login failure; username=$username ip=$ip", t)(
          Login500MarkerContext)
        InternalServerError(Json.obj("errorCode" -> "LOGIN_INTERNAL_ERROR"))
      }
    }
  }

  /** POST /api/auth/refresh —— v1：无状态刷新（B2 加轮换+重用检测） */
  def refresh(): Action[JsValue] = Action.async(parse.json) { request =>
    Future.successful(Ok(Json.obj("status" -> "ok", "note" -> "refresh validation lands in B2")))
  }

  /** POST /api/auth/logout —— v1：客户端丢弃（服务端吊销在 B2） */
  def logout(): Action[AnyContent] = Action { Ok(Json.obj("status" -> "ok")) }

  /** POST /api/auth/change-password —— 改密即 tokenVersion+1（全端点失效） */
  def changePassword(): Action[JsValue] = Action.async(parse.json) { request =>
    val username = extractString(request.body, "username").getOrElse("")
    val oldPw = extractString(request.body, "oldPassword").getOrElse("")
    val newPw = extractString(request.body, "newPassword").getOrElse("")
    identityStore.findByUsername(username).flatMap {
      case Some(user) if !PasswordHashing.verify(oldPw, user.passwordHash) =>
        Future.successful(Unauthorized(Json.obj("errorCode" -> "BAD_CREDENTIALS")))
      case Some(user) =>
        identityStore.updatePassword(username, PasswordHashing.hash(newPw), user.tokenVersion + 1)
          .map(_ => Ok(Json.obj("status" -> "ok", "note" -> "all tokens invalidated")))
      case None => Future.successful(Unauthorized(Json.obj("errorCode" -> "BAD_CREDENTIALS")))
    }
  }

  /** GET /api/auth/me —— 已认证身份回显（E302 收口：原 B1 占位升级；D5-A 增租户上下文回显） */
  def me(): Action[AnyContent] = authAction { authReq =>
    val base = Json.obj("userId" -> authReq.userId, "status" -> "ok")
    Ok(authReq.tenant
      .map(t => base ++ Json.obj("tenantId" -> t.tenantId))
      .getOrElse(base))
  }

  /** GET /api/auth/tenants —— 活跃归属清单（L7·Q3 裁决 2026-09-07：多组织选租户的挑选面）。
    * 名册判据与登录 activeTenantIds 同源——登录能见的 tid 这里全量可见。 */
  def tenants(): Action[AnyContent] = authAction.async { authReq =>
    activeTenantIds(authReq.userId).map { tids =>
      val current: String = authReq.tenant.map(_.tenantId).getOrElse("")
      Ok(Json.obj("status" -> "ok", "tenantIds" -> tids) ++
        Json.obj("currentTenantId" -> current))
    }
  }

  /** POST /api/auth/tenant-switch —— 切换租户会话（L7·Q3 裁决：token 重签发·改动面小·token 模型不变）。
    * 成员资格按切换时刻名册实时判定（与登录同源——登录后被动出册的成员无法再切入该租户）；
    * 成功＝重签发带 tid 的 access token（refresh token 与租户无关不重发）；
    * 非成员 → 403 TENANT_NOT_MEMBER（跨租户切换被名册拒绝，不静默放行）。 */
  def tenantSwitch(): Action[JsValue] = authAction.async(parse.json) { authReq =>
    val target = extractString(authReq.body, "tenantId").getOrElse("")
    if (target.isEmpty) {
      Future.successful(BadRequest(Json.obj("errorCode" -> "TENANT_ID_REQUIRED",
        "message" -> "tenantId required: pick from GET /api/auth/tenants")))
    } else {
      activeTenants(authReq.userId).map { case (ownerIds, rosterIds) =>
        if ((rosterIds ++ ownerIds).contains(target)) {
          val (access, _) = tokens.issueAccessToken(authReq.userId, 1, Some(TenantIdentity(target, mroleFor(ownerIds, target))))
          Ok(Json.obj("status" -> "ok", "accessToken" -> access,
            "tenantId" -> target, "expiresIn" -> tokens.accessTtlSeconds))
        } else {
          Forbidden(Json.obj("errorCode" -> "TENANT_NOT_MEMBER",
            "message" -> s"no active membership in tenant $target"))
        }
      }
    }
  }

  private def failLogin(username: String, ip: String): Future[Result] = {
    rateLimiter.recordFailure(username, ip)
    Future.successful(Unauthorized(Json.obj("errorCode" -> "BAD_CREDENTIALS", "message" -> "invalid credentials")))
  }
}
