package net.imadz.metarch.application

import net.imadz.common.auth.PasswordHashing
import javax.inject.{Inject, Singleton}
import net.imadz.metarch.application.aggregates.TenantProtocol._
import net.imadz.metarch.application.aggregates.UserProtocol._
import net.imadz.metarch.application.aggregates.WorkgroupProtocol._
import net.imadz.metarch.application.aggregates.repository.{TenantRepository, UserRepository, WorkgroupRepository}
import net.imadz.metarch.infrastructure.api.OkReceipt
import net.imadz.common.auth.ApiError
import net.imadz.metarch.infrastructure.auth.{IdentityStore, UserDoc}
import play.api.libs.json.JsString

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** RegisterUserFlow：注册开户 one-call 五步编排（B5-L D3 下沉·应用服务＝流程/事务边界，MANUAL 实写面——血缘审计「application service」类）。
 *
 *  真源锚点：story_RegisterUser（stories.json info.usecase＝完整对话规格；六键摘要 orchestrates 五步链）
 *  ＋S2-21 编排建模元数据化（知识库 design_docs/usecase_metadata_design.md）。
 *  规格步骤 ⇄ 本实现 --gate 对账：
 *    ⓪username 查重（R-REG：命中 409 USERNAME_TAKEN，Mongo username 唯一索引双保险）→ ①RegisterUser（工厂发号 generatedId 为聚合键）
 *    → ②identityStore.insert 身份库双写（SideEffect·登录可用，username=注册者自选登录名）
 *    → ③OnboardTenant（tenantId 服务端发号，前端零申报）→ ④AssignTenantOwner（soft 软断言 A2-06·失败不阻断）
 *    → ⑤CreateWorkgroup。此后重登 token 即带 tid（登录 owner 视图合并）；onboard-tenant 端点保留为补偿路径。
 *
  *  R-REG（2026-08-29 用户拍板）：userId 恒后端发号——入参只收 username，无 userId 参数；
 *  generatedId = UUID 同时充当聚合键＋登录身份 _id（名册引用链 ownerUserId 全链 UUID 通）。
 *  参数序＝协议 RegisterUser 声明序（生成器 flow 委托按 Req 字段序传参——错序即换值，禁手改）。
 *  D3b（EP_Mech_Key_Retirement·T 权威·修正 app）：tenantName/workgroupName/phoneNumber 随协议
 *  optional 化（④层声明 optional=true）→ Option[String]；下游 OnboardTenant/CreateWorkgroup
 *  仍 String——getOrElse("") 行为保持（缺省=未开域同名补偿路径可后补；真 None 语义挂账②）。
 */
@Singleton
class RegisterUserFlow @Inject() (
    userRepository: UserRepository,
    tenantRepository: TenantRepository,
    workgroupRepository: WorkgroupRepository,
    identityStore: IdentityStore,
)(implicit ec: ExecutionContext) {

  private implicit val timeout: akka.util.Timeout = akka.util.Timeout(5.seconds)

  def register(username: String, email: String, password: String, displayName: String,
               authMethod: String, tenantName: Option[String], workgroupName: Option[String],
               phoneNumber: Option[String]): Future[Either[ApiError, OkReceipt]] = {
    val tenantNameV = tenantName.getOrElse("")
    val workgroupNameV = workgroupName.getOrElse("")
    identityStore.findByUsername(username).flatMap {
      case Some(_) => Future.successful(Left(ApiError(409, "USERNAME_TAKEN", s"username already taken: $username")))
      case None =>
        val generatedId = java.util.UUID.randomUUID().toString
        userRepository.entityRef(generatedId).ask {
          (replyTo: akka.actor.typed.ActorRef[RegisterUserReply]) =>
            RegisterUser(net.imadz.common.Id.of(generatedId), email, password, displayName, authMethod, tenantName, workgroupName, phoneNumber, generatedId, replyTo)
        }.flatMap { reply =>
          reply.error match {
            case Some(e) => Future.successful(Left(ApiError.badRequest(e.code, e.message)))
            case None =>
              identityStore.insert(UserDoc(_id = generatedId, username = username,
                passwordHash = PasswordHashing.hash(password), roles = List("tenantOwner"), tokenVersion = 1,
                createdAt = System.currentTimeMillis())).flatMap { _ =>
                val tenantId = java.util.UUID.randomUUID().toString
                tenantRepository.entityRef(tenantId).ask {
                  (replyTo: akka.actor.typed.ActorRef[OnboardTenantReply]) =>
                    OnboardTenant(net.imadz.common.Id.of(tenantId), tenantNameV, net.imadz.common.Id.of(generatedId), generatedId, replyTo)
                }.flatMap { tReply =>
                  tReply.error match {
                    case Some(e) => Future.successful(Left(ApiError.badRequest(e.code, e.message)))
                    case None =>
                      // 租户 Owner 软链（A2-06）：AssignTenantOwner 失败不阻断开域主流程
                      tenantRepository.entityRef(tenantId).ask {
                        (replyTo: akka.actor.typed.ActorRef[AssignTenantOwnerReply]) =>
                          AssignTenantOwner(net.imadz.common.Id.of(tenantId), net.imadz.common.Id.of(generatedId), generatedId, replyTo)
                      }.map(_.error.isEmpty).recover { case _ => false }.flatMap { _ =>
                        val workgroupId = java.util.UUID.randomUUID().toString
                        workgroupRepository.entityRef(workgroupId).ask {
                          (replyTo: akka.actor.typed.ActorRef[CreateWorkgroupReply]) =>
                            CreateWorkgroup(net.imadz.common.Id.of(workgroupId), net.imadz.common.Id.of(tenantId), workgroupNameV, generatedId, replyTo)
                        }.map { wReply =>
                          wReply.error match {
                            case Some(e) => Left(ApiError.badRequest(e.code, e.message))
                            case None => Right(OkReceipt(generatedId, JsString("user-profiles")))
                          }
                        }
                      }
                  }
                }.recover { case NonFatal(t) =>
                  play.api.Logger(getClass).error("registerUser tenant chain failure", t)
                  Left(ApiError(500, "INTERNAL_ERROR", "internal error, please retry later"))
                }
            }
          }
        }.recover { case NonFatal(t) =>
          play.api.Logger(getClass).error("registerUser ask-chain failure", t)
          Left(ApiError(500, "INTERNAL_ERROR", "internal error, please retry later"))
        }
    }.recover { case NonFatal(t) =>
      play.api.Logger(getClass).error("registerUser username-uniqueness probe failure", t)
      Left(ApiError(500, "INTERNAL_ERROR", "internal error, please retry later"))
    }
  }
}
