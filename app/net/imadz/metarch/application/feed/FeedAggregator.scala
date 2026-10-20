package net.imadz.metarch.application.feed

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import net.imadz.metarch.application.projection.repository._

final case class FeedActor(userId: String, displayName: String)
final case class FeedProject(projectId: String, name: String)
final case class FeedItem(itemType: String, viewKey: String, entityId: String, title: String,
  actors: List[FeedActor], project: Option[FeedProject], occurredAt: Instant, relevance: String)
final case class InboxItem(viewKey: String, entityId: String, summary: String, occurredAt: Instant)
final case class InboxBucket(key: String, count: Int, items: List[InboxItem])
final case class UserRef(userId: String, displayName: String, email: Option[String])
final case class MeSnapshot(userId: String, displayName: Option[String], email: Option[String],
  tenantId: Option[String], tenantName: Option[String], roles: List[String], status: String)

object FeedAggregator {

  val Modules: Seq[String] = Seq("workshop", "knowledge", "admin")
  val DefaultLimit: Int = 6
  val MaxLimit: Int = 20
  private val OpenPatchStatuses = Set("PENDING", "READY_TO_MERGE")
  private val BucketOrder: Seq[String] = Seq("collision", "question", "parked", "patch", "conflict")

  private def byDesc(items: Iterable[FeedItem]): List[FeedItem] =
    items.toList.sortBy(i => (-i.occurredAt.toEpochMilli, i.entityId, i.itemType))

  private def inboxDesc(items: Iterable[InboxItem]): List[InboxItem] =
    items.toList.sortBy(i => (-i.occurredAt.toEpochMilli, i.entityId))

  private def firstNonNullOrEpoch(is: Instant*): Instant =
    is.find(_ != null).getOrElse(Instant.EPOCH)

  private def snippet(s: String): String = {
    val t = if (s == null) "" else s.trim
    if (t.length <= 24) t else t.take(24) + "…"
  }

  private def short(id: String): String = {
    val t = if (id == null) "" else id
    if (t.length <= 8) t else t.take(8)
  }
}

@javax.inject.Singleton
class FeedAggregator @javax.inject.Inject() (
  scheduleWorkshopRepo: ScheduleWorkshopDecisionViewRepository,
  openSessionRepo: OpenSessionDecisionViewRepository,
  initiateProjectRepo: InitiateProjectDecisionViewRepository,
  parkConceptRepo: ParkConceptDecisionViewRepository,
  classifySubdomainRepo: ClassifySubdomainDecisionViewRepository,
  discoverContextRepo: DiscoverContextDecisionViewRepository,
  defineContextRelationRepo: DefineContextRelationDecisionViewRepository,
  captureNoteRepo: CaptureNoteDecisionViewRepository,
  submitKnowledgePatchRepo: SubmitKnowledgePatchDecisionViewRepository,
  createCatalogVersionRepo: CreateCatalogVersionDecisionViewRepository,
  onboardTenantRepo: OnboardTenantDecisionViewRepository,
  inviteTenantMemberRepo: InviteTenantMemberDecisionViewRepository,
  inviteWorkgroupMemberRepo: InviteWorkgroupMemberDecisionViewRepository,
  updateUserProfileRepo: UpdateUserProfileDecisionViewRepository)(implicit ec: ExecutionContext) {

  import FeedAggregator._

  private case class Ctx(caller: String,
    tenantId: Option[String],
    names: Map[String, String],
    emails: Map[String, String],
    projectNames: Map[String, String],
    projectOwner: Map[String, String],
    projectWorkgroup: Map[String, String],
    workgroupMembers: Map[String, List[net.imadz.common.CommonTypes.Id]], // 批3 UUID 统一后成员名单为 List[Id]
    workshopProject: Map[String, String],
    sessionWorkshop: Map[String, String])

  // R2 恢复精化（2026-08-27）：租户作用域数据源——有 tid 走 findByTenantId，无 tid 空（不泄漏他租户）
  private def scoped[T](tid: Option[String], byTenant: String => Future[List[T]]): Future[List[T]] =
    tid match {
      case Some(t) => byTenant(t)
      case None    => Future.successful(Nil)
    }

  private def loadCtx(caller: String): Future[Ctx] = {
    val profilesF = updateUserProfileRepo.findAll()
    val rosterF = inviteTenantMemberRepo.findAll()
    val onboardF = onboardTenantRepo.findAll()
    val projectsF = initiateProjectRepo.findAll()
    val workgroupsF = inviteWorkgroupMemberRepo.findAll()
    val workshopsF = scheduleWorkshopRepo.findAll()
    val sessionsF = openSessionRepo.findAll()
    for {
      profiles <- profilesF
      roster <- rosterF
      onboard <- onboardF
      projects <- projectsF
      workgroups <- workgroupsF
      workshops <- workshopsF
      sessions <- sessionsF
    } yield {
      val namesB = scala.collection.mutable.Map.empty[String, String]
      val emailsB = scala.collection.mutable.Map.empty[String, String]
      profiles.foreach { p =>
        if (p.displayName != null && p.displayName.nonEmpty) namesB += (p.userId -> p.displayName)
        if (p.email != null && p.email.nonEmpty) emailsB += (p.userId -> p.email)
      }
      roster.foreach { m =>
        if (!namesB.contains(m.userId) && m.displayName != null && m.displayName.nonEmpty) namesB += (m.userId -> m.displayName)
        if (!emailsB.contains(m.userId) && m.email != null && m.email.nonEmpty) emailsB += (m.userId -> m.email)
      }
      // R2 恢复精化（2026-08-27）：调用者租户解析（与登录语义一致：名册活跃行 ∪ owner 视图，多租户歧义→None）
      val callerRoster = roster.filter(r => r.userId == caller && r.joinedAt.isDefined && r.removedAt.isEmpty).map(_.tenantId)
      // S2-81 名册换源：成员判定＝orgMemberIds 含 orgMemberIdOf(tenantId, caller)（原 List[Id].contains(String) 恒 false）
      val ownerTenant = onboard.filter(v => v.archivedAt.isEmpty && v.suspendedAt.isEmpty && (v.ownerUserId == caller || net.imadz.metarch.domain.entities.OrganizationMemberIds.isMemberOf(v.tenantId, caller, v.orgMemberIds))).map(_.tenantId)
      val tenantId = (callerRoster ++ ownerTenant).distinct match {
        case single :: Nil => Some(single)
        case _             => None
      }
      Ctx(caller,
        tenantId,
        namesB.toMap,
        emailsB.toMap,
        projects.map(p => p.projectId -> p.name).toMap,
        projects.map(p => p.projectId -> p.ownerUserId).toMap,
        projects.map(p => p.projectId -> p.workgroupId).toMap,
        workgroups.map(w => w.workgroupId -> (w.memberUserIds ++ w.invitedUserIds).distinct).toMap, // List[Id]：contains 比较点已 toString 对齐
        workshops.map(w => w.workshopId -> w.projectId).toMap,
        sessions.collect { case s if s.workshopId != null => s.sessionId -> s.workshopId }.toMap)
    }
  }

  private def actor(ctx: Ctx, userId: String): Option[FeedActor] =
    ctx.names.get(userId).filter(_.nonEmpty).map(FeedActor(userId, _))

  private def actors(ctx: Ctx, userIds: List[String]): List[FeedActor] =
    userIds.filter(_ != null).distinct.flatMap(actor(ctx, _)).toList

  private def projectOf(ctx: Ctx, projectId: String): Option[FeedProject] =
    ctx.projectNames.get(projectId).map(FeedProject(projectId, _))

  private def relProject(ctx: Ctx, projectId: String): String =
    if (ctx.projectOwner.get(projectId).contains(ctx.caller)) "owner"
    else if (ctx.projectWorkgroup.get(projectId).flatMap(ctx.workgroupMembers.get).exists(_.map(_.toString).contains(ctx.caller))) "member"
    else "tenant"

  private def relUsers(ctx: Ctx, driverIds: List[String]): String =
    if (driverIds.contains(ctx.caller)) "owner" else "tenant"

  private def mkItem(ctx: Ctx, itemType: String, viewKey: String, entityId: String, title: String,
    actorIds: List[String], project: Option[FeedProject], at: Instant, relevance: String): FeedItem =
    FeedItem(itemType, viewKey, entityId, title, actors(ctx, actorIds), project, at, relevance)

  private def lineage(ctx: Ctx, sessionId: String): Option[FeedProject] =
    ctx.sessionWorkshop.get(sessionId)
      .flatMap(ctx.workshopProject.get)
      .flatMap(pid => projectOf(ctx, pid))

  private def workshopModule(ctx: Ctx): Future[List[FeedItem]] = {
    val swsF = scoped(ctx.tenantId, scheduleWorkshopRepo.findByTenantId)
    val ossF = scoped(ctx.tenantId, openSessionRepo.findByTenantId)
    val pjsF = scoped(ctx.tenantId, initiateProjectRepo.findByTenantId)
    for {
      sws <- swsF
      oss <- ossF
      pjs <- pjsF
    } yield {
      val scheduled = sws.collect {
        case sw if sw.scheduledAt.isDefined =>
          mkItem(ctx, "workshop.scheduled", "schedule-workshop", sw.workshopId,
            s"工作坊已排期（项目 ${ctx.projectNames.getOrElse(sw.projectId, sw.projectId)}）",
            Nil, projectOf(ctx, sw.projectId), sw.scheduledAt.get, relProject(ctx, sw.projectId))
      }.toList
      val sessions = oss.flatMap { os =>
        val times = List(
          (os.openedAt, "session.opened", "已打开"),
          (os.preparedAt, "session.prepared", "已就绪"),
          (os.closedAt, "session.closed", "已关闭"))
        times.flatMap {
          case (t, ty, verb) if t.isDefined =>
            val rel =
              if ((os.participantUserIds ++ os.observerUserIds).map(_.toString).contains(ctx.caller)) "member" else "tenant"
            val proj = ctx.workshopProject.get(os.workshopId).flatMap(pid => projectOf(ctx, pid))
            Some(mkItem(ctx, ty, "open-session", os.sessionId,
              s"场次「${os.name}」$verb", os.participantUserIds.map(_.toString), proj, t.get, rel))
          case _ => None
        }
      }.toList
      val projects = pjs.flatMap { p =>
        val driverIds = List(p.createdBy, p.ownerUserId).filter(_ != null)
        val rel =
          if (driverIds.contains(ctx.caller)) "owner"
          else if (ctx.workgroupMembers.get(p.workgroupId).exists(_.map(_.toString).contains(ctx.caller))) "member"
          else "tenant"
        List(
          ("project.initiated", "已立项", p.createdAt),
          ("project.started", "已启动", p.startedAt),
          ("project.paused", "已暂停", p.pausedAt),
          ("project.completed", "已完成", p.completedAt),
          ("project.cancelled", "已取消", p.cancelledAt)).flatMap {
          case (ty, verb, t) if t.isDefined =>
            Some(mkItem(ctx, ty, "initiate-project", p.projectId, s"项目「${p.name}」$verb",
              driverIds, Some(FeedProject(p.projectId, p.name)), t.get, rel))
          case _ => None
        }
      }.toList
      byDesc(scheduled ++ sessions ++ projects)
    }
  }

  private def knowledgeModule(ctx: Ctx): Future[List[FeedItem]] = {
    val pcsF = scoped(ctx.tenantId, parkConceptRepo.findByTenantId)
    val cssF = scoped(ctx.tenantId, classifySubdomainRepo.findByTenantId)
    val dcsF = scoped(ctx.tenantId, discoverContextRepo.findByTenantId)
    val crsF = scoped(ctx.tenantId, defineContextRelationRepo.findByTenantId)
    val notesF = scoped(ctx.tenantId, captureNoteRepo.findByTenantId)
    val patchesF = scoped(ctx.tenantId, submitKnowledgePatchRepo.findByTenantId)
    val catsF = scoped(ctx.tenantId, createCatalogVersionRepo.findByTenantId)
    for {
      pcs <- pcsF
      css <- cssF
      dcs <- dcsF
      crs <- crsF
      notes <- notesF
      patches <- patchesF
      cats <- catsF
    } yield {
      val concepts = pcs.flatMap { pc =>
        val drivers = List(pc.establishedBy, pc.parkedBy, pc.createdBy).filter(_ != null)
        List(
          ("concept.established", "已建立", pc.establishedAt, pc.establishedBy, true),
          ("concept.parked", "已停放", pc.parkedAt, pc.parkedBy, pc.removedAt.isEmpty)).flatMap {
          case (ty, verb, t, by, guard) if t.isDefined && guard =>
            Some(mkItem(ctx, ty, "park-concept", pc.conceptId.toString,
              s"概念「${pc.name}」$verb", List(by), None, t.get, relUsers(ctx, drivers)))
          case _ => None
        }
      }.toList
      val subdomains = css.collect {
        case cs if cs.classifiedAt.isDefined =>
          mkItem(ctx, "subdomain.classified", "classify-subdomain", cs.subdomainId,
            s"子域「${cs.name}」已归类（${cs.kind}）", List(cs.classifiedBy), None, cs.classifiedAt.get,
            relUsers(ctx, List(cs.classifiedBy)))
      }.toList
      val contexts = dcs.collect {
        case dc if dc.discoveredAt.isDefined =>
          mkItem(ctx, "context.discovered", "discover-context", dc.contextId,
            s"上下文「${dc.name}」已发现", List(dc.discoveredBy), None, dc.discoveredAt.get,
            relUsers(ctx, List(dc.discoveredBy)))
      }.toList
      val relations = crs.collect {
        case cr if cr.createdAt.isDefined =>
          mkItem(ctx, "context-mapping.defined", "define-context-relation", cr.relationId,
            s"上下文关系（${cr.kind}）已定义", List(cr.createdBy), None, cr.createdAt.get,
            relUsers(ctx, List(cr.createdBy)))
      }.toList
      val noteItems = notes.collect {
        case n if n.removedAt.isEmpty && n.createdAt.isDefined =>
          mkItem(ctx, "note.captured", "capture-note", n.noteId,
            s"便签：「${snippet(n.content)}」", List(n.createdBy),
            lineage(ctx, n.sessionId), n.createdAt.get, relUsers(ctx, List(n.createdBy)))
      }.toList
      val patchItems = patches.flatMap { p =>
        List(
          ("patch.submitted", "已提交", p.createdAt),
          ("patch.merged", "已合并", p.mergedAt)).flatMap {
          case (ty, verb, t) if t.isDefined =>
            Some(mkItem(ctx, ty, "submit-knowledge-patch", p.patchId,
              s"知识补丁 ${short(p.patchId)} $verb", List(p.createdBy),
              lineage(ctx, p.sessionId), t.get, relUsers(ctx, List(p.createdBy))))
          case _ => None
        }
      }.toList
      val catalogItems = cats.flatMap { c =>
        List(
          ("catalog.released", "已发布", c.releasedAt, c.releasedBy),
          ("catalog.exported", "已导出", c.exportedAt, c.exportedBy)).flatMap {
          case (ty, verb, t, by) if t.isDefined =>
            Some(mkItem(ctx, ty, "create-catalog-version", c.catalogId,
              s"目录版本 v${c.version} $verb", List(by), projectOf(ctx, c.projectId), t.get,
              relUsers(ctx, List(by))))
          case _ => None
        } ++
          (if (c.conflictId != null && c.resolvedAt.isEmpty)
            Some(mkItem(ctx, "catalog.conflict-detected", "create-catalog-version", c.catalogId,
              s"目录版本 v${c.version} 检测到冲突 ${c.conflictId}", Nil, projectOf(ctx, c.projectId),
              firstNonNullOrEpoch(c.detectedAt.get, c.assembledAt.get), "tenant"))
          else None)
      }.toList
      byDesc(concepts ++ subdomains ++ contexts ++ relations ++ noteItems ++ patchItems ++ catalogItems)
    }
  }

  private def adminModule(ctx: Ctx): Future[List[FeedItem]] = {
    // S0-P0 读侧隔离补漏（2026-09-01）：tenant/roster 两源按调用者租户裁剪（原 findAll 全库发射他租户入驻/名册事件）
    val tenantsF = scoped(ctx.tenantId, tid => onboardTenantRepo.findAll().map(_.filter(_.tenantId == tid)))
    val membersF = scoped(ctx.tenantId, tid => inviteTenantMemberRepo.findAll().map(_.filter(_.tenantId == tid)))
    val workgroupsF = scoped(ctx.tenantId, inviteWorkgroupMemberRepo.findByTenantId)
    for {
      ts <- tenantsF
      ms <- membersF
      ws <- workgroupsF
    } yield {
      val tenantItems = ts.flatMap { t =>
        val rel =
          if (t.ownerUserId != null && t.ownerUserId == ctx.caller) "owner"
          // S2-81 名册换源：成员判定＝orgMemberId 典范派生（原 userId 直查名册）
          else if (net.imadz.metarch.domain.entities.OrganizationMemberIds.isMemberOf(t.tenantId, ctx.caller, t.orgMemberIds)) "member" else "tenant"
        List(
          ("tenant.onboarded", "已入驻", t.createdAt),
          ("tenant.suspended", "已停用", t.suspendedAt),
          ("tenant.archived", "已归档", t.archivedAt)).flatMap {
          case (ty, verb, at) if at.isDefined =>
            Some(mkItem(ctx, ty, "onboard-tenant", t.tenantId, s"租户「${t.name}」$verb",
              List(t.ownerUserId), None, at.get, rel))
          case _ => None
        }
      }.toList
      val memberItems = ms.flatMap { m =>
        val who = ctx.names.getOrElse(m.userId, m.userId)
        List(
          ("tenant-member.joined", "已加入租户", m.joinedAt),
          ("tenant-member.removed", "已移出租户", m.removedAt)).flatMap {
          case (ty, verb, at) if at.isDefined =>
            Some(mkItem(ctx, ty, "invite-tenant-member", m.userId, s"$who $verb",
              List(m.userId), None, at.get, relUsers(ctx, List(m.userId))))
          case _ => None
        }
      }.toList
      val workgroupItems = ws.collect {
        case w if w.createdAt.isDefined =>
          val rel =
            if (w.ownerUserId != null && w.ownerUserId == ctx.caller) "owner"
            else if ((w.memberUserIds ++ w.invitedUserIds).map(_.toString).contains(ctx.caller)) "member"
            else "tenant"
          mkItem(ctx, "workgroup.created", "invite-workgroup-member", w.workgroupId,
            s"工作组「${w.name}」已创建", List(w.ownerUserId), None, w.createdAt.get, rel)
      }.toList
      byDesc(tenantItems ++ memberItems ++ workgroupItems)
    }
  }

  def feed(caller: String, modules: Set[String], scope: String, limit: Int,
    before: Option[Instant]): Future[List[(String, List[FeedItem])]] = {
    // R2 恢复精化（2026-08-27，R10 补列完成）：各 module 数据源已切 findByTenantId（scoped），
    // 无租户调用者得空模块（不泄漏他租户）；scope=mine/tenant 语义不变。
    loadCtx(caller).flatMap { ctx =>
      val wanted = if (modules.isEmpty) Modules else Modules.filter(modules.contains)
      val effLimit = if (limit <= 0) DefaultLimit else math.min(limit, MaxLimit)
      val effScope = if (scope == "mine") "mine" else "tenant"
      Future.sequence(List(
        workshopModule(ctx).map("workshop" -> _),
        knowledgeModule(ctx).map("knowledge" -> _),
        adminModule(ctx).map("admin" -> _))).map { pairs =>
        pairs.collect { case pair @ (m, _) if wanted.contains(m) => pair }.map {
          case (m, items) =>
            val filtered = items
              .filter(i => before.forall(i.occurredAt.isBefore))
              .filterNot(i => effScope == "mine" && i.relevance == "tenant")
            m -> byDesc(filtered).take(effLimit)
        }
      }
    }
  }

// R2/R3 恢复精化（2026-08-27）：租户解析（名册活跃行 ∪ owner 视图，多租户歧义→None；与登录同源语义）
  def resolveTenantId(caller: String): Future[Option[String]] =
    for {
      roster <- inviteTenantMemberRepo.findAll()
      onboard <- onboardTenantRepo.findAll()
    } yield {
      val rosterIds = roster.filter(r => r.userId == caller && r.joinedAt.isDefined && r.removedAt.isEmpty).map(_.tenantId)
      // S2-81 名册换源：同 loadCtx 口径（orgMemberIds 含 orgMemberIdOf(tenantId, caller)）
      val ownerIds = onboard.filter(v => v.archivedAt.isEmpty && v.suspendedAt.isEmpty && (v.ownerUserId == caller || net.imadz.metarch.domain.entities.OrganizationMemberIds.isMemberOf(v.tenantId, caller, v.orgMemberIds))).map(_.tenantId)
      (rosterIds ++ ownerIds).distinct match {
        case single :: Nil => Some(single)
        case _             => None
      }
    }

  def inbox(tenantId: Option[String]): Future[(Int, List[InboxBucket])] = {
    // R3 恢复精化（2026-08-27，R10 补列完成）：inbox 数据源按调用者租户裁剪——租户由 FeedApiController
    // 传入（与登录 resolveTenantMembership 同源解析）；无租户调用者得空桶（不泄漏他租户）。
    tenantId match {
      case Some(tid) =>
        val notesF = captureNoteRepo.findByTenantId(tid)
        val conceptsF = parkConceptRepo.findByTenantId(tid)
        val patchesF = submitKnowledgePatchRepo.findByTenantId(tid)
        val catalogsF = createCatalogVersionRepo.findByTenantId(tid)
        captureNoteRepo.findByTenantId(tid).flatMap { notes =>
          parkConceptRepo.findByTenantId(tid).flatMap { concepts =>
            submitKnowledgePatchRepo.findByTenantId(tid).flatMap { patches =>
              createCatalogVersionRepo.findByTenantId(tid).map { catalogs =>
                // R3 恢复（原 yield 块）
                {
        val collision = InboxBucket("collision",
          notes.count(n => n.collisionName != null && n.removedAt.isEmpty),
          inboxDesc(notes.collect {
            case n if n.collisionName != null && n.removedAt.isEmpty && n.createdAt.isDefined =>
              InboxItem("capture-note", n.noteId, s"命名碰撞「${n.collisionName}」待裁决", n.createdAt.get)
          }))
        val question = InboxBucket("question",
          notes.count(n => n.questionId != null && n.answer == null && n.dismissedAt.isEmpty),
          inboxDesc(notes.collect {
            case n if n.questionId != null && n.answer == null && n.dismissedAt.isEmpty &&
              n.raisedAt.isDefined =>
              InboxItem("capture-note", n.noteId, s"疑问：「${snippet(n.content)}」待回答", n.raisedAt.get)
          }))
        val parked = InboxBucket("parked",
          concepts.count(pc => pc.parkedAt.isDefined && pc.removedAt.isEmpty),
          inboxDesc(concepts.collect {
            case pc if pc.parkedAt.isDefined && pc.removedAt.isEmpty =>
              InboxItem("park-concept", pc.conceptId.toString, s"概念「${pc.name}」停放中", pc.parkedAt.get)
          }))
        val patch = InboxBucket("patch",
          patches.count(p => OpenPatchStatuses.contains(p.status)),
          inboxDesc(patches.collect {
            case p if OpenPatchStatuses.contains(p.status) && p.createdAt.isDefined =>
              InboxItem("submit-knowledge-patch", p.patchId,
                s"知识补丁 ${short(p.patchId)} 待处理（${p.status}）", p.createdAt.get)
          }))
        val conflict = InboxBucket("conflict",
          catalogs.count(c => c.conflictId != null && c.resolvedAt.isEmpty),
          inboxDesc(catalogs.collect {
            case c if c.conflictId != null && c.resolvedAt.isEmpty =>
              InboxItem("create-catalog-version", c.catalogId,
                s"目录版本 v${c.version} 冲突 ${c.conflictId} 待解决",
                firstNonNullOrEpoch(c.detectedAt.get, c.assembledAt.get))
          }))
        val all = List(collision, question, parked, patch, conflict)
        val ordered = BucketOrder.map(k => all.find(_.key == k).get).toList
        (ordered.map(_.count).sum, ordered)
                }
              }
            }
          }
        }
      case None => Future.successful((0, List.empty[InboxBucket]))
    }
  }

  def usersBatch(ids: Seq[String]): Future[List[UserRef]] = {
    updateUserProfileRepo.findAll().flatMap(profiles =>
      inviteTenantMemberRepo.findAll().map(roster => (profiles, roster))).map {
      case (profiles, roster) =>
        var names = Map.empty[String, String]
        var emails = Map.empty[String, Option[String]]
        roster.foreach { m =>
          if (m.displayName != null && m.displayName.nonEmpty) names += (m.userId -> m.displayName)
          if (m.email != null && m.email.nonEmpty) emails += (m.userId -> Some(m.email))
        }
        profiles.foreach { p =>
          if (p.displayName != null && p.displayName.nonEmpty) names += (p.userId -> p.displayName)
          if (!emails.contains(p.userId)) {
            if (p.email != null && p.email.nonEmpty) emails += (p.userId -> Some(p.email))
            else emails += (p.userId -> None)
          }
        }
        ids.filter(_ != null).distinct.collect {
          case id if names.contains(id) => UserRef(id, names(id), emails.getOrElse(id, None))
        }.toList
    }
  }

  def me(caller: String, roles: List[String]): Future[MeSnapshot] = {
    val profilesF = updateUserProfileRepo.findAll()
    val tenantsF = onboardTenantRepo.findAll()
    // S0-P0 读侧隔离补漏（2026-09-01）：租户取调用者归属行（与 inbox resolveTenantId 同源），原 headOption 可能报他租户
    resolveTenantId(caller).flatMap { tidOpt =>
      for {
        profiles <- profilesF
        tenants <- tenantsF
      } yield {
        val p = profiles.find(_.userId == caller)
        val t = tidOpt.flatMap(tid => tenants.find(_.tenantId == tid))
        MeSnapshot(caller,
          p.flatMap(x => Option(x.displayName).filter(_.nonEmpty)),
          p.flatMap(x => Option(x.email).filter(_.nonEmpty)),
          t.map(_.tenantId), t.map(_.name), roles, "ACTIVE")
      }
    }
  }
}
