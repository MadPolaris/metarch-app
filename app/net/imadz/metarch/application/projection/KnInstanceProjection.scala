package net.imadz.metarch.application.projection

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.contrib.persistence.mongodb.MongoReadJournal
import akka.persistence.query.Offset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import net.imadz.common.application.projection.ScalikeJdbcSession
import net.imadz.metarch.application.aggregates._
import net.imadz.metarch.application.projection.handlers.KnInstanceProjectionHandler
import net.imadz.metarch.application.projection.repository.KnInstanceStore
import net.imadz.metarch.infrastructure.proto.concept.ConceptEventPO
import net.imadz.metarch.infrastructure.proto.context.ContextEventPO
import net.imadz.metarch.infrastructure.proto.context_relation.ContextRelationEventPO
import net.imadz.metarch.infrastructure.proto.do_r.DoREventPO
import net.imadz.metarch.infrastructure.proto.fsm_definition.FsmDefinitionEventPO
import net.imadz.metarch.infrastructure.proto.knowledge_patch.KnowledgePatchEventPO
import net.imadz.metarch.infrastructure.proto.model_catalog.ModelCatalogEventPO
import net.imadz.metarch.infrastructure.proto.note.NoteEventPO
import net.imadz.metarch.infrastructure.proto.organization_member.OrganizationMemberEventPO
import net.imadz.metarch.infrastructure.proto.project.ProjectEventPO
import net.imadz.metarch.infrastructure.proto.question.QuestionEventPO
import net.imadz.metarch.infrastructure.proto.relation.RelationEventPO
import net.imadz.metarch.infrastructure.proto.session.SessionEventPO
import net.imadz.metarch.infrastructure.proto.stage.StageEventPO
import net.imadz.metarch.infrastructure.proto.subdomain.SubdomainEventPO
import net.imadz.metarch.infrastructure.proto.tenant.TenantEventPO
import net.imadz.metarch.infrastructure.proto.work_ticket.WorkTicketEventPO
import net.imadz.metarch.infrastructure.proto.workgroup.WorkgroupEventPO
import net.imadz.metarch.infrastructure.proto.workshop.WorkshopEventPO
import scalapb.GeneratedOneof

/**
 * R1 实例层投影（批4·手写领地·声明见 KnInstanceStore 头注）。
 *
 * 与既有读模型投影同基建：每聚合 tag 族一个 JdbcProjection.exactlyOnce，
 * handler 泛化（零逐事件样板），offset 与业务写同事务。
 *
 * 并发面（kn_kb_storage_plan §一.2 v3.1 并发评估）：每事件 fanout ＝ 该聚合族既有视图数＋1
 * （本投影）；register-user 场景 3 视图＋1＝4 并发投影（低于文档预估的 6+——三表合一单投影
 * 组合，而非三独立投影）。READ-COMMITTED 已全局生效；死锁面复测随 live 拉起演练。
 */
object KnInstanceProjection {

  val projectionName = "kn_instance_projection"

  def createProjection[E <: GeneratedOneof](
      system: ActorSystem[_],
      index: Int,
      family: KnInstanceFamily[E]
  ): ExactlyOnceProjection[Offset, EventEnvelope[E]] = {
    implicit val sys: ActorSystem[_] = system
    val tag = s"${family.tagPrefix}$index" // 与聚合 tagger 同式：XxxTagN

    JdbcProjection.exactlyOnce(
      projectionId = ProjectionId(projectionName, tag),
      sourceProvider = EventSourcedProvider.eventsByTag[E](
        system = system,
        readJournalPluginId = MongoReadJournal.Identifier,
        tag = tag
      ),
      handler = () => new KnInstanceProjectionHandler(family, new KnInstanceStore),
      sessionFactory = () => new ScalikeJdbcSession()
    )
  }

  /** 逐族拉起 ShardedDaemonProcess（name 带 tagPrefix 保唯一——19 族各自独立 daemon 集）。 */
  def initFamilyProjection[E <: GeneratedOneof](system: ActorSystem[_], family: KnInstanceFamily[E]): Unit = {
    ShardedDaemonProcess(system).init(
      name = s"kn-instance-${family.tagPrefix}",
      numberOfInstances = family.tags.size,
      behaviorFactory = index => ProjectionBehavior(createProjection(system, index, family)),
      settings = ShardedDaemonProcessSettings(system),
      stopMessage = Some(ProjectionBehavior.Stop)
    )
  }
}

/** 一个聚合族的实例投影装配参数（tag 与聚合 XxxAggregate.tags 同源）。 */
final case class KnInstanceFamily[E <: GeneratedOneof](
    tagPrefix: String,
    tags: Vector[String]
)

/** 19 聚合族全表（tag 与聚合 bootstrap 同源——XxxAggregate.tags；事件类型由 E 携带）。
 * User 不入实例面（用户裁决 2026-09-06「user 在 graph projection 上为什么有坑位？不需要吧」）——
 * 用户/租户属身份基建而非租户本体知识，图面（GraphEventProjector 8 源）本就不含 User。 */
object KnInstanceFamilies {

  val all: List[KnInstanceFamily[_ <: GeneratedOneof]] = List(
    KnInstanceFamily[ConceptEventPO.Event]("ConceptTag", ConceptAggregate.tags),
    KnInstanceFamily[ContextEventPO.Event]("ContextTag", ContextAggregate.tags),
    KnInstanceFamily[ContextRelationEventPO.Event]("ContextRelationTag", ContextRelationAggregate.tags),
    KnInstanceFamily[DoREventPO.Event]("DoRTag", DoRAggregate.tags),
    KnInstanceFamily[FsmDefinitionEventPO.Event]("FsmDefinitionTag", FsmDefinitionAggregate.tags),
    KnInstanceFamily[KnowledgePatchEventPO.Event]("KnowledgePatchTag", KnowledgePatchAggregate.tags),
    KnInstanceFamily[ModelCatalogEventPO.Event]("ModelCatalogTag", ModelCatalogAggregate.tags),
    KnInstanceFamily[NoteEventPO.Event]("NoteTag", NoteAggregate.tags),
    KnInstanceFamily[OrganizationMemberEventPO.Event]("OrganizationMemberTag", OrganizationMemberAggregate.tags),
    KnInstanceFamily[ProjectEventPO.Event]("ProjectTag", ProjectAggregate.tags),
    KnInstanceFamily[QuestionEventPO.Event]("QuestionTag", QuestionAggregate.tags),
    KnInstanceFamily[RelationEventPO.Event]("RelationTag", RelationAggregate.tags),
    KnInstanceFamily[SessionEventPO.Event]("SessionTag", SessionAggregate.tags),
    KnInstanceFamily[StageEventPO.Event]("StageTag", StageAggregate.tags),
    KnInstanceFamily[SubdomainEventPO.Event]("SubdomainTag", SubdomainAggregate.tags),
    KnInstanceFamily[TenantEventPO.Event]("TenantTag", TenantAggregate.tags),
    KnInstanceFamily[WorkTicketEventPO.Event]("WorkTicketTag", WorkTicketAggregate.tags),
    KnInstanceFamily[WorkgroupEventPO.Event]("WorkgroupTag", WorkgroupAggregate.tags),
    KnInstanceFamily[WorkshopEventPO.Event]("WorkshopTag", WorkshopAggregate.tags)
  )
}
