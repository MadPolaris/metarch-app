package net.imadz.modules

import javax.inject.{Inject, Singleton}
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import net.imadz.metarch.infrastructure.bootstrap.{PolicyOnApplyKnowledgePatchBootstrap, PolicyOnConfirmPatchMergedBootstrap, PolicyOnCreateCatalogVersionBootstrap, PolicyOnEstablishConceptBootstrap, PolicyOnRecordConceptLandingsBootstrap}
import org.slf4j.LoggerFactory
import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment}

/**
 * Policy 执行侧装配（EP_Catalog_Concept_Write_Model §3.4·手写领地）。
 *
 * 为何不并入 ApplicationBootstrap：该文件是 CodegenMain 生成件（整文件覆盖写盘），
 * 手改会被下次重生成冲掉；且生成器的 ApplicationBootstrapBuilder 并不产 policy 接线——
 * ApplicationBootstrap 里的 `with PolicyOnCreateCatalogVersionBootstrap` 是历史手改残留，
 * 而 initPolicyOnCreateCatalogVersionRunner 从未被调用（task #12「全仓 0 引用」的真身）。
 * 故照 KnInstanceProjectionModule / KnJournalModule 同款：独立 eager singleton 模块拉起。
 *
 * 四条链（写侧闭环，EP §3.4 图·Option A 两阶段 2026-09-22；④ 为 #4d 补的最后一跳）：
 *   ① ProjectInitiated            → CreateCatalogVersion （立项建目录·PolicyOnCreateCatalogVersion）
 *   ② KnowledgePatchMergeCommitted → ApplyKnowledgePatch （提交目录应用·PolicyOnApplyKnowledgePatch）
 *        ⚠️ Option A 改订：原盯 KnowledgePatchMerged，该事件已改义为「终态回执」——
 *           `MergeKnowledgePatch` 不再自称合并完成，只发 Commit 事件。
 *   ③ KnowledgePatched            → ConfirmPatchMerged   （catalog 应用成功回执·PolicyOnConfirmPatchMerged）
 *   ④ KnowledgePatched            → EstablishConcept      （概念真落 Concept 聚合·PolicyOnEstablishConcept）
 *        ⚠️ ④ 是**扇出**件：一个事件带 List[ConceptDelta]（只取 action=insert）→ N 条命令，各投不同
 *           的 Concept 实体（分片键 = 各自 conceptId）。③④ 同盯 KnowledgePatched 但目标聚合不同：
 *           ③ 回执补丁生命周期，④ 把目录索引里的概念**兑现为真实体**——此前 catalog 登记了号却永远
 *           没有 Concept 聚合（「聚合只发自己事件」铁律下 catalog 编译期发不出 EstablishConcept）。
 *   ⑤ KnowledgePatchMergeCommitted → RecordConceptLandings （概念回锚·PolicyOnRecordConceptLandings·2026-09-26）
 *        ⚠️ ⑤ 与 ④ **分职不可合**：④ 走 insert（建概念·那一刻 AST 还没导出、不可能有落点）；
 *           ⑤ 走 update（回锚·代码已生成，锚＝生成期发现的代码落点）。两者扇出同一 List<ConceptDelta>
 *           但过滤条件互斥（insert vs 带锚的 update）。原「给 EstablishConcept 加 anchors」方案已废。
 *        ⚠️ ⑤ 与 ② 同触发事件（KnowledgePatchMergeCommitted）同 tag 源，各投各的命令，互不干扰。
 *        ⚠️ ①②③④⑤ 皆跨聚合件（五对聚合互不相同）。②③ 合起来构成两阶段：**catalog 是权威，
 *           它说应用成功了 patch 才真 MERGED**。
 *
 * 延迟 12s：让 ScalikeJdbcSetup（立即）与 ApplicationBootstrap（8s·sharding/聚合）先立起；
 * 投影自带 RestartWithBackoff，迟到无害。
 */
class PolicyProjectionModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(bind[PolicyProjectionRunner].toSelf.eagerly())
}

@Singleton
class PolicyProjectionRunner @Inject() (classicSystem: akka.actor.ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)

  private val t = new Thread(() => {
    try {
      Thread.sleep(12000)
      val system = classicSystem.toTyped
      val sharding = ClusterSharding(system)
      // Bootstrap 是 trait（提供 init 方法）——建具名对象混入两者后再调用。
      val bootstraps = new Object
        with PolicyOnCreateCatalogVersionBootstrap
        with PolicyOnApplyKnowledgePatchBootstrap
        with PolicyOnConfirmPatchMergedBootstrap
        with PolicyOnEstablishConceptBootstrap
        with PolicyOnRecordConceptLandingsBootstrap
      bootstraps.initPolicyOnCreateCatalogVersionRunner(system, sharding)
      bootstraps.initPolicyOnApplyKnowledgePatchRunner(system, sharding)
      bootstraps.initPolicyOnConfirmPatchMergedRunner(system, sharding)
      bootstraps.initPolicyOnEstablishConceptRunner(system, sharding)
      bootstraps.initPolicyOnRecordConceptLandingsRunner(system, sharding)
      log.info("POLICY-PROJECTION-RUNNER: initialized (5 policies)")
    } catch {
      case e: Throwable => log.error("POLICY-PROJECTION-RUNNER FAILED: {}", e.getMessage, e)
    }
  }, "policy-projection-runner")
  t.setDaemon(true)
  t.start()
}
