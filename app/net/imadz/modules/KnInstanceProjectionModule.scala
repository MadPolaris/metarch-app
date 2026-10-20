package net.imadz.modules

import javax.inject.{Inject, Singleton}
import akka.actor.typed.scaladsl.adapter._
import net.imadz.metarch.application.projection.{KnInstanceFamilies, KnInstanceProjection}
import net.imadz.metarch.application.projection.repository.KnInstanceStore
import org.slf4j.LoggerFactory
import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment}

/**
 * R1 实例层投影装配（批4·手写领地·声明见 KnInstanceStore 头注）。
 *
 * 为何不走 ApplicationBootstrap：该文件是 CodegenMain 生成件（整文件覆盖写盘），
 * 手改会被下次重生成冲掉（routes 残卷/MANUAL 块被冲同族教训·EP_S2_48 批1）——
 * 故以独立 eager singleton 模块拉起（ApplicationBootstrapRunner 同范式）。
 */
class KnInstanceProjectionModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(bind[KnInstanceProjectionRunner].toSelf.eagerly())
}

@Singleton
class KnInstanceProjectionRunner @Inject() (classicSystem: akka.actor.ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)

  // 延迟 12s：让 ScalikeJdbcSetup（立即）与 ApplicationBootstrap（8s·sharding/聚合）先立起；
  // 投影自带 RestartWithBackoff，迟到无害
  private val t = new Thread(() => {
    try {
      Thread.sleep(12000)
      val system = classicSystem.toTyped
      new KnInstanceStore().ensureTables()
      KnInstanceFamilies.all.foreach(family => KnInstanceProjection.initFamilyProjection(system, family))
      log.info("KN-INSTANCE-PROJECTION-RUNNER: initialized ({} families)", Int.box(KnInstanceFamilies.all.size))
    } catch {
      case e: Throwable => log.error("KN-INSTANCE-PROJECTION-RUNNER FAILED: {}", e.getMessage, e)
    }
  }, "kn-instance-projection-runner")
  t.setDaemon(true)
  t.start()
}
