package net.imadz.modules

import javax.inject.{Inject, Singleton}
import akka.actor.typed.scaladsl.adapter._
import net.imadz.common.application.projection.ProjectionTailWatchdog
import org.slf4j.LoggerFactory
import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment}

/**
 * BUG-012（2026-09-06 在线服务模式切换战役批1-C）：ProjectionTailWatchdog 接线。
 *
 * Watchdog 本体（检测＝realtime capped 头部推进而 offset store 全表零提交；
 * 自愈＝ProjectionManagement.updateOffset 单流重建）早已就绪但 start() 无调用方——
 * ApplicationBootstrap 为 CodegenMain 生成件（整文件覆盖写盘），接线走独立
 * eager singleton 模块（KnInstanceProjectionModule/GraphEventProjectionModule 同范式）。
 */
class ProjectionTailWatchdogModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(bind[ProjectionTailWatchdogRunner].toSelf.eagerly())
}

@Singleton
class ProjectionTailWatchdogRunner @Inject() (classicSystem: akka.actor.ActorSystem) {

  private val log = LoggerFactory.getLogger(getClass)

  // 延迟 22s：ScalikeJdbcSetup（立即）→ ApplicationBootstrap（8s）→ KnInstance（12s）
  // → 图投影（15s）→ 本 watchdog（观察面＝全部投影 offset 健康，晚于所有被监督者启动）
  private val t = new Thread(() => {
    try {
      Thread.sleep(22000)
      ProjectionTailWatchdog.start()(classicSystem.toTyped)
      log.info("PROJECTION-TAIL-WATCHDOG-RUNNER: initialized (BUG-012 supervision armed)")
    } catch {
      case e: Throwable => log.error("PROJECTION-TAIL-WATCHDOG-RUNNER FAILED: {}", e.getMessage, e)
    }
  }, "projection-tail-watchdog-runner")
  t.setDaemon(true)
  t.start()
}
