package net.imadz.modules

import javax.inject.{Inject, Provider, Singleton}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import net.imadz.common.auth.{AuthConfiguration, ConfigurationLike}
import net.imadz.metarch.infrastructure.bootstrap.ApplicationBootstrap
import play.api.{Configuration, Environment, Logger}
import play.api.inject.{Binding, Module}

/**
 * BootstrapModule锛坰caffold 灞傦紝2026-08-24锛夛細寤惰繜鎷夎捣 ApplicationBootstrap銆? * eager singleton 浼氬湪 classicSystem 瀹屾垚 extensions 娉ㄥ唽鍓嶅疄渚嬪寲锛圫erializationExtension 鎶? * non-registered锛夆€斺€旀敼涓虹函 Java 瀹堟姢绾跨▼寤惰繜 8s 鎵ц锛堜笉渚濊禆 akka dispatcher锛夈€? */
class BootstrapModule(environment: Environment, configuration: Configuration) extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(
      bind[ConfigurationLike].to[AuthConfiguration],
      // 债④ port 绑定（部署窗 2026-10-07）：语义批 ef801f75 起生成件 ApplicationBootstrap 构造器收
      // port trait——缺绑定＝启动 Guice CreationException（部署窗实证）。实现＝functions.yaml
      // ports.bridge 薄桥 object（interfaces→structure 合法方向·di-poc BootstrapModule 同款先例）。
      bind[net.imadz.metarch.domain.service.ConceptDeltaAdjudicator].toInstance(net.imadz.metarch.domain.service.ConceptDeltaAdjudicatorBridge),
      bind[net.imadz.metarch.domain.service.ContentFingerprintPort].toInstance(net.imadz.metarch.domain.service.ContentFingerprintBridge),
      bind[ApplicationBootstrapRunner].toSelf.eagerly()
    )
}

@Singleton
class ApplicationBootstrapRunner @Inject()(
    classicSystem: akka.actor.ActorSystem,
    bootstrapProvider: Provider[ApplicationBootstrap]
) {
  private val log = Logger(getClass)

  // 读侧连接池必须立即初始化（default 池被 ViewApiController/投影仓库使用），
  // 延迟 8s 会让启动初期的请求撞上 "Connection pool is not yet initialized"。
  // SerializationExtension 时序约束仅适用于 ApplicationBootstrap 内的聚合初始化，与本初始化无关。
  net.imadz.common.application.projection.ScalikeJdbcSetup.init(classicSystem.toTyped)

  val t = new Thread(() => {
    try {
      Thread.sleep(8000)
      bootstrapProvider.get()
      System.out.println("BOOTSTRAP-RUNNER: initializing now")
    } catch {
      case t: Throwable => System.err.println("BOOTSTRAP-RUNNER FAILED: " + t)
    }
  }, "application-bootstrap-runner")
  t.setDaemon(true)
  t.start()
}

