package net.imadz.modules

import javax.inject.{Inject, Singleton}
import net.imadz.metarch.application.kb.{KnJournalService, KnJournalStore}
import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment}

import scala.concurrent.ExecutionContext

/**
 * M1 审计面装配（批6·手写领地·声明见 KnJournalStore 头注）。
 *
 * eager 绑定：启动即 ensureIndexes（TTL 180d＋查询序·幂等·失败不阻断启动——降级纪律）。
 * 不走 ApplicationBootstrap（生成件整文件覆盖写盘·批4 同款裁决）。
 */
class KnJournalModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] =
    Seq(bind[KnJournalIndexRunner].toSelf.eagerly())
}

@Singleton
class KnJournalIndexRunner @Inject() (store: KnJournalStore)(implicit ec: ExecutionContext) {
  store.ensureIndexes()
}
