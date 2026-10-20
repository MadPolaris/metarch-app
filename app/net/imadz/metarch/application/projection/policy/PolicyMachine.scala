package net.imadz.metarch.application.projection.policy

import akka.Done
import akka.projection.eventsourced.EventEnvelope
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/** 策略投影运行时（ep-ml-poc3-6·效果显化终形）：
  * 四段效果管线 × 单一循环——全部 policy 共享，壳不再各自编舞。
  *
  *   Decode : PO.Event => Option[E]                 翻译（机制·本运行时注入的 decode）
  *   Decide : E => List[C]                          决策（纯·kernel 函数）
  *   Execute: C => Future[Either[iMadzError, Unit]] 效果（机制·gateway 函数）
  *   Dispose: (E, iMadzError) => FailureVerdict     失败语义（纯·kernel 函数）
  *
  * 原则：效果显化为签名、不散落在编舞；运行时是唯一的 Future 所有权人；
  * kernel 只出纯函数对 (decide, dispose)——无类、无 port、无 Future。
  */
object PolicyMachine {

  private val log = LoggerFactory.getLogger(PolicyMachine.getClass)

  /** 全部 policy handler 的统一 process 体。
    *
    * @param decode     翻译：PO 事件 → 领域事件（None＝非本策略关注·优雅跳过）
    * @param decide     纯决策：领域事件 → 命令列表（kernel）
    * @param execute    效果：命令 → 回执（glue·sharding ask 等）
    * @param dispose    纯失败语义：回执错误 → 裁决（kernel·Retryable/Permanent）
    * @param bury       收容：DLQ 留痕（glue·Permanent 的机制侧）
    */
  def run[E, C](
    envelope: EventEnvelope[_],
    aggType: String,
    decode: Any => Option[E],
    decide: E => List[C],
    execute: C => Future[Either[net.imadz.common.CommonTypes.iMadzError, Unit]],
    dispose: (E, net.imadz.common.CommonTypes.iMadzError) => FailureVerdict,
    bury: (akka.projection.eventsourced.EventEnvelope[_], String) => Future[Done],
  )(implicit ec: ExecutionContext): Future[Done] =
    decode(envelope.event) match {
      case None => Future.successful(Done) // 非本策略关注事件（BUG-012 教训：优雅跳过）
      case Some(e) =>
        val commands = decide(e)
        Future
          .traverse(commands)(execute)
          .flatMap { _ => Future.successful(Done) }
          .recoverWith {
            case f: net.imadz.common.CommonTypes.iMadzError =>
              dispose(e, f) match {
                case FailureVerdict.Permanent(what, _) =>
                  bury(envelope, s"$aggType·$what")
                case FailureVerdict.Retryable(err) =>
                  Future.failed(new RuntimeException(s"[policy] $aggType 重试失败：err=${err.code}"))
              }
          }
    }
}
