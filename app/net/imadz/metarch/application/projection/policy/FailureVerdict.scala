package net.imadz.metarch.application.projection.policy

import net.imadz.common.CommonTypes.iMadzError

/** 失败裁决二值（ep-ml-poc3-5 切面·3.6 终形沿用）：分类在 kernel 纯函数 dispose·处置在 glue 运行时。 */
sealed trait FailureVerdict extends Product with Serializable
object FailureVerdict {
  /** 可重试：壳原样 Future.failed → offset 不提交 → akka at-least-once 重试 */
  final case class Retryable(err: iMadzError) extends FailureVerdict
  /** 永久：壳交 Sink 收容（DLQ 留痕）→ offset 推进不堵流（L6 裁决） */
  final case class Permanent(what: String, err: iMadzError) extends FailureVerdict
}
