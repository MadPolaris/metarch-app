package net.imadz.metarch.application.projection.policy

import akka.Done
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.concurrent.Future

/** DLQ 收容机制（ep-ml-poc3-7：五处手抄 dlqAndSwallow 的唯一实现·全 policy 共享）。
  * L6 语义：落 kn_projection_dlq 留痕后吞掉——offset 推进不堵流；幂等 event_id 主键；双落空降级日志。 */
object DlqBury {

  private val log = LoggerFactory.getLogger(DlqBury.getClass)

  def apply(envelope: akka.projection.eventsourced.EventEnvelope[_], aggType: String, what: String): Future[Done] = {
    val eventId = s"${envelope.persistenceId}|${envelope.sequenceNr}"
    log.warn(s"[policy] 投影毒丸转 DLQ（$aggType·$what·offset 推进不堵流）: $eventId")
    try {
      NamedDB('default).localTx { implicit s =>
        sql"""INSERT IGNORE INTO kn_projection_dlq
              (event_id, agg_type, event_type, persistence_id, sequence_nr, error, failed_at_millis)
              VALUES (${eventId}, ${aggType}, 'PolicyFailure',
                ${envelope.persistenceId}, ${envelope.sequenceNr}, ${what.take(1000)}, ${System.currentTimeMillis()})""".update.apply()
      }
    } catch { case e: Throwable => log.warn(s"DLQ 收容失败（$eventId 双落空，仅日志留痕）: ${e.getMessage}") }
    Future.successful(Done)
  }
}
