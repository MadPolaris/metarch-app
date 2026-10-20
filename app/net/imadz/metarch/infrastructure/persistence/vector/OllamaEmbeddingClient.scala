package net.imadz.metarch.infrastructure.persistence.vector

import java.net.URI
import java.net.http.{ HttpClient, HttpRequest, HttpResponse }
import java.time.Duration
import javax.inject.{ Inject, Singleton }
import play.api.Configuration
import play.api.libs.json.Json

/**
 * OllamaEmbeddingClient（M3 向量读模型批E·2026-09-07）：bge-m3 嵌入手写基建——
 * HTTP/超时/维度校验全在此，生成侧（投影 upsert 前置嵌入＋检索查询侧嵌入）只机械调用 embed。
 *
 * 投影内同步嵌入（M3 拍板）：写入链＝handler → Impl.upsert → embed(view.embedText) → PgVectorStore.upsert；
 * 嵌入失败＝异常上抛 → 投影 backoff·offset 不前移·零丢失（与图投影停车场同语义）。
 * 查询链＝VectorSearchApiController → Future { embed(query) } → search（阻塞 IO 隔离在 Future）。
 *
 * transport 可桩：doPost 为 protected 单点——Spec 子类覆写即可离线测维度校验/解析逻辑。
 *
 * ⚠️ 手写领地（writeTerritorial 之外）：生成器不覆写本文件。
 */
@Singleton
class OllamaEmbeddingClient @Inject() (config: Configuration) {

  private val embConf = config.get[Configuration]("vector.embedding")
  private val endpoint = embConf.get[String]("endpoint")
  private val model = embConf.get[String]("model")
  private val dims = embConf.get[Int]("dims")
  // Play 2.8 无 ConfigLoader[java.time.Duration]——走 underlying getDuration（"30s" 直解）
  private val timeout = embConf.underlying.getDuration("timeout")

  private lazy val http: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .build()

  /** 同步嵌入单文本：POST {model, input:[text]} → embeddings[0]（float[1024]）。
    * 维度校验：与配置 dims 不符即抛——哑入库（维度错位的向量）比报错危险。 */
  def embed(text: String): Array[Float] = {
    val payload = Json.obj("model" -> model, "input" -> Json.arr(text)).toString()
    val json = Json.parse(doPost(payload))
    (json \ "embeddings").asOpt[Seq[Seq[Float]]].flatMap(_.headOption) match {
      case Some(vec) =>
        if (vec.size != dims)
          throw new IllegalStateException(
            s"嵌入维度 ${vec.size} ≠ 配置 $dims（模型 $model 与 vector.embedding.dims 不一致）")
        vec.toArray
      case None =>
        throw new IllegalStateException(s"ollama 响应缺 embeddings：${json.toString().take(200)}")
    }
  }

  /** transport 单点（protected 可桩）：POST endpoint·200 断言·超时由 request 保证 */
  protected def doPost(payload: String): String = {
    val req = HttpRequest.newBuilder()
      .uri(URI.create(endpoint))
      .timeout(timeout)
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(payload))
      .build()
    val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
    if (resp.statusCode() != 200)
      throw new IllegalStateException(s"ollama ${resp.statusCode()}: ${resp.body().take(200)}")
    resp.body()
  }

  /** 桩/探测用：配置维度（维度校验常量透出） */
  def configuredDims: Int = dims
}
