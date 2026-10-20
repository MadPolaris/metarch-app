// MANUAL (S2-82-B4b · 2026-09-11 · s282 §11)：真源 canonical JSON 工具——协议冻结于 B4-A W1
// （tools/truth_recon.mjs canonicalize：UTF-8·key 字典序·无空白；数组保序）。
// 投影切源（contentSource="projection"）后 /api/truth/:key 的字节与指纹均出自本工具；
// 改动协议＝改真仓与本仓两侧＋s282 批记录，禁止单侧漂移。
package net.imadz.metarch.truth

import play.api.libs.json._

import java.security.MessageDigest

/** canonical JSON（协议冻结·与 tools/truth_recon.mjs L33-41 镜像）。
  *
  * 规约：
  *   - 递归 key 字典序（String 自然序＝JS sort() 对 ASCII 键同口径）
  *   - compact 序列化（无空白·非 ASCII 原样输出——Json.stringify 与 JS JSON.stringify 同口径）
  *   - 数组保序（序是内容的一部分——rebuilder 与文件规范化脚本 b4b_normalize_truth.mjs 镜像同一套排序规则）
  *   - 数字经 BigDecimal 精确往返（真源唯一浮点 51.736——TruthCanonicalJsonSpec 钉死）
  */
object TruthCanonicalJson {

  /** 递归重排 JsObject 键为字典序；JsArray/叶子原样。 */
  def canonicalize(v: JsValue): JsValue = v match {
    case JsObject(fields) => JsObject(fields.toSeq.sortBy(_._1)(Ordering.String).map { case (k, vv) => k -> canonicalize(vv) })
    case JsArray(items)   => JsArray(items.map(canonicalize))
    case other            => other
  }

  /** canonical compact 串（UTF-8 字节内容与 JS canonicalJson() 全等）。 */
  def canonicalString(v: JsValue): String = Json.stringify(canonicalize(v))

  def canonicalBytes(v: JsValue): Array[Byte] = canonicalString(v).getBytes("UTF-8")

  def sha256Hex(bytes: Array[Byte]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(bytes)
    md.digest.map(b => String.format("%02x", java.lang.Byte.valueOf(b))).mkString
  }

  /** falsy-elide 构造帮手（truth_recon L94-96 规约镜像）：剔除 null / false / "" / 空数组键
    * ——文件序列化惯例「显式 falsy ⇔ 键缺省」在两侧等价。
    * 语义位例外（JsNull 必须保留）：statusTransitions.*（dict 值 null＝事件触发无目标态）与
    * stateGates.*.toState——由调用点绕开本帮手显式构造，勿经 obj()。
    */
  def obj(fields: (String, JsValue)*): JsObject = JsObject(fields.filterNot {
    case (_, JsNull)            => true
    case (_, JsBoolean(false))  => true
    case (_, JsString(s))       => s.isEmpty
    case (_, JsArray(items))    => items.isEmpty
    case _                      => false
  })

  /** 递归 falsy-elide（B4c·MetarchRebuild 排序键用）：obj() 只 elide 顶层——info 内层键
    * （isKey:false/pivotal:false 等）在排序键中也须规约，否则与 b4c_normalize 的
    * sortKeyOf（JS elided() 递归）不对称 → 便签序错位。数组元素保序不剔除。
    */
  def elideDeep(v: JsValue): JsValue = v match {
    case JsObject(fields) => JsObject(fields.toSeq.flatMap {
      case (k, JsNull)           => None
      case (k, JsBoolean(false)) => None
      case (k, JsString(s))      if s.isEmpty => None
      case (k, JsArray(items))   if items.isEmpty => None
      case (k, vv)               => Some(k -> elideDeep(vv))
    })
    case JsArray(items) => JsArray(items.map(elideDeep))
    case other          => other
  }
}
