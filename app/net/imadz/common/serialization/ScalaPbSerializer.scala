package net.imadz.common.serialization

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import scalapb.{GeneratedMessage, GeneratedOneof}

/** ScalaPB 事件的 **protobuf 序列化器**（2026-09-23·#14 根因根治 ＋ 变体身份去类名）。
  *
  * 为何存在：事件原走 Java 序列化 ⇒ 不可 schema 演进（Scala `List` 经 `DefaultSerializationProxy`
  * 落盘，类一加字段旧事件即不可读·#14 实证）。原绑定 `"scalapb.GeneratedMessage" = proto`
  * **漏了 oneof 用例类**（事件类是 `GeneratedOneof`，与之**兄弟而非父子**）；而**内建 proto 也用不了**——
  * 它反射调 `toByteArray()`，oneof 用例类没这个方法（实测 `NoSuchMethodException: …toByteArray()`·
  * 写被拒 `WriteMessageRejected`）。
  *
  * ## 两条轴分开（本类的核心设计）
  * - **字段级演进**：protobuf 原生（未知字段保留）——加字段安全。
  * - **变体身份**：**编进字节**（序列化**父消息**，变体＝其中的 oneof 字段号），而**不是**骑在 JVM 类名上。
  *   这是 proto 的原生契约（**契约是字段号不是名字**）⇒ **变体改名不影响读**。
  *   manifest 因此记**父消息类**（`UserEventPO`）而非用例类（`UserEventPO$Event$UserRegistered`）。
  *   Akka 官方对该场景的推荐件即 `SerializerWithStringManifest`（manifest 与类名解耦）。
  *
  * ## 无 Java 回退（`allow-java-serialization = off` 的硬约束）
  * Akka 在 off 下**拒绝构造 `JavaSerializer`**（`JavaSerializationException`）⇒ 本类**不提供** Java 垫片。
  * 存量 Java 事件已随清空重灌退场；旧格式（本类前一版：内层消息字节＋用例类名 manifest）仍走 legacy 分支。
  */
class ScalaPbSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

  /** 未占用（1=Java·2=Protobuf·3=DisabledJava·1234/1235=saga·20160728=object-id-offset）。 */
  override def identifier: Int = 1236

  /** **变体身份去类名**：oneof 用例类 → 记**父消息**类名（变体改名不动它）。 */
  override def manifest(o: AnyRef): String = o match {
    case g: GeneratedOneof   => parentOf(g.getClass).getName
    case m: GeneratedMessage => m.getClass.getName
    case other               => throw new IllegalArgumentException(
      s"ScalaPbSerializer 只处理 ScalaPB 消息：${other.getClass.getName}")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    // 用例类自身无字节面 → 装进父消息再序列化（变体＝父消息里的 oneof 字段号）
    case g: GeneratedOneof   => parentWith(g).toByteArray
    case m: GeneratedMessage => m.toByteArray
    case other               => throw new IllegalArgumentException(
      s"ScalaPbSerializer 只处理 ScalaPB 消息：${other.getClass.getName}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val cls = Class.forName(manifest)
    // legacy：manifest 是用例类（本类前一版格式＝内层消息字节）
    if (classOf[GeneratedOneof].isAssignableFrom(cls)) wrap(cls, parse(valueType(cls), bytes))
    else if (classOf[GeneratedMessage].isAssignableFrom(cls)) {
      // 新格式：manifest 是消息类；若它带 oneof（父消息）→ 取出变体用例类
      val msg = parse(cls, bytes)
      oneofAccessor(cls).map(_.invoke(msg)).getOrElse(msg)
    } else throw new IllegalArgumentException(s"非 ScalaPB 类：${cls.getName}")
  }

  // ── 反射助手（ScalaPB 生成物形状固定·不靠名字字符串解析）──────────────────
  // 用例类嵌套形状：<Parent>$<OneofTrait>$<Case>
  // ⚠️ 实测：用例类.getEnclosingClass 给的是 **trait 的伴生对象**（名字带尾 `$`），
  //   再取一层才是父消息 ⇒ parentOf = getEnclosingClass×2。

  private def parentOf(caseCls: Class[_]): Class[_] =
    caseCls.getEnclosingClass match {
      case null => throw new IllegalArgumentException(s"用例类无外层：${caseCls.getName}")
      case t    => Option(t.getEnclosingClass).getOrElse(
        throw new IllegalArgumentException(s"oneof 外壳无外层：${t.getName}"))
    }

  private def valueType(cls: Class[_]): Class[_] =
    cls.getDeclaredFields.find(_.getName == "value").map(_.getType).getOrElse(
      throw new IllegalArgumentException(s"oneof 类无 value 字段：${cls.getName}"))

  private def companion(cls: Class[_]): AnyRef =
    Class.forName(cls.getName + "$").getField("MODULE$").get(null)

  private def parse(cls: Class[_], bytes: Array[Byte]): AnyRef = {
    val c = companion(cls)
    c.getClass.getMethod("parseFrom", classOf[Array[Byte]]).invoke(c, bytes)
  }

  private def wrap(cls: Class[_], inner: AnyRef): AnyRef = {
    val c = companion(cls)
    val innerCls = valueType(cls)
    val apply = c.getClass.getMethods.find(m =>
      m.getName == "apply" && m.getParameterCount == 1 &&
        m.getParameterTypes()(0).isAssignableFrom(innerCls)).getOrElse(
      throw new IllegalArgumentException(s"oneof 伴生无可用 apply(1)：${cls.getName}"))
    apply.invoke(c, inner)
  }

  /** 把用例类实例装进父消息（`with<Oneof>(case)`）——序列化父消息用。
    * 设值口按「参数类型能收下本用例类」匹配（trait 通常是包它的那个 sealed trait）。
    */
  private def parentWith(g: GeneratedOneof): GeneratedMessage = {
    val parent = parentOf(g.getClass)
    val c = companion(parent)
    val dflt = c.getClass.getMethod("defaultInstance").invoke(c)
    val setter = dflt.getClass.getMethods.find(m =>
      m.getName.startsWith("with") && m.getParameterCount == 1 &&
        m.getParameterTypes()(0).isAssignableFrom(g.getClass)).getOrElse(
      throw new IllegalArgumentException(s"父消息无 with<oneof> 设值口：${parent.getName}"))
    setter.invoke(dflt, g).asInstanceOf[GeneratedMessage]
  }

  /** 父消息的 oneof 取值口：无参、返回类型是 `GeneratedOneof` 子类的那个方法（如 `event()`）。
    * ⚠️ 实测排除项：Scala 的**默认参数取值器**（`apply$default$1`／`copy$default$1`／
    * `$lessinit$greater$default$1`）同样无参且返回 `Event`，但给的是 `Event.Empty`——
    * 它们在 `getMethods` 里排在真取值口之前，不排除就会解出 Empty（本类踩过）。
    */
  private def oneofAccessor(cls: Class[_]): Option[java.lang.reflect.Method] =
    cls.getMethods.find(m => m.getParameterCount == 0 && !m.getName.contains("$") &&
      classOf[GeneratedOneof].isAssignableFrom(m.getReturnType))
}
