package net.imadz.metarch.domain.entities

import net.imadz.common.CommonTypes.Id

/** S2-82-B2 wave2：orgMemberId 派生助手的手写恒存位（原挂 OrganizationMemberEntity object——
  * 生成物无 MANUAL 块，S2-81 批D 手加助手在再生成时被丢弃，6 处消费方全红）。
  * 本文件无 CodegenMain 指纹 → gen_manifest 恒 KEEP（--prune 亦不删），再生成永不动摇。
  * S2-81 语义原样迁入（80c7009）：
  *  - orgMemberIdOf：canonical 派生（与 OrganizationMemberApiController/Bootstrap 的分片 entityId 公式一致）。
  *    供 converters 回放 legacy journal 时的空容忍真值派生，与投影写点名册补算。
  *  - isMemberOf：名册成员判定（读侧消费方统一口径）——orgMemberIds 含 orgMemberIdOf(tenantId, userId)。
  *    tenantId/userId 非法 UUID（测试桩/历史行）→ false（Try 容错，不抛）。 */
object OrganizationMemberIds {

  /** S2-81: orgMemberId canonical derivation (consistent with the sharding entityId formula in OrganizationMemberApiController/Bootstrap). */
  def orgMemberIdOf(tenantId: Id, userId: Id): Id =
    net.imadz.common.Id.of(java.util.UUID.nameUUIDFromBytes((tenantId.toString + ":" + userId.toString).getBytes("UTF-8")).toString)

  /** S2-81: 名册成员判定（读侧消费方统一口径）——orgMemberIds 含 orgMemberIdOf(tenantId, userId)。 */
  def isMemberOf(tenantId: String, userId: String, orgMemberIds: List[Id]): Boolean =
    scala.util.Try(orgMemberIdOf(net.imadz.common.Id.of(tenantId), net.imadz.common.Id.of(userId))).toOption
      .exists(om => orgMemberIds != null && orgMemberIds.map(_.toString).contains(om.toString))
}
