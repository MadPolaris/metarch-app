// field.impact — 字段级影响面（EP_Artifacts_Attribution_Closure P4·2026-09-14）
// 形态：KnField {name:$field} 全部出现 × 面（kind=id/st/pm/pl/vo/rm）
//   ＋ owner 聚合归约：容器为聚合概念（id/st 面）直接归属；否则经 KN_FM_OF（pm/pl 容器＝命令/事件）
//   或 KN_RM_OF（rm 容器＝读模型视图·多源视图多 owner）归约；vo 面容器＝KnValueObject（owner 空）
//   ＋ KN_DERIVES_FROM 双向血缘：derivesFrom＝本字段的上游 payload（writtenBy 派生链）；
//     derivedBy＝以本字段为上游的下游 stateField（改我即波及谁）
// 证据契约：每行携带 fieldId/面/容器/owner 归约集/双向血缘 fieldId 集；confidence=mechanical
// 前置：kn_push_l2 已跑（②层字段树＋③层 rm 面）；空结果＝「图中无此字段名的任何面出现」（负断言）
MATCH (f:KnField {name: $field})-[:KN_FIELD_OF]->(c)
WHERE f.tenantId = $tenant AND f.projectKey = $project
OPTIONAL MATCH (c)-[:KN_FM_OF|KN_RM_OF]->(ag:KnConcept {nodeType: 'aggregate'})
WITH f, c, collect(DISTINCT ag.name) AS viaOwners
WITH f, c,
     CASE WHEN c:KnConcept AND c.nodeType = 'aggregate' THEN [c.name] ELSE viaOwners END AS owners
OPTIONAL MATCH (f)-[:KN_DERIVES_FROM]->(up:KnField)
OPTIONAL MATCH (down:KnField)-[:KN_DERIVES_FROM]->(f)
WITH f, owners, collect(DISTINCT up.fieldId) AS ups, collect(DISTINCT down.fieldId) AS downs
RETURN f.fieldId AS fieldId,
       f.kind AS face,
       f.container AS container,
       reduce(s = '', x IN owners | s + CASE WHEN s = '' THEN '' ELSE ',' END + x) AS owners,
       reduce(s = '', x IN ups | s + CASE WHEN s = '' THEN '' ELSE ',' END + x) AS derivesFrom,
       reduce(s = '', x IN downs | s + CASE WHEN s = '' THEN '' ELSE ',' END + x) AS derivedBy
ORDER BY face, fieldId
