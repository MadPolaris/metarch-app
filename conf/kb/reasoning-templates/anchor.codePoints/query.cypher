// anchor.codePoints — 知识元素 ↔ 代码落点（EP_GenerationTime_Code_Anchors B5·2026-09-23）
// 双向：noteId 正向（元素 → 其全部落点）／fqn 反向（代码点位 → 知识元素）。
// ★ 两参皆缺 ⇒ **空集**（不返回全图）——防被误当全量导出（白名单模板须有界）。
// 证据契约：每行携带源 label/源 id/落点 FQN/面/维度/文件；confidence=mechanical。
MATCH (s)-[e:KN_ANCHORED_AT]->(cp:KnCodePoint)
WHERE cp.tenantId = $tenant AND cp.projectKey = $project
  AND (($noteId <> '' AND s.noteId = $noteId) OR ($fqn <> '' AND cp.fqn CONTAINS $fqn))
RETURN labels(s)[0] AS sourceLabel,
       s.noteId AS noteId,
       coalesce(s.fieldId, s.voId, s.conceptId) AS sourceId,
       cp.fqn AS fqn,
       cp.surface AS surface,
       e.dimension AS dimension,
       cp.file AS file
ORDER BY surface, fqn
