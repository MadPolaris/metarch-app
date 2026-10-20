// agg.relates — 聚合缩并图变长路径（M2 模板 4·批7）
// 形态：聚合 -[:AGG_RELATES*1..3]-> 下游聚合（物化边·kind 序列随路径携带）
// 证据契约：每行携带路径节点序列＋kind 序列＋跳数；confidence=mechanical
// 前置：tools/kn_materialize.mjs 已在最近一次 graph release 后重算（否则结果滞后）
MATCH path = (src:KnConcept {nodeType:'aggregate'})-[rs:AGG_RELATES*1..3]->(down:KnConcept {nodeType:'aggregate'})
WHERE src.tenantId = $tenant AND src.projectKey = $project AND src.name = $agg
  AND down.tenantId = $tenant AND down.projectKey = $project
RETURN src.name AS origin,
       down.name AS impacted,
       reduce(s = '', r IN rs | s + CASE WHEN s = '' THEN '' ELSE ',' END + r.kind) AS kinds,
       reduce(s = '', n IN nodes(path) | s + CASE WHEN s = '' THEN '' ELSE '→' END + coalesce(n.name, n.conceptId)) AS pathNodes,
       length(path) AS hops
ORDER BY hops, impacted
