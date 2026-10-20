// impact.aggregate — 聚合影响闭包（M0 模板 1/3）
// 形态：聚合←TARGETS←命令−EMITS→事件−FOLDS_IN→下游聚合
// 证据契约：每行携带完整路径（节点名序列＋边类型序列）；confidence=mechanical
MATCH path = (src:KnConcept {nodeType:'aggregate'})<-[:KN_TARGETS]-(c:KnConcept {nodeType:'command'})-[:KN_EMITS]->(e:KnConcept {nodeType:'event'})-[:KN_FOLDS_IN]->(down:KnConcept {nodeType:'aggregate'})
WHERE src.tenantId = $tenant AND src.projectKey = $project AND src.name = $agg
RETURN src.name AS origin,
       down.name AS impacted,
       reduce(s = '', n IN nodes(path) | s + CASE WHEN s = '' THEN '' ELSE '→' END + coalesce(n.name, n.conceptId)) AS pathNodes,
       reduce(s = '', r IN relationships(path) | s + CASE WHEN s = '' THEN '' ELSE '−' END + type(r)) AS pathRels
ORDER BY impacted
