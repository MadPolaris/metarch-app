// chain.fromAggregate — 策略编排因果链（M0 模板 2/3）
// 形态：触发事件−TRIGGERS→策略−EXECUTES→命令−TARGETS→目标聚合（＋命令 EMITS 事件）
// 证据契约：核心链路径（节点名序列＋边类型序列）＋发射事件列；confidence=mechanical
MATCH path = (ev:KnConcept {nodeType:'event'})-[:KN_RELATES {relationType:'TRIGGERS'}]->(p:KnConcept {nodeType:'policy'})-[:KN_RELATES {relationType:'EXECUTES'}]->(c:KnConcept {nodeType:'command'})-[:KN_TARGETS]->(src:KnConcept {nodeType:'aggregate'})
WHERE src.tenantId = $tenant AND src.projectKey = $project AND src.name = $agg
OPTIONAL MATCH (c)-[:KN_EMITS]->(e2:KnConcept {nodeType:'event'})
RETURN ev.name AS triggerEvent,
       p.name AS policy,
       c.name AS command,
       src.name AS aggregate,
       e2.name AS emittedEvent,
       reduce(s = '', n IN nodes(path) | s + CASE WHEN s = '' THEN '' ELSE '→' END + coalesce(n.name, n.conceptId)) AS pathNodes,
       reduce(s = '', r IN relationships(path) | s + CASE WHEN s = '' THEN '' ELSE '−' END + type(r)) AS pathRels
ORDER BY triggerEvent, emittedEvent
