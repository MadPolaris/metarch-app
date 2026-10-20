// relAudit — 本体公理白名单校验（M0 模板 3/3）
// 公理（白名单·逐条审计）：
//   cmdNoTarget     命令必有落点聚合（KN_TARGETS 出边）——「命令必有 BY」的可执行形态
//   evtNoSource     事件必有触发源（KN_EMITS 入边或 timer/externalSystem 经 TRIGGERS）
//   foldsMisdirected KN_FOLDS_IN 全边必须 event→aggregate（push_l2 写入纪律）
// 输出＝违例清单（bucket＋conceptId）；空集＝公理全过
MATCH (n:KnConcept)
WHERE n.tenantId = $tenant AND n.projectKey = $project
  AND (
    (n.nodeType = 'command' AND NOT EXISTS { MATCH (n)-[:KN_TARGETS]->(:KnConcept) })
    OR (n.nodeType = 'event' AND NOT EXISTS { MATCH (:KnConcept)-[:KN_EMITS]->(n) }
        AND NOT EXISTS { MATCH (:KnConcept {nodeType:'timer'})-[:KN_RELATES {relationType:'TRIGGERS'}]->(n) }
        AND NOT EXISTS { MATCH (:KnConcept {nodeType:'externalSystem'})-[:KN_RELATES {relationType:'TRIGGERS'}]->(n) })
  )
WITH 'concept:' + n.nodeType AS bucket, n.conceptId AS id
RETURN bucket, id
UNION
MATCH (x)-[fin:KN_FOLDS_IN]->(y)
WHERE x.tenantId = $tenant AND x.projectKey = $project
  AND NOT (x.nodeType = 'event' AND y.nodeType = 'aggregate')
RETURN 'foldsMisdirected' AS bucket, coalesce(x.conceptId, '?') + '→' + coalesce(y.conceptId, '?') AS id
ORDER BY bucket, id
