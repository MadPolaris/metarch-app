// process.family — 过程族跨域问答（D6 四面锚定·2026-09-19）
// 两侧证据：
//   ① 行为面：engplane-lab 作用域里 processFamily 锚到该族的 EngplaneObject
//      （属性直取：publish.mjs 已把 processFamily 写为节点属性·不靠 JSON 串匹配）。
//   ② 执行面：EngplaneJournalFamily 节点（journal_index.mjs 汇入·按族聚合的计数与 kind 直方图）。
// 负断言纪律：结果为空＝「本图中无该族锚定路径」，不得升级为「系统中不存在」。
// 参数：$family（九族键名·必填）。
MATCH (o:EngplaneObject {tenantId:'engplane-lab', projectKey:'engplane-lab'})
WHERE o.processFamily = $family
RETURN 'behavior' AS side, o.id AS anchorId, o.processFamily AS family, o.j AS detail
UNION
MATCH (f:EngplaneJournalFamily {tenantId:'engplane-lab', projectKey:'engplane-lab'})
WHERE f.family = $family
RETURN 'execution' AS side, f.id AS anchorId, f.family AS family, f.j AS detail
