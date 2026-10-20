-- 立项A（2026-09-28·inv 规则面 RuleRef 三槽）：FMD 视图聚合级规则引用列——
-- FieldModelDeclared.aggregateRuleRefs（40 条 AGG 类·剥 GEN 后）投影列。命令/事件级两槽
-- （preconditionRefs/postconditionRefs）不占列——commands/events 整 spec JSON 列自动携带。
-- 存量库经此 ALTER 补列；fresh initdb 顺序执行 1..11（2.sql 建表无此列·11.sql 补齐）。
USE cqrs_jdbc_demo;
ALTER TABLE `fieldmodeldefinitiondecisionview_table` ADD COLUMN `aggregateRuleRefs` MEDIUMTEXT NULL;
