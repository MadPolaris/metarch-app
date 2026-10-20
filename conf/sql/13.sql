-- 13.sql（余债清偿批·2026-10-07→Phase 4 退役批·2026-10-09 手术）：四新决策视图存量库补表。
-- Phase 4 退役（2026-10-09）：questionlistview / stageprogressdecisionview / fsmdefinitiondecisionview
--   三投影退役（真源删 readModel 便签→管线 DELETE 23 件·零消费者实证 routes 34 路由无一命中），
--   三表已 DROP（.rounds/phase4-drop-manifest-20261009.txt·offset 死账 2 行同批清）。
--   本件只保留 userlookupbyemailorphoneview（活投影·UserTag0 流在役）。
-- 背景：四视图以 ③层 readModel 便签形态存在（不在 story/readmodels.json·2.sql DDL 生成器不覆盖），
--   今晨 rmd 补推建出 RMD 实体 → 投影流启动 → 表不存在 → JdbcProjection 毒丸
--   （Table doesn't exist·RestartWithBackoff 循环——S2-63 停摆家族新例证）。
-- 本件只为已初始化的存量库补表（照 12.sql 惯例·CREATE IF NOT EXISTS 幂等）；
--   列形状＝真仓生成 RepositoryImpl INSERT 契约（Case class 权威）·主键＝实体 Id（ON DUPLICATE 冲突键）。
-- 自由文本列按 S2-75 判例直接 TEXT（VARCHAR(255) 截断＝停投影毒丸·live ALTER 前科）。
-- fresh initdb 顺序：1.sql → 2.sql（已含 33 表）→ … → 13.sql（幂等）。
CREATE TABLE IF NOT EXISTS `userlookupbyemailorphoneview_table` (
  `displayName` VARCHAR(255),
  `email` VARCHAR(255),
  `phoneNumber` VARCHAR(255),
  `status` VARCHAR(255),
  `userId` VARCHAR(255),
  PRIMARY KEY (`userId`)
);
