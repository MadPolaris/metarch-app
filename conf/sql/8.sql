-- S2-89 Phase 1（2026-09-14·洞① knowledgeId 建模）：CaptureNoteDecisionView 知识库概念 id 列——
-- 可空（无知识库关联的便签为 NULL；R3c 替身判定/R4 同名概念判据·EP_S2_89 R1-R6）。
-- 存量库经此 ALTER 补列；fresh initdb 顺序执行 1..8（2.sql 建表无此列·8.sql 补齐——7.sql S2-82-B3 同款先例）。
USE cqrs_jdbc_demo;
ALTER TABLE `capturenotedecisionview_table` ADD COLUMN `knowledgeId` VARCHAR(255) NULL;
