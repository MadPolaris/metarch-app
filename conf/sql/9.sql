-- EP_Catalog_Concept_Write_Model 甲批（2026-09-22·typed concepts 列名收敛）：ModelCatalog 族六视图的
--   `patchSet.knowledgeDelta` 列改名 `concepts`（真源 knowledgeDelta:String → concepts:List<ConceptDelta>；
--   `gen_projection_ddl` 随之改列名·2.sql 已按新名列）。**存量库**因 2.sql 只有 CREATE TABLE IF NOT EXISTS
--   而不会补列 ⇒ 投影 INSERT 撞 `Unknown column 'concepts' in 'field list'` → 该投影静默停摆
--   （2026-09-23 E2E 实测：链驱到 merge 全绿但三个 ModelCatalog 决策视图全崩·ERROR 只在应用日志里）。
-- ⚠️ 与 6/7/8 不同：那三份补的列 2.sql **没有**，本列 2.sql **已有** ⇒ 直接 ALTER 会在 fresh initdb 上撞重复列。
--   故本文件写成**幂等**（information_schema 判在否再 ALTER），fresh/存量都可顺序执行 9。
USE cqrs_jdbc_demo;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='assemblemetadatadecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `assemblemetadatadecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='createcatalogversiondecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `createcatalogversiondecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='exportmetadatadecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `exportmetadatadecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='releasecatalogversiondecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `releasecatalogversiondecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='releasemetadataversiondecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `releasemetadataversiondecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
SET @ddl := IF((SELECT COUNT(*) FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME='resolvecatalogconflictdecisionview_table' AND COLUMN_NAME='concepts')=0,
  'ALTER TABLE `resolvecatalogconflictdecisionview_table` ADD COLUMN `concepts` TEXT NULL', 'SELECT 1');
PREPARE s FROM @ddl; EXECUTE s; DEALLOCATE PREPARE s;
