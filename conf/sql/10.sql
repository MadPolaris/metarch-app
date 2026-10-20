-- S1 聚合面批（2026-09-27）：ModelCatalog 族七视图 `concepts` 列 TEXT(64KB) → MEDIUMTEXT(16MB)。
-- 判例：19:46 submit_knowledge_patch_decision_view 在 69,258 字节 concepts 载荷上
--   `Data too long for column 'concepts'` → 投影按恢复策略停摆（Note 系投影连锁冻结，
--   contract_note_projection 契测「投影行不可见」红）。同族判例＝server-concepts-column-64kb
--   （64KB 毒流家族·列扩容为根治档；按字节分批为另一档——本批取列扩容）。
-- 幂等：DATA_TYPE 已为 mediumtext 则跳过（CONDITIONAL ALTER 同 9.sql 形态）。
-- @generated-by 手术批（写侧 GetState 范式前置修复）
DELIMITER $$

DROP PROCEDURE IF EXISTS upgrade_concepts_mediumtext$$
CREATE PROCEDURE upgrade_concepts_mediumtext()
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE tbl VARCHAR(128);
  DECLARE tables_list TEXT DEFAULT 'assemblemetadatadecisionview_table,createcatalogversiondecisionview_table,exportmetadatadecisionview_table,releasecatalogversiondecisionview_table,releasemetadataversiondecisionview_table,resolvecatalogconflictdecisionview_table,submitknowledgepatchdecisionview_table';

  WHILE i < 7 DO
    SET tbl = SUBSTRING_INDEX(SUBSTRING_INDEX(tables_list, ',', i + 1), ',', -1);
    IF (SELECT COUNT(*) FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA='cqrs_jdbc_demo' AND TABLE_NAME=tbl AND COLUMN_NAME='concepts' AND DATA_TYPE='text') > 0 THEN
      SET @sql = CONCAT('ALTER TABLE `', tbl, '` MODIFY COLUMN `concepts` MEDIUMTEXT NULL');
      PREPARE st FROM @sql; EXECUTE st; DEALLOCATE PREPARE st;
      SELECT CONCAT('upgraded: ', tbl) AS log_line;
    END IF;
    SET i = i + 1;
  END WHILE;
END$$

DELIMITER ;

CALL upgrade_concepts_mediumtext();
DROP PROCEDURE IF EXISTS upgrade_concepts_mediumtext;
