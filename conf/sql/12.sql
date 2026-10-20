-- 12.sql（立项B·2026-09-29）：usecasedefinitiondecisionview_table 存量库补表
-- fresh initdb 顺序：1.sql → 2.sql（已含本表 CREATE IF NOT EXISTS）→ … → 12.sql（幂等）
-- 本件只为已初始化的存量库补表（2.sql 重放亦可——CREATE IF NOT EXISTS 幂等，本件保显式迁移语义）
CREATE TABLE IF NOT EXISTS `usecasedefinitiondecisionview_table` (
  `definitionId` VARCHAR(255),
  `tenantId` VARCHAR(255),
  `useCaseName` VARCHAR(255),
  `realizes` TEXT,
  `actors` TEXT,
  `trigger` VARCHAR(255),
  `preconditions` TEXT,
  `postconditions` TEXT,
  `orchestrationType` VARCHAR(255),
  `mainFlowJson` TEXT,
  `extensionsJson` TEXT,
  `generatedIds` TEXT,
  `optionalInputs` TEXT,
  `declaredAt` BIGINT,
  `declaredBy` VARCHAR(255),
  `removedAt` BIGINT,
  `removedBy` VARCHAR(255),
  PRIMARY KEY (`definitionId`)
);
