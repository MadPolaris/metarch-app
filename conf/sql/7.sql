-- S2-82-B3（2026-09-13·软删除令）：VO/RMD 行墓碑列——ValueObjectRemoved/ReadModelRemoved 投影为
-- removedAt 戳（行留档·rebuilder 滤墓碑）。FMD 6.sql 同款先例。
-- 存量库经此 ALTER 补列；fresh initdb 顺序执行 1..7（2.sql 建表无此列·7.sql 补齐）。
USE cqrs_jdbc_demo;
ALTER TABLE `valueobjectdefinitiondecisionview_table` ADD COLUMN `removedAt` BIGINT NULL;
ALTER TABLE `readmodeldefinitiondecisionview_table` ADD COLUMN `removedAt` BIGINT NULL;
