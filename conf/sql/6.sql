-- E5-WP1 B1（2026-09-13·软删除令）：FMD 聚合墓碑列——FieldModelRemoved 投影由行级 DELETE 改为
-- removedAt 戳（行留档·rebuilder 滤墓碑）。对齐 CaptureNote/CemEdge/RoleCommandBinding 三族墓碑先例。
-- 存量库经此 ALTER 补列；fresh initdb 顺序执行 1..6（2.sql 建表无此列·6.sql 补齐）。
USE cqrs_jdbc_demo;
ALTER TABLE `fieldmodeldefinitiondecisionview_table` ADD COLUMN `removedAt` BIGINT NULL;
