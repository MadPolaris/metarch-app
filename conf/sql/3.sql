-- R0.5 因果审计——命令侧落账（批3 · Option A，2026-09-06）
-- Source: app/controllers/filter/CommandAuditFilter.scala（ScalikeJdbcCommandAuditStore.ensureTable 同源——
-- 改运行时 DDL 必同步本文件；运行时 CREATE TABLE IF NOT EXISTS 自建，本文件为 DBA 参考/重建底账）
-- MySQL dialect; H2 test-compatible via jdbc:h2:mem:...;MODE=MySQL

CREATE TABLE IF NOT EXISTS `command_audit` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `request_id` VARCHAR(64) NOT NULL,
  `occurred_at_millis` BIGINT NOT NULL,
  `method` VARCHAR(8) NOT NULL,
  `route_pattern` VARCHAR(255),
  `path` VARCHAR(512) NOT NULL,
  `controller` VARCHAR(255),
  `command` VARCHAR(128),
  `entity_id` VARCHAR(128),
  `user_id` VARCHAR(128),
  `tenant_id` VARCHAR(128),
  `status` INT NOT NULL,
  `duration_ms` BIGINT NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `uq_command_audit_request_id` UNIQUE (`request_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 因果对齐查询（命令→事件边重建·v4r10 世界事件无时间戳，以实体内序号邻接为主锚）：
--   SELECT * FROM command_audit WHERE entity_id = ? ORDER BY occurred_at_millis;
--   SELECT command, entity_id, occurred_at_millis FROM command_audit
--     WHERE occurred_at_millis BETWEEN ? AND ? ORDER BY occurred_at_millis;
-- 大表后可手工补二级索引（H2 不兼容故不入建表语句，MySQL 手工执行）：
--   CREATE INDEX idx_cmd_audit_cmd_time ON command_audit (command, occurred_at_millis);
--   CREATE INDEX idx_cmd_audit_entity_time ON command_audit (entity_id, occurred_at_millis);
