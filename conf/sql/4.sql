-- R1 实例投影——kn_instance_* 表族（批4·kn_kb_storage_plan §2.3/§4.3，2026-09-06）
-- Source: app/net/imadz/metarch/application/projection/repository/KnInstanceStore.scala（ensureTables 同源——
-- 改运行时 DDL 必同步本文件；运行时 CREATE TABLE IF NOT EXISTS 自建，本文件为 DBA 参考/重建底账）
-- MySQL dialect; H2 test-compatible via jdbc:h2:mem:...;MODE=MySQL
-- 手写领地（行为侧）·CodegenMain 不产本文件族——领地声明挂 EP_S2_48 批1

-- 聚合实例：每实体一行，version=最新 sequenceNr（重放幂等：GREATEST 护栏）
CREATE TABLE IF NOT EXISTS `kn_aggregate_instance` (
  `instance_id` VARCHAR(255) NOT NULL,
  `agg_type` VARCHAR(128) NOT NULL,
  `tenant_id` VARCHAR(128),
  `status` VARCHAR(64),
  `version` BIGINT NOT NULL,
  `updated_at_millis` BIGINT NOT NULL,
  PRIMARY KEY (`instance_id`, `agg_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 事件流水：insert-only，event_id = persistenceId|sequenceNr（＝journal 指针·载荷指针化——
-- 不存业务长文，R2/M1 需要 payload 时经 (persistence_id, sequence_nr) 回查 journal）
-- causation_id：R0.5 批3 裁决——事件自身不带因果（metadata 通道判死），由 command_audit
-- 对齐注入（时间窗＋实体内序号邻接），本列预留 NULL
CREATE TABLE IF NOT EXISTS `kn_instance_event` (
  `event_id` VARCHAR(255) NOT NULL,
  `instance_id` VARCHAR(255) NOT NULL,
  `agg_type` VARCHAR(128) NOT NULL,
  `event_type` VARCHAR(128) NOT NULL,
  `sequence_nr` BIGINT NOT NULL,
  `persistence_id` VARCHAR(255) NOT NULL,
  `occurred_at_millis` BIGINT NOT NULL,
  `tenant_id` VARCHAR(128),
  `causation_id` VARCHAR(255),
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 策略触发：R1 结构占位——数据源缺口已挂账（策略守护命令为进程内派发，不经 HTTP，
-- CommandAuditFilter（批3）捕获不到；待内部命令漏斗补因果发射后填充）
CREATE TABLE IF NOT EXISTS `kn_policy_firing` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `policy_id` VARCHAR(128) NOT NULL,
  `trigger_event_id` VARCHAR(255) NOT NULL,
  `target_command` VARCHAR(128),
  `fired_at_millis` BIGINT NOT NULL,
  `tenant_id` VARCHAR(128),
  PRIMARY KEY (`id`),
  CONSTRAINT `uq_kn_policy_firing` UNIQUE (`policy_id`, `trigger_event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 投影毒丸 DLQ：handler 失败落此表后 offset 越过（tag 不堵——capture-note varchar(255)
-- 固定 offset 永重试前科的反制，kn_kb_storage_plan §八 风险⑥）
CREATE TABLE IF NOT EXISTS `kn_projection_dlq` (
  `event_id` VARCHAR(255) NOT NULL,
  `agg_type` VARCHAR(128),
  `event_type` VARCHAR(128),
  `persistence_id` VARCHAR(255),
  `sequence_nr` BIGINT,
  `error` VARCHAR(1024),
  `failed_at_millis` BIGINT NOT NULL,
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 大表后可手工补二级索引（H2 不兼容故不入建表语句，MySQL 手工执行）：
--   CREATE INDEX idx_knie_instance_time ON kn_instance_event (instance_id, sequence_nr);
--   CREATE INDEX idx_knie_tenant_time ON kn_instance_event (tenant_id, occurred_at_millis);
--   CREATE INDEX idx_knai_tenant ON kn_aggregate_instance (tenant_id);
