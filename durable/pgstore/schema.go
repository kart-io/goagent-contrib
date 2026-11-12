package pgstore

// SchemaVersion 是本包要求的表结构版本，New 会校验。
const SchemaVersion = 1

// Schema 是建表语句，由宿主纳入自己的迁移流程；New 不自动建表。
const Schema = `
CREATE TABLE goagent_durable_schema (version int NOT NULL);
INSERT INTO goagent_durable_schema (version) VALUES (1);

CREATE TABLE goagent_durable_snapshot (
  ns         text        NOT NULL,
  id         text        NOT NULL,
  data       bytea       NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ns, id)
);

CREATE TABLE goagent_durable_lease (
  ns         text        NOT NULL,
  id         text        NOT NULL,
  token      text        NOT NULL,
  owner      text        NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (ns, id)
) WITH (fillfactor = 70);
CREATE INDEX goagent_durable_lease_expires_at ON goagent_durable_lease (expires_at);

CREATE TABLE goagent_durable_scope (
  ns         text        PRIMARY KEY,
  removed_at timestamptz NOT NULL
);
`

// SnapshotCompressionLZ4 把快照列的 TOAST 压缩改为 lz4：大快照（如长期记忆的基础快照）写入时 CPU 开销远低于默认的 pglz，
// 同表中可压缩的 JSON 快照仍然压缩。需要 PostgreSQL 14+ 且服务端支持 lz4，否则语句报错、表不变。
// 只改元数据、不重写表，已有的值保持原压缩方式直到被改写；新旧二进制都能读写，因此不升级 SchemaVersion。
// 由宿主纳入迁移流程，执行时建议设置 lock_timeout；回滚执行 SnapshotCompressionPGLZ。
const SnapshotCompressionLZ4 = `ALTER TABLE goagent_durable_snapshot ALTER COLUMN data SET COMPRESSION lz4`

// SnapshotCompressionPGLZ 恢复默认的 pglz 压缩（SnapshotCompressionLZ4 的回滚），同样只影响之后写入的值。
const SnapshotCompressionPGLZ = `ALTER TABLE goagent_durable_snapshot ALTER COLUMN data SET COMPRESSION pglz`

// SessionSchemaVersion 是会话日志表结构的版本，NewSessionStore 会校验。
const SessionSchemaVersion = 1

// SessionSchema 是会话日志的建表语句，与 Schema 一起由宿主纳入迁移流程（锁使用 Schema 中的租约表）；
// 只使用 durable.Store 的部署不需要它。
const SessionSchema = `
CREATE TABLE goagent_session_schema (version int NOT NULL);
INSERT INTO goagent_session_schema (version) VALUES (1);

CREATE TABLE goagent_session (
  id         text        PRIMARY KEY,
  last_seq   bigint      NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE goagent_session_event (
  session_id text   NOT NULL REFERENCES goagent_session (id),
  seq        bigint NOT NULL,
  type       text   NOT NULL,
  event      bytea  NOT NULL,
  PRIMARY KEY (session_id, seq)
);
`
