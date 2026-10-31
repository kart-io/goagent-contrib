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
