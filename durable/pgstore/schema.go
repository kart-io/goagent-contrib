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
