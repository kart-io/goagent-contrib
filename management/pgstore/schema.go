package pgstore

// SchemaVersion 是 mg_schema 中记录的表结构版本，New 会校验。
const SchemaVersion = 1

// Schema 是管理实体与核对记录的建表语句，由宿主纳入自己的迁移流程；New 不建表。
// 整段可重复执行：已有的表、列、约束和索引保持不变，所以也可以直接用于升级已有的库。
// management.Kinds 的每一种实体对应一张 mg_<kind> 表，列与约束相同。
const Schema = `
CREATE TABLE IF NOT EXISTS mg_schema (version integer PRIMARY KEY CHECK (version = 1));
INSERT INTO mg_schema VALUES (1) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS mg_agents (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_agents_created ON mg_agents (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_versions (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_versions_created ON mg_versions (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_sessions (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_sessions_created ON mg_sessions (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_tasks (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_tasks_created ON mg_tasks (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_attempts (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_attempts_created ON mg_attempts (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_approvals (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_approvals_created ON mg_approvals (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_commands (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_commands_created ON mg_commands (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_idempotency (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_idempotency_created ON mg_idempotency (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_audit (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_audit_created ON mg_audit (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_outbox (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_outbox_created ON mg_outbox (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_workers (
  tenant_id  text        NOT NULL,
  id         text        NOT NULL,
  revision   bigint      NOT NULL CHECK (revision > 0),
  created_at timestamptz NOT NULL,
  data       jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id),
  CHECK (data->>'id' = id),
  CHECK (data->>'tenant_id' = tenant_id),
  CHECK ((data->>'revision')::bigint = revision)
);
CREATE INDEX IF NOT EXISTS mg_workers_created ON mg_workers (tenant_id, created_at, id);

CREATE TABLE IF NOT EXISTS mg_reconciliations (
  tenant_id      text        NOT NULL,
  id             text        NOT NULL,
  task_id        text        NOT NULL,
  version_hash   text        NOT NULL,
  evidence_hash  text        NOT NULL,
  resolution     text        NOT NULL,
  verified_by    text        NOT NULL,
  verified_at    timestamptz NOT NULL,
  operation_refs jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id)
);

ALTER TABLE mg_versions ADD COLUMN IF NOT EXISTS agent_id text GENERATED ALWAYS AS (data->>'agent_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_versions_agent_id_fk' AND conrelid = 'mg_versions'::regclass) THEN
    ALTER TABLE mg_versions ADD CONSTRAINT mg_versions_agent_id_fk FOREIGN KEY (tenant_id, agent_id) REFERENCES mg_agents (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_sessions ADD COLUMN IF NOT EXISTS agent_id text GENERATED ALWAYS AS (data->>'agent_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_sessions_agent_id_fk' AND conrelid = 'mg_sessions'::regclass) THEN
    ALTER TABLE mg_sessions ADD CONSTRAINT mg_sessions_agent_id_fk FOREIGN KEY (tenant_id, agent_id) REFERENCES mg_agents (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_tasks ADD COLUMN IF NOT EXISTS session_id text GENERATED ALWAYS AS (data->>'session_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_tasks_session_id_fk' AND conrelid = 'mg_tasks'::regclass) THEN
    ALTER TABLE mg_tasks ADD CONSTRAINT mg_tasks_session_id_fk FOREIGN KEY (tenant_id, session_id) REFERENCES mg_sessions (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_tasks ADD COLUMN IF NOT EXISTS agent_id text GENERATED ALWAYS AS (data->>'agent_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_tasks_agent_id_fk' AND conrelid = 'mg_tasks'::regclass) THEN
    ALTER TABLE mg_tasks ADD CONSTRAINT mg_tasks_agent_id_fk FOREIGN KEY (tenant_id, agent_id) REFERENCES mg_agents (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_approvals ADD COLUMN IF NOT EXISTS task_id text GENERATED ALWAYS AS (data->>'task_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_approvals_task_id_fk' AND conrelid = 'mg_approvals'::regclass) THEN
    ALTER TABLE mg_approvals ADD CONSTRAINT mg_approvals_task_id_fk FOREIGN KEY (tenant_id, task_id) REFERENCES mg_tasks (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_attempts ADD COLUMN IF NOT EXISTS task_id text GENERATED ALWAYS AS (data->>'task_id') STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_attempts_task_id_fk' AND conrelid = 'mg_attempts'::regclass) THEN
    ALTER TABLE mg_attempts ADD CONSTRAINT mg_attempts_task_id_fk FOREIGN KEY (tenant_id, task_id) REFERENCES mg_tasks (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_sessions ADD COLUMN IF NOT EXISTS version_id text GENERATED ALWAYS AS ((data->>'agent_id')||'@'||(data->>'version')) STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_sessions_version_fk' AND conrelid = 'mg_sessions'::regclass) THEN
    ALTER TABLE mg_sessions ADD CONSTRAINT mg_sessions_version_fk FOREIGN KEY (tenant_id, version_id) REFERENCES mg_versions (tenant_id, id);
  END IF;
END $$;

ALTER TABLE mg_tasks ADD COLUMN IF NOT EXISTS version_id text GENERATED ALWAYS AS ((data->>'agent_id')||'@'||(data->>'version')) STORED;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'mg_tasks_version_fk' AND conrelid = 'mg_tasks'::regclass) THEN
    ALTER TABLE mg_tasks ADD CONSTRAINT mg_tasks_version_fk FOREIGN KEY (tenant_id, version_id) REFERENCES mg_versions (tenant_id, id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS mg_tasks_state ON mg_tasks (tenant_id, (data->>'state'), created_at, id);
CREATE INDEX IF NOT EXISTS mg_commands_state ON mg_commands (tenant_id, (data->>'state'), created_at, id);
CREATE INDEX IF NOT EXISTS mg_approvals_state ON mg_approvals (tenant_id, (data->>'state'), created_at, id);
CREATE INDEX IF NOT EXISTS mg_outbox_state ON mg_outbox (tenant_id, (data->>'state'), created_at, id);
`
