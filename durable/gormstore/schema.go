package gormstore

// SchemaVersion 是本包要求的表结构版本，New 会校验。
const SchemaVersion = 1

// SessionSchemaVersion 是会话日志表结构的版本，NewSessionStore 会校验。
const SessionSchemaVersion = 1

// 闸门表的行数固定：前 scopeSlots 行给作用域，其余给租约。改动须同时改建表语句并升级 SchemaVersion。
const (
	scopeSlots = 256
	leaseSlots = 512
)

// SchemaPostgres 是 PostgreSQL 的建表语句，由宿主纳入自己的迁移流程；New 不自动建表。
// goagent_gorm_gate 是预置的锁行：本包用它的行锁串行化作用域删除与租约的获取、释放，不能增删。
const SchemaPostgres = `
CREATE TABLE goagent_gorm_schema (version int NOT NULL);
INSERT INTO goagent_gorm_schema (version) VALUES (1);

CREATE TABLE goagent_gorm_gate (slot int PRIMARY KEY);
INSERT INTO goagent_gorm_gate (slot) SELECT generate_series(0, 767);

CREATE TABLE goagent_gorm_snapshot (
  ns         text        NOT NULL,
  id         text        NOT NULL,
  data       bytea       NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (ns, id)
);

CREATE TABLE goagent_gorm_lease (
  ns         text        NOT NULL,
  id         text        NOT NULL,
  token      text        NOT NULL,
  owner      text        NOT NULL,
  expires_at timestamptz NOT NULL,
  PRIMARY KEY (ns, id)
);
CREATE INDEX goagent_gorm_lease_expires_at ON goagent_gorm_lease (expires_at);

CREATE TABLE goagent_gorm_scope (
  ns         text        PRIMARY KEY,
  removed_at timestamptz NOT NULL
);
`

// SessionSchemaPostgres 是 PostgreSQL 的会话日志建表语句，依赖 SchemaPostgres 的租约表；
// 只使用 durable.Store 的部署不需要它。
const SessionSchemaPostgres = `
CREATE TABLE goagent_gorm_session_schema (version int NOT NULL);
INSERT INTO goagent_gorm_session_schema (version) VALUES (1);

CREATE TABLE goagent_gorm_session (
  id         text        PRIMARY KEY,
  last_seq   bigint      NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE TABLE goagent_gorm_session_event (
  session_id text   NOT NULL REFERENCES goagent_gorm_session (id),
  seq        bigint NOT NULL,
  type       text   NOT NULL,
  event      bytea  NOT NULL,
  PRIMARY KEY (session_id, seq)
);
`

// SchemaMySQL 是 MySQL 8 的建表语句，含义同 SchemaPostgres。
// 键列为 VARBINARY：按字节比较与排序，不受排序规则和尾部空格影响；命名空间与 ID 各不超过 1024 字节，
// 超长的写入在严格模式（MySQL 8 默认）下报错。时间列存 UTC。
const SchemaMySQL = `
CREATE TABLE goagent_gorm_schema (version INT NOT NULL) ENGINE=InnoDB;
INSERT INTO goagent_gorm_schema (version) VALUES (1);

CREATE TABLE goagent_gorm_gate (slot INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO goagent_gorm_gate (slot)
WITH RECURSIVE s (n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 767)
SELECT n FROM s;

CREATE TABLE goagent_gorm_snapshot (
  ns         VARBINARY(1024) NOT NULL,
  id         VARBINARY(1024) NOT NULL,
  data       LONGBLOB        NOT NULL,
  updated_at DATETIME(6)     NOT NULL,
  PRIMARY KEY (ns, id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE goagent_gorm_lease (
  ns         VARBINARY(1024) NOT NULL,
  id         VARBINARY(1024) NOT NULL,
  token      VARBINARY(255)  NOT NULL,
  owner      VARBINARY(512)  NOT NULL,
  expires_at DATETIME(6)     NOT NULL,
  PRIMARY KEY (ns, id),
  KEY goagent_gorm_lease_expires_at (expires_at)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE goagent_gorm_scope (
  ns         VARBINARY(1024) NOT NULL PRIMARY KEY,
  removed_at DATETIME(6)     NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
`

// SessionSchemaMySQL 是 MySQL 8 的会话日志建表语句，含义同 SessionSchemaPostgres。
const SessionSchemaMySQL = `
CREATE TABLE goagent_gorm_session_schema (version INT NOT NULL) ENGINE=InnoDB;
INSERT INTO goagent_gorm_session_schema (version) VALUES (1);

CREATE TABLE goagent_gorm_session (
  id         VARBINARY(1024) NOT NULL PRIMARY KEY,
  last_seq   BIGINT          NOT NULL,
  created_at DATETIME(6)     NOT NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

CREATE TABLE goagent_gorm_session_event (
  session_id VARBINARY(1024) NOT NULL,
  seq        BIGINT          NOT NULL,
  type       VARBINARY(255)  NOT NULL,
  event      LONGBLOB        NOT NULL,
  PRIMARY KEY (session_id, seq),
  CONSTRAINT goagent_gorm_session_event_session FOREIGN KEY (session_id) REFERENCES goagent_gorm_session (id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
`
