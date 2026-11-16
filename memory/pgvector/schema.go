package pgvector

// SchemaVersion 是本包要求的表结构版本，New 会校验。
const SchemaVersion = 1

// Schema 是建表语句，由宿主纳入自己的迁移流程；New 不自动建表。
// 不含 CREATE EXTENSION vector：该语句需要较高权限，由宿主或 DBA 预先执行。
const Schema = `
CREATE TABLE goagent_memory_vectors_schema (version int NOT NULL);
INSERT INTO goagent_memory_vectors_schema (version) VALUES (1);

CREATE TABLE goagent_memory_vectors (
  ns  text[] NOT NULL,
  id  text   NOT NULL,
  dim int    NOT NULL,
  vec vector NOT NULL,
  PRIMARY KEY (ns, id)
);
`
