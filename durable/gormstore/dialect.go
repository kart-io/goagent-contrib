package gormstore

// dialect 是各数据库写法不同的片段，其余语句两种数据库共用。时间一律取数据库时钟。
type dialect struct {
	// now 是当前时间；after、before 是当前时间加、减 ? 毫秒。
	now, after, before string
	// upsertSnapshot 的参数依次为 ns、id、data。
	upsertSnapshot string
	// addTombstone 写入墓碑，已有的墓碑保留原时间；参数为 ns。
	addTombstone string
	// ensureSession 建一个序号为 0 的会话行，已存在时不改动并持有它的排他行锁或等待其提交；参数为 id。
	ensureSession string
	// deleteBatch 删除一个命名空间的至多 N 行；格式化参数为表名与 N，查询参数为 ns。
	deleteBatch string
	// listSessions 按字节序返回全部会话 ID。
	listSessions string
}

var dialects = map[string]dialect{
	"postgres": {
		now:    `now()`,
		after:  `now() + ? * interval '1 millisecond'`,
		before: `now() - ? * interval '1 millisecond'`,
		upsertSnapshot: `INSERT INTO goagent_gorm_snapshot (ns, id, data, updated_at) VALUES (?, ?, ?, now())
ON CONFLICT (ns, id) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at`,
		addTombstone:  `INSERT INTO goagent_gorm_scope (ns, removed_at) VALUES (?, now()) ON CONFLICT (ns) DO NOTHING`,
		ensureSession: `INSERT INTO goagent_gorm_session (id, last_seq, created_at) VALUES (?, 0, now()) ON CONFLICT (id) DO NOTHING`,
		deleteBatch:   `DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE ns = ? LIMIT %[2]d)`,
		listSessions:  `SELECT id FROM goagent_gorm_session ORDER BY id COLLATE "C"`,
	},
	"mysql": {
		now:    `UTC_TIMESTAMP(6)`,
		after:  `TIMESTAMPADD(MICROSECOND, ? * 1000, UTC_TIMESTAMP(6))`,
		before: `TIMESTAMPADD(MICROSECOND, -(? * 1000), UTC_TIMESTAMP(6))`,
		upsertSnapshot: `INSERT INTO goagent_gorm_snapshot (ns, id, data, updated_at) VALUES (?, ?, ?, UTC_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE data = VALUES(data), updated_at = VALUES(updated_at)`,
		// 不用 INSERT IGNORE：它在重复键上只留共享锁，随后升级为排他锁时并发事务会互相死锁，且会把超长值截断。
		addTombstone:  `INSERT INTO goagent_gorm_scope (ns, removed_at) VALUES (?, UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE ns = ns`,
		ensureSession: `INSERT INTO goagent_gorm_session (id, last_seq, created_at) VALUES (?, 0, UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE id = id`,
		deleteBatch:   `DELETE FROM %[1]s WHERE ns = ? LIMIT %[2]d`,
		listSessions:  `SELECT id FROM goagent_gorm_session ORDER BY id`,
	},
}
