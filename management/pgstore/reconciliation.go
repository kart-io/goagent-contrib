package pgstore

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/kart-io/goagent/codec/json"
	"github.com/kart-io/goagent/core/durable"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
)

// reconciliationSchema is applied by Migrate. It is not a Document kind and
// does not change the mg_schema version: the table is additive and only read
// through Lookup.
const reconciliationSchema = `CREATE TABLE IF NOT EXISTS mg_reconciliations(
tenant_id text NOT NULL,
id text NOT NULL,
task_id text NOT NULL,
version_hash text NOT NULL,
evidence_hash text NOT NULL,
resolution text NOT NULL,
verified_by text NOT NULL,
verified_at timestamptz NOT NULL,
operation_refs jsonb NOT NULL,
PRIMARY KEY(tenant_id,id))`

// normalized brings a record to what the table can represent: UTC at
// microsecond precision and a non-nil operation list, so a retry of the same
// record compares equal to what was stored.
func normalized(r mg.Reconciliation) mg.Reconciliation {
	r.VerifiedAt = r.VerifiedAt.UTC().Truncate(time.Microsecond)
	if r.OperationRefs == nil {
		r.OperationRefs = []durable.OperationRef{}
	}
	return r
}

func sameReconciliation(a, b mg.Reconciliation) bool {
	a, b = normalized(a), normalized(b)
	return a.ID == b.ID && a.TenantID == b.TenantID && a.TaskID == b.TaskID && a.VersionHash == b.VersionHash &&
		a.EvidenceHash == b.EvidenceHash && a.Resolution == b.Resolution && a.VerifiedBy == b.VerifiedBy &&
		a.VerifiedAt.Equal(b.VerifiedAt) && slices.Equal(a.OperationRefs, b.OperationRefs)
}

// RecordReconciliation stores host-verified evidence for a task in attention.
// Recording the same ID with identical content again is a no-op; the same ID
// with different content returns ErrManagementConflict and leaves the stored
// record untouched. IDs are scoped by tenant.
func (s *Store) RecordReconciliation(ctx context.Context, r mg.Reconciliation) error {
	if r.ID == "" || r.TenantID == "" {
		return agentErrors.ErrManagementInvalid
	}
	r = normalized(r)
	refs, err := json.Marshal(r.OperationRefs)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO mg_reconciliations(tenant_id,id,task_id,version_hash,evidence_hash,resolution,verified_by,verified_at,operation_refs)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(tenant_id,id) DO NOTHING`,
		r.TenantID, r.ID, r.TaskID, r.VersionHash, r.EvidenceHash, r.Resolution, r.VerifiedBy, r.VerifiedAt, refs)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	stored, err := s.Lookup(ctx, mg.Actor{TenantID: r.TenantID}, r.ID)
	if err != nil {
		return err
	}
	if !sameReconciliation(stored, r) {
		return agentErrors.ErrManagementConflict
	}
	return nil
}

// Lookup implements management.ReconciliationReader. It only sees records of
// the actor's tenant; anything else, including an empty tenant, is NotFound.
func (s *Store) Lookup(ctx context.Context, a mg.Actor, id string) (r mg.Reconciliation, err error) {
	var refs []byte
	err = s.pool.QueryRow(ctx, `SELECT id,tenant_id,task_id,version_hash,evidence_hash,resolution,verified_by,verified_at,operation_refs FROM mg_reconciliations WHERE tenant_id=$1 AND id=$2`, a.TenantID, id).
		Scan(&r.ID, &r.TenantID, &r.TaskID, &r.VersionHash, &r.EvidenceHash, &r.Resolution, &r.VerifiedBy, &r.VerifiedAt, &refs)
	if errors.Is(err, pgx.ErrNoRows) {
		return mg.Reconciliation{}, agentErrors.ErrManagementNotFound
	}
	if err != nil {
		return mg.Reconciliation{}, err
	}
	r.VerifiedAt = r.VerifiedAt.UTC()
	err = json.Unmarshal(refs, &r.OperationRefs)
	return
}
