package pgstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/kart-io/goagent/codec/json"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
)

// RecordReconciliation implements management.ReconciliationStore.
func (s *Store) RecordReconciliation(ctx context.Context, r mg.Reconciliation) error {
	if r.ID == "" || r.TenantID == "" {
		return agentErrors.ErrManagementInvalid
	}
	r = mg.NormalizeReconciliation(r)
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
	if !mg.SameReconciliation(stored, r) {
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
	if err = json.Unmarshal(refs, &r.OperationRefs); err != nil {
		return mg.Reconciliation{}, err
	}
	return mg.NormalizeReconciliation(r), nil
}

var _ mg.ReconciliationStore = (*Store)(nil)
