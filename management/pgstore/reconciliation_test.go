package pgstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/kart-io/goagent/core/durable"
	agentErrors "github.com/kart-io/goagent/errors"
	mg "github.com/kart-io/goagent/management"
	"github.com/stretchr/testify/require"
)

var _ mg.ReconciliationReader = (*Store)(nil)

func uniqueTenant() string { return fmt.Sprintf("rec-%d", time.Now().UnixNano()) }

func sampleReconciliation(tenant, id string) mg.Reconciliation {
	return mg.Reconciliation{
		ID: id, TenantID: tenant, TaskID: "task-1", VersionHash: "vh", EvidenceHash: "eh",
		Resolution: "applied", VerifiedBy: "ops",
		VerifiedAt:    time.Date(2026, 11, 20, 1, 2, 3, 456789000, time.UTC),
		OperationRefs: []durable.OperationRef{{ParentKey: "p", ID: "op-1"}, {ParentKey: "p", ID: "op-2"}},
	}
}

func TestReconciliationRoundTrip(t *testing.T) {
	ctx, s := database(t)
	want := sampleReconciliation(uniqueTenant(), "rec-1")
	require.NoError(t, s.RecordReconciliation(ctx, want))
	got, err := s.Lookup(ctx, mg.Actor{TenantID: want.TenantID, Subject: "u"}, want.ID)
	require.NoError(t, err)
	require.True(t, want.VerifiedAt.Equal(got.VerifiedAt))
	want.VerifiedAt, got.VerifiedAt = time.Time{}, time.Time{}
	require.Equal(t, want, got)
}

func TestReconciliationIdempotentAndConflict(t *testing.T) {
	ctx, s := database(t)
	base := sampleReconciliation(uniqueTenant(), "rec-1")
	require.NoError(t, s.RecordReconciliation(ctx, base))
	require.NoError(t, s.RecordReconciliation(ctx, base), "same content is a no-op")
	// Sub-microsecond differences are below the stored precision and must not count as a change.
	sameAtStoredPrecision := base
	sameAtStoredPrecision.VerifiedAt = base.VerifiedAt.Add(300 * time.Nanosecond)
	require.NoError(t, s.RecordReconciliation(ctx, sameAtStoredPrecision))

	for name, mutate := range map[string]func(*mg.Reconciliation){
		"resolution": func(r *mg.Reconciliation) { r.Resolution = "not_applied" },
		"evidence":   func(r *mg.Reconciliation) { r.EvidenceHash = "other" },
		"task":       func(r *mg.Reconciliation) { r.TaskID = "task-2" },
		"verifiedAt": func(r *mg.Reconciliation) { r.VerifiedAt = r.VerifiedAt.Add(time.Second) },
		"operations": func(r *mg.Reconciliation) { r.OperationRefs = r.OperationRefs[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.OperationRefs = append([]durable.OperationRef(nil), base.OperationRefs...)
			mutate(&changed)
			require.ErrorIs(t, s.RecordReconciliation(ctx, changed), agentErrors.ErrManagementConflict)
		})
	}
	got, err := s.Lookup(ctx, mg.Actor{TenantID: base.TenantID}, base.ID)
	require.NoError(t, err)
	require.Equal(t, "applied", got.Resolution, "a rejected record must not overwrite the stored one")
}

func TestReconciliationRejectsIncompleteRecord(t *testing.T) {
	ctx, s := database(t)
	for name, mutate := range map[string]func(*mg.Reconciliation){
		"no id":     func(r *mg.Reconciliation) { r.ID = "" },
		"no tenant": func(r *mg.Reconciliation) { r.TenantID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			r := sampleReconciliation(uniqueTenant(), "rec-1")
			mutate(&r)
			require.ErrorIs(t, s.RecordReconciliation(ctx, r), agentErrors.ErrManagementInvalid)
		})
	}
}

func TestReconciliationLookupIsTenantScoped(t *testing.T) {
	ctx, s := database(t)
	owner := uniqueTenant()
	require.NoError(t, s.RecordReconciliation(ctx, sampleReconciliation(owner, "rec-1")))
	for name, actor := range map[string]mg.Actor{
		"other tenant": {TenantID: owner + "-other"},
		"empty tenant": {},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := s.Lookup(ctx, actor, "rec-1")
			require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
		})
	}
	_, err := s.Lookup(ctx, mg.Actor{TenantID: owner}, "missing")
	require.ErrorIs(t, err, agentErrors.ErrManagementNotFound)
	// The same ID in another tenant is an independent record.
	other := sampleReconciliation(owner+"-other", "rec-1")
	other.Resolution = "not_applied"
	require.NoError(t, s.RecordReconciliation(ctx, other))
	got, err := s.Lookup(ctx, mg.Actor{TenantID: owner}, "rec-1")
	require.NoError(t, err)
	require.Equal(t, "applied", got.Resolution)
}

func TestMigrateIsIdempotentWithReconciliations(t *testing.T) {
	ctx, s := database(t)
	r := sampleReconciliation(uniqueTenant(), "rec-1")
	require.NoError(t, s.RecordReconciliation(ctx, r))
	require.NoError(t, Migrate(ctx, s.pool))
	require.NoError(t, Migrate(ctx, s.pool))
	_, err := s.Lookup(ctx, mg.Actor{TenantID: r.TenantID}, r.ID)
	require.NoError(t, err, "re-running Migrate must keep existing reconciliations")
	var v int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT version FROM mg_schema`).Scan(&v))
	require.Equal(t, 1, v, "reconciliations must not bump the mg_schema version")
}
