//go:build goagent_storetest

// Needs management/storetest, which ships with goagent v0.60.0. Run with
// -tags goagent_storetest against a goagent that has it.

package pgstore

import (
	"testing"

	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/storetest"
)

func TestListConformance(t *testing.T) {
	storetest.RunListConformance(t, func(t *testing.T) mg.Store {
		_, s := database(t)
		// RunListConformance wants an empty store; its rows live under one tenant.
		_, err := s.pool.Exec(t.Context(), `DELETE FROM mg_outbox WHERE tenant_id=$1`, "storetest-tenant")
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
