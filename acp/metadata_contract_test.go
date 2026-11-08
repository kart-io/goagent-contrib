package acp

import (
	"context"
	"fmt"
	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/constants"
	"github.com/kart-io/goagent/core/session"
	agentErrors "github.com/kart-io/goagent/errors"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestListPersistsBeforePromptAcrossStoreReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := session.NewFileStore(dir)
	require.NoError(t, err)
	h := start(t, NewServer(plainFactory(newModel(), nil), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	cwd := t.TempDir()
	id := h.newSession(t, cwd)
	reopened, err := session.NewFileStore(dir)
	require.NoError(t, err)
	h2 := start(t, NewServer(plainFactory(newModel(), nil), WithSessionStore(reopened)), newEditor())
	h2.initialize(t, acpsdk.ClientCapabilities{})
	list, err := h2.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{})
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
	require.Equal(t, id, list.Sessions[0].SessionId)
	require.Equal(t, cwd, list.Sessions[0].Cwd)
	require.NotNil(t, list.Sessions[0].UpdatedAt)
	other := filepath.Join(cwd, "other")
	list, err = h2.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{Cwd: &other})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}
func TestListPaginationAndLegacyMetadata(t *testing.T) {
	store := session.NewMemoryStore()
	cwd := t.TempDir()
	for i := 0; i < 101; i++ {
		meta, err := session.NewEvent("acp/session_metadata", map[string]any{"schema": 1, "cwd": cwd, "updatedAt": "2026-10-31T00:00:00Z"})
		require.NoError(t, err)
		_, err = store.Append(context.Background(), fmt.Sprintf("s%03d", i), 0, meta)
		require.NoError(t, err)
	}
	legacy, err := session.NewEvent(constants.SessionEventTurnStart, map[string]any{})
	require.NoError(t, err)
	_, err = store.Append(context.Background(), "legacy", 0, legacy)
	require.NoError(t, err)
	h := start(t, NewServer(plainFactory(newModel(), nil), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	list, err := h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{})
	require.NoError(t, err)
	require.Len(t, list.Sessions, 100)
	require.Equal(t, acpsdk.SessionId("s000"), list.Sessions[0].SessionId)
	require.NotNil(t, list.NextCursor)
	next, err := h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{Cursor: list.NextCursor})
	require.NoError(t, err)
	require.Len(t, next.Sessions, 1)
	require.Equal(t, acpsdk.SessionId("s100"), next.Sessions[0].SessionId)
	require.Nil(t, next.NextCursor)
	bad := "坏游标"
	_, err = h.client.ListSessions(context.Background(), acpsdk.ListSessionsRequest{Cursor: &bad})
	requireCode(t, err, -32602)
}

type conflictStore struct {
	session.Store
	attempts int
}

func (s *conflictStore) Append(ctx context.Context, id string, expected int64, events ...session.Event) (int64, error) {
	s.attempts++
	if s.attempts < 3 {
		return 0, agentErrors.ErrSessionConflict
	}
	return s.Store.Append(ctx, id, expected, events...)
}
func TestMetadataAppendRetriesConflicts(t *testing.T) {
	store := &conflictStore{Store: session.NewMemoryStore()}
	h := start(t, NewServer(plainFactory(newModel(), nil), WithSessionStore(store)), newEditor())
	h.initialize(t, acpsdk.ClientCapabilities{})
	h.newSession(t, t.TempDir())
	require.Equal(t, 3, store.attempts)
}
