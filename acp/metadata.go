package acp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	acpsdk "github.com/coder/acp-go-sdk"
	"github.com/kart-io/goagent/core/session"
)

const metadataType = "acp/session_metadata"
const pageSize = 100

type sessionMetadata struct {
	Schema    int       `json:"schema"`
	Cwd       string    `json:"cwd"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func appendMetadata(ctx context.Context, store session.Store, id string, meta sessionMetadata) (err error) {
	unlock, err := store.Lock(ctx, id, "acp-metadata")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlock()) }()
	ev, err := session.NewEvent(metadataType, meta)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		events, readErr := store.Read(ctx, id, 0)
		if readErr != nil && !errors.Is(readErr, session.ErrNotFound) {
			return readErr
		}
		_, err = store.Append(ctx, id, session.LastSeq(events), ev)
		if !errors.Is(err, session.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("acp: 元数据追加冲突超过 3 次：%w", err)
}

func (s *Session) persistMetadata(ctx context.Context) error {
	if s.conn.srv.store == nil {
		return nil
	}
	return appendMetadata(ctx, s.conn.srv.store, s.id, sessionMetadata{Schema: 1, Cwd: s.cwd, UpdatedAt: time.Now().UTC()})
}

type listItem struct {
	id   string
	meta sessionMetadata
}
type listCursor struct {
	UpdatedAt time.Time `json:"updatedAt"`
	ID        string    `json:"id"`
}

func readMetadata(events []session.Event) (sessionMetadata, bool, error) {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type != metadataType {
			continue
		}
		var meta sessionMetadata
		if err := json.Unmarshal(events[i].Data, &meta); err != nil {
			return meta, false, err
		}
		if meta.Schema != 1 || !filepath.IsAbs(meta.Cwd) || meta.UpdatedAt.IsZero() {
			return meta, false, errors.New("acp: 会话元数据无效")
		}
		meta.Cwd = filepath.Clean(meta.Cwd)
		return meta, true, nil
	}
	return sessionMetadata{}, false, nil
}

// ListSessions 从持久事件还原目录；旧日志须显式 load 后才能进入列表。
func (c *conn) ListSessions(ctx context.Context, p acpsdk.ListSessionsRequest) (acpsdk.ListSessionsResponse, error) {
	if c.srv.store == nil {
		return acpsdk.ListSessionsResponse{}, acpsdk.NewMethodNotFound(acpsdk.AgentMethodSessionList)
	}
	var cursor listCursor
	if p.Cursor != nil {
		data, err := base64.RawURLEncoding.DecodeString(*p.Cursor)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.ID == "" || cursor.UpdatedAt.IsZero() {
			return acpsdk.ListSessionsResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": "acp: 无效的会话游标"})
		}
	}
	if p.Cwd != nil && !filepath.IsAbs(*p.Cwd) {
		return acpsdk.ListSessionsResponse{}, acpsdk.NewInvalidParams(map[string]any{"error": "acp: cwd 必须是绝对路径"})
	}
	ids, err := c.srv.store.List(ctx)
	if err != nil {
		return acpsdk.ListSessionsResponse{}, internalError("列出会话", err)
	}
	var items []listItem
	for _, id := range ids {
		events, err := c.srv.store.Read(ctx, id, 0)
		if err != nil {
			return acpsdk.ListSessionsResponse{}, internalError("读取会话元数据", err)
		}
		meta, ok, err := readMetadata(events)
		if err != nil {
			return acpsdk.ListSessionsResponse{}, internalError("解析会话元数据", err)
		}
		if !ok || (p.Cwd != nil && meta.Cwd != filepath.Clean(*p.Cwd)) {
			continue
		}
		if p.Cursor != nil && (meta.UpdatedAt.After(cursor.UpdatedAt) || (meta.UpdatedAt.Equal(cursor.UpdatedAt) && id <= cursor.ID)) {
			continue
		}
		items = append(items, listItem{id, meta})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].meta.UpdatedAt.Equal(items[j].meta.UpdatedAt) {
			return items[i].id < items[j].id
		}
		return items[i].meta.UpdatedAt.After(items[j].meta.UpdatedAt)
	})
	out := acpsdk.ListSessionsResponse{Sessions: []acpsdk.SessionInfo{}}
	for i, item := range items {
		if i == pageSize {
			last := items[i-1]
			data, err := json.Marshal(listCursor{last.meta.UpdatedAt, last.id})
			if err != nil {
				return acpsdk.ListSessionsResponse{}, internalError("编码会话游标", err)
			}
			next := base64.RawURLEncoding.EncodeToString(data)
			out.NextCursor = &next
			break
		}
		updated := item.meta.UpdatedAt.UTC().Format(time.RFC3339Nano)
		out.Sessions = append(out.Sessions, acpsdk.SessionInfo{SessionId: acpsdk.SessionId(item.id), Cwd: item.meta.Cwd, UpdatedAt: &updated})
	}
	return out, nil
}
