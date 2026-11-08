package management

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"strings"
	"time"
)

type Service struct{ cfg Config }

func New(c Config) (*Service, error) {
	if c.Store == nil || c.Sessions == nil || c.HistoryReader == nil || c.Authorizer == nil || len(c.Factories) == 0 {
		return nil, ErrInvalid
	}
	defaults := DefaultLimits()
	if c.Limits == (Limits{}) {
		c.Limits = defaults
	}
	if c.Limits.Workers < 1 || c.Limits.MaxPending < 1 || c.Limits.PageSize < 1 || c.Limits.MaxPageSize < c.Limits.PageSize || c.Limits.MaxPageSize > 200 || c.Limits.MaxInputBytes < 1 || c.Limits.MaxOutputBytes < 1 || c.Limits.IdempotencyRetention <= 0 {
		return nil, ErrInvalid
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	for name, f := range c.Factories {
		if name == "" || f == nil {
			return nil, ErrInvalid
		}
	}
	copyFactories := make(map[string]Factory, len(c.Factories))
	for k, v := range c.Factories {
		copyFactories[k] = v
	}
	c.Factories = copyFactories
	return &Service{c}, nil
}
func (s *Service) meta(a Actor) Meta        { return Meta{uuid.NewString(), a.TenantID, 1, s.cfg.Now().UTC()} }
func digest(raw []byte) string              { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func encode(v any) (json.RawMessage, error) { return json.Marshal(v) }
func document(kind string, v any) (Document, error) {
	raw, err := encode(v)
	if err != nil {
		return Document{}, err
	}
	var m Meta
	if err = json.Unmarshal(raw, &m); err != nil {
		return Document{}, err
	}
	return Document{kind, m.TenantID, m.ID, m.Revision, m.CreatedAt, raw}, nil
}
func insert(ctx context.Context, tx Tx, kind string, v any) error {
	d, err := document(kind, v)
	if err != nil {
		return err
	}
	return tx.Insert(ctx, d)
}
func update(ctx context.Context, tx Tx, kind string, v any, expected int64) error {
	d, err := document(kind, v)
	if err != nil {
		return err
	}
	return tx.CAS(ctx, d, expected)
}
func read[T any](ctx context.Context, r Reader, kind, tenant, id string) (v T, err error) {
	d, err := r.Get(ctx, kind, tenant, id)
	if err == nil {
		err = json.Unmarshal(d.Data, &v)
	}
	return
}
func all[T any](ctx context.Context, r Reader, kind, tenant string, q Query) ([]T, error) {
	ds, err := r.List(ctx, kind, Selection{TenantID: tenant, Query: q, Visibility: Visibility{All: true}})
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(ds))
	for _, d := range ds {
		var v T
		if err = json.Unmarshal(d.Data, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *Service) auth(ctx context.Context, a Actor, action, kind, id string) error {
	if err := ValidateActor(a); err != nil {
		return err
	}
	return s.cfg.Authorizer.Authorize(ctx, a, action, ResourceRef{Type: kind, ID: id, TenantID: a.TenantID})
}
func (s *Service) audit(ctx context.Context, tx Tx, a Actor, kind, id, op, reason string) error {
	return insert(ctx, tx, "audit", Audit{Meta: s.meta(a), ResourceType: kind, ResourceID: id, Operation: op, Actor: a, Reason: reason})
}
func mutate[T any](s *Service, ctx context.Context, a Actor, key, op string, request any, fn func(Tx) (T, error)) (out T, err error) {
	if ValidateActor(a) != nil {
		return out, ErrUnauthenticated
	}
	if key == "" || len(key) > 256 {
		return out, ErrInvalid
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return out, ErrInvalid
	}
	raw, err = CanonicalJSON(raw)
	if err != nil {
		return out, err
	}
	hash := digest(raw)
	id := digest([]byte(op + "\x00" + key))
	err = s.cfg.Store.WithinTx(ctx, func(tx Tx) error {
		old, e := read[Idempotency](ctx, tx, "idempotency", a.TenantID, id)
		if e == nil && old.ExpiresAt.After(s.cfg.Now()) {
			if old.Hash != hash {
				return ErrConflict
			}
			return json.Unmarshal(old.Response, &out)
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			return e
		}
		if e == nil {
			if e = tx.Delete(ctx, "idempotency", a.TenantID, id, old.Revision); e != nil {
				return e
			}
		}
		out, e = fn(tx)
		if e != nil {
			return e
		}
		response, e := encode(out)
		if e != nil {
			return e
		}
		m := s.meta(a)
		m.ID = id
		return insert(ctx, tx, "idempotency", Idempotency{Meta: m, Operation: op, Key: key, Hash: hash, Response: response, ExpiresAt: s.cfg.Now().Add(s.cfg.Limits.IdempotencyRetention)})
	})
	return
}

// CanonicalJSON 使用精确数值并拒绝重复键和多份 JSON，作为幂等比较的输入。
func CanonicalJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, ErrInvalid
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	return json.Marshal(v)
}
func decodeValue(d *json.Decoder) (any, error) {
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	switch delim {
	case '{':
		m := map[string]any{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, e
			}
			key, ok := k.(string)
			if !ok {
				return nil, ErrInvalid
			}
			if _, ok = m[key]; ok {
				return nil, ErrInvalid
			}
			m[key], e = decodeValue(d)
			if e != nil {
				return nil, e
			}
		}
		_, err = d.Token()
		return m, err
	case '[':
		a := []any{}
		for d.More() {
			v, e := decodeValue(d)
			if e != nil {
				return nil, e
			}
			a = append(a, v)
		}
		_, err = d.Token()
		return a, err
	}
	return nil, ErrInvalid
}
func validText(s string, max int) bool {
	return s != "" && strings.TrimSpace(s) == s && len([]rune(s)) <= max
}
func versionID(agent, version string) string { return agent + "@" + version }
func (s *Service) command(ctx context.Context, tx Tx, a Actor, kind, task, session string) (Command, error) {
	c := Command{Meta: s.meta(a), Kind: kind, TaskID: task, SessionID: session, State: "pending", Actor: a, AvailableAt: s.cfg.Now().UTC()}
	return c, insert(ctx, tx, "commands", c)
}
func wrapInvalid(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w：%v", ErrInvalid, err)
}
