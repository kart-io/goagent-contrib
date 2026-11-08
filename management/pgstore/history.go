package pgstore

import (
	"encoding/json"
	"github.com/kart-io/goagent/core/session"
	"time"
)

func sessionEvent(seq int64, kind string, at time.Time, data json.RawMessage) session.Event {
	return session.Event{Seq: seq, Type: kind, Time: at, Data: data}
}
