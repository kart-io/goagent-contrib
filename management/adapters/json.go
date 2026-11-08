package adapters

import (
	"bytes"
	"encoding/json"
)

func bytesReader(raw json.RawMessage) *bytes.Reader { return bytes.NewReader(raw) }
