package integration_test

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	"github.com/kart-io/goagent/codec/json"
	mg "github.com/kart-io/goagent/management"
	"github.com/kart-io/goagent/management/httpapi"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAllManagementRoutes(t *testing.T) {
	f := setup(t, nil)
	f.worker(t)
	h, err := httpapi.New(f.s, httpapi.Config{ActorFromRequest: func(*http.Request) (mg.Actor, error) { return f.actor, nil }})
	require.NoError(t, err)
	server := httptest.NewServer(h)
	defer server.Close()
	request := func(method, path, body string, status int) map[string]any {
		t.Helper()
		req, e := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		require.NoError(t, e)
		req.Header.Set("Idempotency-Key", uuid.NewString())
		response, e := server.Client().Do(req)
		require.NoError(t, e)
		defer response.Body.Close()
		raw, e := io.ReadAll(response.Body)
		require.NoError(t, e)
		require.Equal(t, status, response.StatusCode, string(raw))
		var out map[string]any
		require.NoError(t, json.Unmarshal(raw, &out))
		require.NotContains(t, string(raw), `"execution_key"`)
		return out
	}
	agent := request("POST", "/v1/agents", `{"name":"HTTP客服"}`, 201)
	id := agent["id"].(string)
	request("POST", "/v1/agents/"+id+"/versions", `{"version":"v1","runner_kind":"test","implementation_version":"code-v1","config":{}}`, 201)
	request("POST", "/v1/agents/"+id+"/publish", `{"version":"v1","expected_revision":1}`, 200)
	se := request("POST", "/v1/sessions", `{"agent_id":"`+id+`"}`, 202)
	sid := se["id"].(string)
	require.Eventually(t, func() bool { v, e := f.s.GetSession(f.ctx, f.actor, sid); return e == nil && v.Status == "ready" }, 5*time.Second, 10*time.Millisecond)
	task := request("POST", "/v1/tasks", `{"session_id":"`+sid+`","input":"查询"}`, 202)
	tid := task["id"].(string)
	require.Eventually(t, func() bool { v, e := f.s.GetTask(f.ctx, f.actor, tid); return e == nil && v.State.Terminal() }, 5*time.Second, 10*time.Millisecond)
	for _, path := range []string{"/v1/agents", "/v1/agents/" + id, "/v1/agents/" + id + "/versions", "/v1/sessions", "/v1/sessions/" + sid, "/v1/sessions/" + sid + "/history", "/v1/tasks", "/v1/tasks/" + tid, "/v1/approvals", "/v1/runs", "/v1/runs/" + tid} {
		request("GET", path, "", 200)
	}
	request("POST", "/v1/agents/"+id+"/disable", `{"expected_revision":2}`, 200)
	request("POST", "/v1/agents/"+id+"/enable", `{"expected_revision":3}`, 200)
	request("GET", "/v1/tasks/"+uuid.NewString(), "", 404)
	request("POST", "/v1/tasks", `{"session_id":"`+sid+`","input":1,"actor":"伪造"}`, 400)
	request("POST", "/v1/tasks", strings.Repeat(" ", 1<<20)+`{}`, 413)
}
func TestClientDisconnectDoesNotCancelAcceptedTask(t *testing.T) {
	f := setup(t, nil)
	f.worker(t)
	se := f.session(t)
	h, e := httpapi.New(f.s, httpapi.Config{ActorFromRequest: func(*http.Request) (mg.Actor, error) { return f.actor, nil }})
	require.NoError(t, e)
	ctx, cancel := context.WithCancel(f.ctx)
	raw, _ := json.Marshal(mg.SubmitTaskRequest{SessionID: se.ID, Input: json.RawMessage(`"持久接纳"`)})
	req := httptest.NewRequest("POST", "/v1/tasks", bytes.NewReader(raw)).WithContext(ctx)
	req.Header.Set("Idempotency-Key", uuid.NewString())
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, 202, rr.Code)
	cancel()
	var task mg.Task
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &task))
	require.Eventually(t, func() bool {
		v, e := f.s.GetTask(context.Background(), f.actor, task.ID)
		return e == nil && v.State == mg.Succeeded
	}, 5*time.Second, 10*time.Millisecond)
}
