package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInvalidRequestsNeverReachStorage(t *testing.T) {
	h := Handler{Keys: map[string]string{"test-token": "tenant"}, Ping: func(context.Context) error { return errors.New("offline") }}.Routes()
	for _, tc := range []struct {
		name, method, path, token, key, media, body string
		status                                      int
	}{
		{"unauthorized", "POST", "/v1/events", "", "key", "application/json", `{}`, 401},
		{"wrong-token", "POST", "/v1/events", "wrong", "key", "application/json", `{}`, 401},
		{"missing-key", "POST", "/v1/events", "test-token", "", "application/json", `{}`, 400},
		{"bad-media", "POST", "/v1/events", "test-token", "key", "text/plain", `{}`, 415},
		{"unknown-field", "POST", "/v1/events", "test-token", "key", "application/json", `{"type":"x","payload":{},"tenant":"victim"}`, 400},
		{"trailing-json", "POST", "/v1/events", "test-token", "key", "application/json", `{"type":"x","payload":{}} {}`, 400},
		{"null-payload", "POST", "/v1/events", "test-token", "key", "application/json", `{"type":"x","payload":null}`, 422},
		{"array-payload", "POST", "/v1/events", "test-token", "key", "application/json", `{"type":"x","payload":[]}`, 422},
		{"invalid-type", "POST", "/v1/events", "test-token", "key", "application/json", `{"type":"BAD TYPE","payload":{}}`, 422},
		{"too-large", "POST", "/v1/events", "test-token", "key", "application/json", strings.Repeat("x", 65537), 413},
		{"invalid-id", "GET", "/v1/events/bad", "test-token", "", "", "", 404},
		{"method", "DELETE", "/v1/events", "test-token", "", "", "", 405},
		{"health", "GET", "/healthz", "", "", "", "", 200},
		{"readiness-fails", "GET", "/readyz", "", "", "", "", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			r.Header.Set("Idempotency-Key", tc.key)
			r.Header.Set("Content-Type", tc.media)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
			if tc.status >= 400 && w.Header().Get("Content-Type") != "application/problem+json" {
				t.Fatal("missing problem details")
			}
		})
	}
}
