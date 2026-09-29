package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/muhammad-musa-ml/dispatch-api/internal/api"
	"github.com/muhammad-musa-ml/dispatch-api/internal/delivery"
	"github.com/muhammad-musa-ml/dispatch-api/internal/events"
)

func setup(t *testing.T) (*pgxpool.Pool, events.Store, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("TEST_DATABASE_URL is required")
		}
		t.Skip("set TEST_DATABASE_URL to run real PostgreSQL tests")
	}
	p, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	tenant := fmt.Sprintf("test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, table := range []string{"delivery_attempts", "deliveries", "events"} {
			if _, err := p.Exec(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1 OR tenant_id=$2", tenant, tenant+"-other"); err != nil {
				t.Error(err)
			}
		}
		p.Close()
	})
	return p, events.Store{Pool: p}, tenant
}

func seed(t *testing.T, s events.Store, tenant, key string) events.Event {
	t.Helper()
	e, _, err := s.Create(context.Background(), tenant, key, "hash-"+key, "note.created", json.RawMessage(`{"note_id":"synthetic-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestConcurrentIdempotencyAndTenantIsolation(t *testing.T) {
	p, s, tenant := setup(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	var created atomic.Int32
	ids := make(chan string, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, new, err := s.Create(ctx, tenant, "same-key", "same-hash", "note.created", json.RawMessage(`{}`))
			if err != nil {
				t.Error(err)
				return
			}
			if new {
				created.Add(1)
			}
			ids <- e.ID
		}()
	}
	wg.Wait()
	close(ids)
	if created.Load() != 1 {
		t.Fatalf("created %d events", created.Load())
	}
	id := ""
	for got := range ids {
		if id != "" && id != got {
			t.Fatal("different IDs for retries")
		}
		id = got
	}
	if _, _, err := s.Create(ctx, tenant, "same-key", "different-hash", "note.created", json.RawMessage(`{}`)); err != events.ErrConflict {
		t.Fatalf("conflict: %v", err)
	}
	other, _, err := s.Create(ctx, tenant+"-other", "same-key", "same-hash", "note.created", json.RawMessage(`{}`))
	if err != nil || other.ID == id {
		t.Fatal("tenant keys were not independent")
	}
	if _, err = s.Get(ctx, tenant+"-other", id); err != pgx.ErrNoRows {
		t.Fatal("cross-tenant read")
	}
	var count int
	if err = p.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE tenant_id=$1`, tenant).Scan(&count); err != nil || count != 1 {
		t.Fatalf("outbox count=%d err=%v", count, err)
	}
}

func TestHTTPContractAndAttemptIsolation(t *testing.T) {
	p, s, tenant := setup(t)
	handler := api.Handler{Store: s, Keys: map[string]string{"alpha": tenant, "bravo": tenant + "-other"}, Ping: p.Ping}.Routes()
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Idempotency-Key", "contract-key")
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	body := `{"type":"note.created","payload":{"id":"synthetic-1"}}`
	first := request("POST", "/v1/events", "alpha", body)
	if first.Code != 201 {
		t.Fatalf("%d %s", first.Code, first.Body)
	}
	var e events.Event
	if err := json.Unmarshal(first.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	if w := request("POST", "/v1/events", "alpha", body); w.Code != 200 || w.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatal("replay contract")
	}
	if w := request("POST", "/v1/events", "alpha", body+" "); w.Code != 409 {
		t.Fatal("byte-different replay must conflict")
	}
	if w := request("POST", "/v1/events", "alpha", `{"type":"note.created","payload":{"null":"\u0000"}}`); w.Code != 422 {
		t.Fatalf("unrepresentable JSONB should be 422, got %d", w.Code)
	}
	for _, suffix := range []string{"", "/attempts"} {
		if w := request("GET", "/v1/events/"+e.ID+suffix, "alpha", ""); w.Code != 200 {
			t.Fatal("owner read")
		}
		if w := request("GET", "/v1/events/"+e.ID+suffix, "bravo", ""); w.Code != 404 {
			t.Fatal("cross-tenant disclosure")
		}
	}
}

func TestExclusiveClaimAndStaleCompletionFence(t *testing.T) {
	p, s, tenant := setup(t)
	e := seed(t, s, tenant, "lease")
	ctx := context.Background()
	w := delivery.New(p, nil)
	jobs := make(chan delivery.Job, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := w.Claim(ctx)
			if err == nil {
				jobs <- j
			} else if err != pgx.ErrNoRows {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(jobs)
	var old delivery.Job
	count := 0
	for j := range jobs {
		old = j
		count++
	}
	if count != 1 {
		t.Fatalf("claimed %d times", count)
	}
	if _, err := p.Exec(ctx, `UPDATE deliveries SET lease_until=now()-interval '1 second' WHERE tenant_id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := w.Claim(ctx)
	if err != nil || reclaimed.Attempt != 2 {
		t.Fatalf("reclaim %v %v", reclaimed, err)
	}
	if err = w.Complete(ctx, old, 200, "delivered", 0); err != nil {
		t.Fatal(err)
	}
	current, _ := s.Get(ctx, tenant, e.ID)
	if current.Status != "accepted" {
		t.Fatal("stale worker changed status")
	}
	if err = w.Complete(ctx, reclaimed, 200, "delivered", 0); err != nil {
		t.Fatal(err)
	}
	current, _ = s.Get(ctx, tenant, e.ID)
	if current.Status != "delivered" {
		t.Fatal("new owner could not complete")
	}
	attempts, err := s.Attempts(ctx, tenant, e.ID)
	if err != nil || len(attempts) != 2 || attempts[0].Outcome != "started" || attempts[1].Outcome != "delivered" {
		t.Fatalf("attempt history: %v %v", attempts, err)
	}
}

func TestSignedRetryThenDelivery(t *testing.T) {
	p, s, tenant := setup(t)
	e := seed(t, s, tenant, "retry")
	ctx := context.Background()
	secret := "a-long-test-signing-secret-12345678"
	var calls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		parts := strings.Split(r.Header.Get("Dispatch-Signature"), ",")
		if len(parts) != 2 {
			t.Error("missing signature")
			w.WriteHeader(401)
			return
		}
		stamp := strings.TrimPrefix(parts[0], "t=")
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(stamp + "."))
		mac.Write(body)
		if strings.TrimPrefix(parts[1], "v1=") != hex.EncodeToString(mac.Sum(nil)) {
			t.Error("bad signature")
		}
		if r.Header.Get("Dispatch-Event-ID") != e.ID {
			t.Error("unstable event ID")
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer receiver.Close()
	w := delivery.New(p, map[string]delivery.Target{tenant: {URL: receiver.URL, Secret: secret}})
	j, err := w.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Process(ctx, j); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Claim(ctx); err != pgx.ErrNoRows {
		t.Fatal("retry ignored due_at")
	}
	if _, err = p.Exec(ctx, `UPDATE deliveries SET due_at=now() WHERE tenant_id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	j, err = w.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Process(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, tenant, e.ID)
	if got.Status != "delivered" || calls.Load() != 2 {
		t.Fatal("delivery failed")
	}
}

func TestPermanentFailureAndRetryExhaustion(t *testing.T) {
	for _, code := range []int{400, 429, 503, 302} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			p, s, tenant := setup(t)
			e := seed(t, s, tenant, "fail")
			ctx := context.Background()
			var calls atomic.Int32
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "http://127.0.0.1:1/should-not-follow")
				w.WriteHeader(code)
			}))
			defer receiver.Close()
			w := delivery.New(p, map[string]delivery.Target{tenant: {URL: receiver.URL, Secret: "a-long-test-signing-secret-12345678"}})
			for range 5 {
				j, err := w.Claim(ctx)
				if err == pgx.ErrNoRows {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if err = w.Process(ctx, j); err != nil {
					t.Fatal(err)
				}
				if _, err = p.Exec(ctx, `UPDATE deliveries SET due_at=now() WHERE tenant_id=$1`, tenant); err != nil {
					t.Fatal(err)
				}
			}
			got, _ := s.Get(ctx, tenant, e.ID)
			if got.Status != "dead_letter" {
				t.Fatal("not terminal")
			}
			want := int32(5)
			if code == 400 || code == 302 {
				want = 1
			}
			if calls.Load() != want {
				t.Fatalf("requests=%d want=%d", calls.Load(), want)
			}
		})
	}
}

func TestFinalLeaseCrashDoesNotSendSixthRequest(t *testing.T) {
	p, s, tenant := setup(t)
	e := seed(t, s, tenant, "last-lease")
	ctx := context.Background()
	w := delivery.New(p, nil)
	if _, err := p.Exec(ctx, `UPDATE deliveries SET attempts=5,lease_until=now()-interval '1 second' WHERE tenant_id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	j, err := w.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Crash again on the bookkeeping claim: it reuses record 6 and stays bounded.
	if _, err = p.Exec(ctx, `UPDATE deliveries SET lease_until=now()-interval '1 second' WHERE tenant_id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	j, err = w.Claim(ctx)
	if err != nil || j.Attempt != 6 {
		t.Fatalf("exhaustion reclaim: %+v %v", j, err)
	}
	if err = w.Process(ctx, j); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, tenant, e.ID)
	if got.Status != "dead_letter" {
		t.Fatal("last lease never terminated")
	}
}

func TestAmbiguousReceiverSuccessNeedsDeduplication(t *testing.T) {
	p, s, tenant := setup(t)
	e := seed(t, s, tenant, "ambiguous")
	ctx := context.Background()
	var mu sync.Mutex
	seen := map[string]bool{}
	effects, calls := 0, 0
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		id := r.Header.Get("Dispatch-Event-ID")
		if !seen[id] {
			seen[id] = true
			effects++
		}
		// The business effect happened, but the sender sees failure.
		if calls == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer receiver.Close()
	w := delivery.New(p, map[string]delivery.Target{tenant: {URL: receiver.URL, Secret: "a-long-test-signing-secret-12345678"}})
	for range 2 {
		j, err := w.Claim(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Process(ctx, j); err != nil {
			t.Fatal(err)
		}
		if _, err = p.Exec(ctx, `UPDATE deliveries SET due_at=now() WHERE tenant_id=$1`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(ctx, tenant, e.ID)
	if err != nil || got.Status != "delivered" {
		t.Fatal("event not delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || effects != 1 {
		t.Fatalf("calls=%d effects=%d", calls, effects)
	}
}
