package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/muhammad-musa-ml/dispatch-api/internal/events"
)

const MaxAttempts = 5

type Target struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
}
type Worker struct {
	Pool    *pgxpool.Pool
	Targets map[string]Target
	Client  *http.Client
}
type Job struct {
	Tenant  string
	Event   events.Event
	Attempt int
}

func ValidateTargets(targets map[string]Target, allowHTTP bool) error {
	if len(targets) == 0 {
		return errors.New("DELIVERY_TARGETS is required")
	}
	for tenant, target := range targets {
		u, err := url.Parse(target.URL)
		if err != nil || tenant == "" || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) || len(target.Secret) < 32 {
			return errors.New("targets require a tenant, HTTPS URL without credentials or fragment, and a 32-character secret")
		}
	}
	return nil
}

func New(pool *pgxpool.Pool, targets map[string]Target) *Worker {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // never pass signatures to an environment-configured proxy
	transport.MaxIdleConnsPerHost = 4
	return &Worker{Pool: pool, Targets: targets, Client: &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

// Claim releases the row lock before delivery. The attempt number fences stale workers.
func (w *Worker) Claim(ctx context.Context) (Job, error) {
	var j Job
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return j, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `UPDATE deliveries SET attempts=LEAST(attempts+1,6),lease_until=now()+interval '15 seconds'
WHERE (tenant_id,event_id) = (SELECT tenant_id,event_id FROM deliveries WHERE NOT completed AND due_at<=now() AND (lease_until IS NULL OR lease_until<now()) ORDER BY due_at,tenant_id,event_id FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING tenant_id,event_id,attempts`).Scan(&j.Tenant, &j.Event.ID, &j.Attempt)
	if err != nil {
		return j, err
	}
	err = tx.QueryRow(ctx, `SELECT type,payload,created_at FROM events WHERE tenant_id=$1 AND id=$2`, j.Tenant, j.Event.ID).Scan(&j.Event.Type, &j.Event.Payload, &j.Event.CreatedAt)
	if err != nil {
		return j, err
	}
	// Reuse record 6 if the final bookkeeping attempt crashes.
	_, err = tx.Exec(ctx, `INSERT INTO delivery_attempts (tenant_id,event_id,number) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, j.Tenant, j.Event.ID, j.Attempt)
	if err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}

// Complete updates the event and attempt only if this worker still owns the lease.
func (w *Worker) Complete(ctx context.Context, j Job, status int, outcome string, delay time.Duration) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	terminal := outcome == "delivered" || outcome == "dead_letter"
	tag, err := tx.Exec(ctx, `UPDATE deliveries SET completed=$4,lease_until=NULL,due_at=now()+($5::double precision * interval '1 second') WHERE tenant_id=$1 AND event_id=$2 AND attempts=$3 AND NOT completed`, j.Tenant, j.Event.ID, j.Attempt, terminal, delay.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE delivery_attempts SET finished_at=now(),outcome=$4,http_status=$5 WHERE tenant_id=$1 AND event_id=$2 AND number=$3`, j.Tenant, j.Event.ID, j.Attempt, outcome, status)
	if err != nil {
		return err
	}
	if terminal {
		if _, err = tx.Exec(ctx, `UPDATE events SET status=$3 WHERE tenant_id=$1 AND id=$2`, j.Tenant, j.Event.ID, outcome); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func Signature(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func RetryDelay(attempt int, retryAfter string, now time.Time) time.Duration {
	// Equal jitter over exponential backoff, capped at 30 seconds for the demo.
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		attempt = 5
	}
	base := time.Second * time.Duration(1<<attempt)
	if base > 30*time.Second {
		base = 30 * time.Second
	}
	delay := base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
	if seconds, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && seconds > 0 {
		if seconds > 60 {
			seconds = 60
		}
		if requested := time.Duration(seconds) * time.Second; requested > delay {
			delay = requested
		}
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		requested := date.Sub(now)
		if requested > 60*time.Second {
			requested = 60 * time.Second
		}
		if requested > delay {
			delay = requested
		}
	}
	return delay
}

func (w *Worker) Process(ctx context.Context, j Job) error {
	// Close an expired final lease without sending a sixth request.
	if j.Attempt > MaxAttempts {
		return w.Complete(ctx, j, 0, "dead_letter", 0)
	}
	target, ok := w.Targets[j.Tenant]
	if !ok {
		return w.Complete(ctx, j, 0, "dead_letter", 0)
	}
	body, err := json.Marshal(struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		Payload   json.RawMessage `json:"payload"`
		CreatedAt time.Time       `json:"created_at"`
	}{j.Event.ID, j.Event.Type, j.Event.Payload, j.Event.CreatedAt})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Dispatch-Event-ID", j.Event.ID)
	req.Header.Set("Dispatch-Attempt", strconv.Itoa(j.Attempt))
	req.Header.Set("Dispatch-Signature", "t="+ts+",v1="+Signature(target.Secret, ts, body))
	resp, err := w.Client.Do(req)
	status, retryAfter := 0, ""
	if err == nil {
		status = resp.StatusCode
		retryAfter = resp.Header.Get("Retry-After")
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
	}
	outcome := "retry"
	if status >= 200 && status < 300 {
		outcome = "delivered"
	} else if (status >= 300 && status < 500 && status != 408 && status != 429) || j.Attempt >= MaxAttempts {
		outcome = "dead_letter"
	}
	return w.Complete(ctx, j, status, outcome, RetryDelay(j.Attempt, retryAfter, time.Now()))
}

func (w *Worker) Run(ctx context.Context, concurrency int) {
	var group sync.WaitGroup
	for range concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			for ctx.Err() == nil {
				j, err := w.Claim(ctx)
				if err == nil {
					err = w.Process(ctx, j)
				}
				if err != nil && !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
					slog.Error("delivery iteration failed", "error_type", "database_or_transport")
				}
				if err != nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(250 * time.Millisecond):
					}
				}
			}
		}()
	}
	group.Wait()
	w.Client.CloseIdleConnections()
}
