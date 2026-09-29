package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/muhammad-musa-ml/dispatch-api/internal/events"
)

type Repository interface {
	Create(context.Context, string, string, string, string, json.RawMessage) (events.Event, bool, error)
	Get(context.Context, string, string) (events.Event, error)
	Attempts(context.Context, string, string) ([]events.Attempt, error)
}

type Handler struct {
	Store Repository
	Keys  map[string]string // token -> tenant, supplied by the operator
	Ping  func(context.Context) error
}

var eventType = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,99}$`)
var eventID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var keyPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

func (h Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := h.Ping(ctx); err != nil {
			problem(w, 503, "database_unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			problem(w, 405, "method_not_allowed")
			return
		}
		h.create(w, r)
	})
	mux.HandleFunc("/v1/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			problem(w, 405, "method_not_allowed")
			return
		}
		h.get(w, r)
	})
	mux.HandleFunc("/v1/events/{id}/attempts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			problem(w, 405, "method_not_allowed")
			return
		}
		tenant, ok := h.tenant(w, r)
		if !ok {
			return
		}
		id := r.PathValue("id")
		if !eventID.MatchString(id) {
			problem(w, 404, "not_found")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		attempts, err := h.Store.Attempts(ctx, tenant, id)
		if errors.Is(err, pgx.ErrNoRows) {
			problem(w, 404, "not_found")
			return
		}
		if err != nil {
			problem(w, 503, "storage_unavailable")
			return
		}
		respond(w, 200, map[string]any{"data": attempts})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { problem(w, 404, "not_found") })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func (h Handler) tenant(w http.ResponseWriter, r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		w.Header().Set("WWW-Authenticate", "Bearer")
		problem(w, 401, "unauthorized")
		return "", false
	}
	got := sha256.Sum256([]byte(strings.TrimPrefix(header, "Bearer ")))
	tenant := ""
	for token, owner := range h.Keys {
		expected := sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(got[:], expected[:]) == 1 {
			tenant = owner
		}
	}
	if tenant == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		problem(w, 401, "unauthorized")
		return "", false
	}
	return tenant, true
}

func (h Handler) create(w http.ResponseWriter, r *http.Request) {
	tenant, ok := h.tenant(w, r)
	if !ok {
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "unsupported_media_type")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if !keyPattern.MatchString(key) {
		problem(w, 400, "invalid_idempotency_key")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			problem(w, 413, "body_too_large")
		} else {
			problem(w, 400, "invalid_body")
		}
		return
	}
	var input struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, 400, "invalid_json")
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		problem(w, 400, "invalid_json")
		return
	}
	if !eventType.MatchString(input.Type) || len(input.Payload) == 0 || bytes.TrimSpace(input.Payload)[0] != '{' {
		problem(w, 422, "invalid_event")
		return
	}
	hash := sha256.Sum256(body)
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	e, created, err := h.Store.Create(ctx, tenant, key, hex.EncodeToString(hash[:]), input.Type, input.Payload)
	if errors.Is(err, events.ErrConflict) {
		problem(w, 409, "idempotency_conflict")
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "22P05" || pgErr.Code == "22003" || pgErr.Code == "22P02") {
		problem(w, 422, "unsupported_payload")
		return
	}
	if err != nil {
		problem(w, 503, "storage_unavailable")
		return
	}
	w.Header().Set("Location", "/v1/events/"+e.ID)
	if created {
		respond(w, 201, e)
	} else {
		w.Header().Set("Idempotency-Replayed", "true")
		respond(w, 200, e)
	}
}

func (h Handler) get(w http.ResponseWriter, r *http.Request) {
	tenant, ok := h.tenant(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if !eventID.MatchString(id) {
		problem(w, 404, "not_found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	e, err := h.Store.Get(ctx, tenant, id)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, 404, "not_found")
		return
	}
	if err != nil {
		problem(w, 503, "storage_unavailable")
		return
	}
	respond(w, 200, e)
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func problem(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": http.StatusText(status), "status": status, "code": code})
}
