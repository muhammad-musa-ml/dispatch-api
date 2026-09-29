// Demo receiver: verifies signatures and fails the first two attempts.
package main

import (
	"crypto/hmac"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/muhammad-musa-ml/dispatch-api/internal/delivery"
)

func main() {
	secret := os.Getenv("WEBHOOK_SECRET")
	if len(secret) < 32 {
		slog.Error("WEBHOOK_SECRET needs 32 characters")
		os.Exit(1)
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536+1024))
		if err != nil {
			http.Error(w, "body", 413)
			return
		}
		parts := strings.Split(r.Header.Get("Dispatch-Signature"), ",")
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") || !strings.HasPrefix(parts[1], "v1=") {
			http.Error(w, "signature", 401)
			return
		}
		ts := strings.TrimPrefix(parts[0], "t=")
		sec, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || sec < time.Now().Add(-5*time.Minute).Unix() || sec > time.Now().Add(5*time.Minute).Unix() {
			http.Error(w, "timestamp", 401)
			return
		}
		got, err := hex.DecodeString(strings.TrimPrefix(parts[1], "v1="))
		expected, _ := hex.DecodeString(delivery.Signature(secret, ts, body))
		if err != nil || !hmac.Equal(got, expected) {
			http.Error(w, "signature", 401)
			return
		}
		id := r.Header.Get("Dispatch-Event-ID")
		attempt, _ := strconv.Atoi(r.Header.Get("Dispatch-Attempt"))
		if attempt <= 2 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "synthetic temporary failure", 503)
			return
		}
		mu.Lock()
		duplicate := seen[id]
		seen[id] = true
		mu.Unlock()
		slog.Info("webhook accepted", "event_id", id, "duplicate", duplicate, "attempt", attempt)
		w.WriteHeader(204)
	})
	srv := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("receiver stopped")
		os.Exit(1)
	}
}
