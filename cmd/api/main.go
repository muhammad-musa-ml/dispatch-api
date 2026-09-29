package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/muhammad-musa-ml/dispatch-api/internal/api"
	"github.com/muhammad-musa-ml/dispatch-api/internal/delivery"
	"github.com/muhammad-musa-ml/dispatch-api/internal/events"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var keys map[string]string
	if err := json.Unmarshal([]byte(os.Getenv("API_KEYS")), &keys); err != nil || len(keys) == 0 {
		return errors.New("API_KEYS must be a nonempty JSON token-to-tenant object")
	}
	for token, tenant := range keys {
		if len(token) < 24 || tenant == "" {
			return errors.New("tokens need at least 24 characters and a nonempty tenant")
		}
	}
	var targets map[string]delivery.Target
	if err := json.Unmarshal([]byte(os.Getenv("DELIVERY_TARGETS")), &targets); err != nil {
		return errors.New("invalid DELIVERY_TARGETS JSON")
	}
	if err := delivery.ValidateTargets(targets, os.Getenv("ALLOW_HTTP_TARGETS") == "1"); err != nil {
		return err
	}
	for _, tenant := range keys {
		if _, ok := targets[tenant]; !ok {
			return errors.New("every API tenant needs a delivery target")
		}
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return errors.New("database connection failed")
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	h := api.Handler{Store: events.Store{Pool: pool}, Keys: keys, Ping: pool.Ping}
	srv := &http.Server{Addr: addr, Handler: h.Routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	stop, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); delivery.New(pool, targets).Run(workerCtx, 4) }()
	defer func() { stopWorker(); <-workerDone }()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	slog.Info("listening", "address", addr)
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-stop.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	}
}
