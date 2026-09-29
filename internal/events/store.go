package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("idempotency key already used with a different body")

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
}

type Store struct{ Pool *pgxpool.Pool }

type Attempt struct {
	Number     int        `json:"number"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    string     `json:"outcome"`
	HTTPStatus int        `json:"http_status"`
}

func (s Store) Attempts(ctx context.Context, tenant, id string) ([]Attempt, error) {
	if _, err := s.Get(ctx, tenant, id); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `SELECT number,started_at,finished_at,outcome,http_status FROM delivery_attempts WHERE tenant_id=$1 AND event_id=$2 ORDER BY number`, tenant, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Attempt{}
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.Number, &a.StartedAt, &a.FinishedAt, &a.Outcome, &a.HTTPStatus); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

// Use a separate SELECT so READ COMMITTED can see a competing insert.
// A single INSERT/SELECT CTE can miss the row that caused the conflict.
func (s Store) Create(ctx context.Context, tenant, key, hash, kind string, payload json.RawMessage) (Event, bool, error) {
	var e Event
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return e, false, err
	}
	id := hex.EncodeToString(b)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return e, false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO events (tenant_id,id,idempotency_key,request_hash,type,payload) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,idempotency_key) DO NOTHING`, tenant, id, key, hash, kind, []byte(payload))
	if err != nil {
		return e, false, err
	}
	var storedHash string
	err = tx.QueryRow(ctx, `SELECT id,type,payload,status,created_at,request_hash FROM events WHERE tenant_id=$1 AND idempotency_key=$2`, tenant, key).Scan(&e.ID, &e.Type, &e.Payload, &e.Status, &e.CreatedAt, &storedHash)
	if err != nil {
		return e, false, err
	}
	if storedHash != hash {
		return Event{}, false, ErrConflict
	}
	if tag.RowsAffected() == 1 {
		if _, err = tx.Exec(ctx, `INSERT INTO deliveries (tenant_id,event_id) VALUES ($1,$2)`, tenant, e.ID); err != nil {
			return Event{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Event{}, false, err
	}
	return e, tag.RowsAffected() == 1, nil
}

func (s Store) Get(ctx context.Context, tenant, id string) (Event, error) {
	var e Event
	err := s.Pool.QueryRow(ctx, `SELECT id,type,payload,status,created_at FROM events WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&e.ID, &e.Type, &e.Payload, &e.Status, &e.CreatedAt)
	return e, err
}
