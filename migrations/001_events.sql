CREATE TABLE events (
    tenant_id text NOT NULL,
    id text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status text NOT NULL DEFAULT 'accepted' CHECK (status IN ('accepted','delivered','dead_letter')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, idempotency_key)
);

-- The event and its pending delivery are inserted in the same transaction.
CREATE TABLE deliveries (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    due_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    completed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id,event_id),
    FOREIGN KEY (tenant_id,event_id) REFERENCES events(tenant_id,id)
);
CREATE INDEX deliveries_due ON deliveries(due_at) WHERE NOT completed;

CREATE TABLE delivery_attempts (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    number integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome text NOT NULL DEFAULT 'started',
    http_status integer NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id,event_id,number),
    FOREIGN KEY (tenant_id,event_id) REFERENCES events(tenant_id,id)
);
