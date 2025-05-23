-- Stores workflow instances and their locked state (with a timestamp)
CREATE TABLE workflow_instance
(
    id           UUID PRIMARY KEY,
    workflow_id  TEXT        NOT NULL,
    input        BYTEA       NOT NULL,
    locked_until TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stores idempotency IDs for each step within a workflow instance
CREATE TABLE step_idempotency
(
    id                   UUID PRIMARY KEY,
    library_version      BIGINT      NOT NULL,
    workflow_id          TEXT        NOT NULL,
    workflow_instance_id UUID        NOT NULL REFERENCES workflow_instance (id) ON DELETE RESTRICT,
    step_id              TEXT        NOT NULL,
    step_version         BIGINT,
    input_fingerprints   JSONB,
    is_only_once         BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stores output of a step, identified by the step_idempotency_id
CREATE TABLE step_cache
(
    step_idempotency_id UUID PRIMARY KEY REFERENCES step_idempotency (id) ON DELETE CASCADE,
    step_id             UUID        NOT NULL,
    step_version        BIGINT      NOT NULL,
    input_fingerprints  TEXT        NOT NULL,
    output              BYTEA       NOT NULL,
    expiry              TIMESTAMPTZ, -- null means never expires
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
