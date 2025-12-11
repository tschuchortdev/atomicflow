-- Stores workflow instances and their locked state (with a timestamp)
CREATE TABLE workflow_instance
(
    workflow_id  UUID        NOT NULL,
    -- Collation determines the locale of strings. Setting the locale "C" explicitly disables this behaviour so that
    -- efficient prefix searches can be made on the index (all queries have to specify COLLATE "C" as well).
    -- Alternatively, we could create an index with text_pattern_ops, which does not require queries to specify COLLATE "C"
    -- but has the disadvantage of not allowing <, <=, >= queries.
    key          TEXT        NOT NULL COLLATE "C",
    input        BYTEA       NOT NULL,
    locked_until TIMESTAMPTZ DEFAULT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_id, key)
);

CREATE TABLE workflow_result
(
    workflow_id  UUID        NOT NULL,
    workflow_instance_key TEXT        NOT NULL COLLATE "C",
    result                JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);

-- Stores idempotency IDs for each step within a workflow instance
CREATE TABLE step_idempotency
(
    id                   UUID PRIMARY KEY,
    library_version      BIGINT,
    workflow_id          UUID        NOT NULL,
    workflow_instance_key TEXT        NOT NULL COLLATE "C",
    step_id              TEXT        NOT NULL COLLATE "C",
    step_version         BIGINT,
    input_fingerprints   JSONB,
    is_only_once         BOOLEAN     NOT NULL DEFAULT FALSE,
    is_overridden        BOOLEAN     DEFAULT FALSE NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);

-- Stores output of a step, identified by the step_idempotency_id
CREATE TABLE step_cache
(
    step_idempotency_id UUID PRIMARY KEY REFERENCES step_idempotency (id) ON DELETE CASCADE,
    step_id             TEXT        NOT NULL COLLATE "C",
    step_version        BIGINT      NOT NULL,
    input_fingerprints  JSONB       NOT NULL,
    output              BYTEA       NOT NULL,
    expiry              TIMESTAMPTZ, -- null means never expires
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stores signal values for a workflow instance
CREATE TABLE workflow_signals
(
    id                   UUID        NOT NULL,
    workflow_id          UUID        NOT NULL,
    workflow_instance_key TEXT        NOT NULL COLLATE "C",
    value                BYTEA       NOT NULL,
    expiry               TIMESTAMPTZ, -- null means never expires
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_id, workflow_instance_key, id),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);

CREATE TABLE workflows_awaiting_timer
(
    awaiter_id            UUID NOT NULL PRIMARY KEY,
    workflow_id           UUID NOT NULL,
    workflow_instance_key TEXT        NOT NULL COLLATE "C",
    restart_after        TIMESTAMPTZ NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);

CREATE TABLE workflows_awaiting_signal
(
    awaiter_id            UUID NOT NULL PRIMARY KEY,
    workflow_id           UUID NOT NULL,
    workflow_instance_key TEXT        NOT NULL COLLATE "C",
    signal_id             UUID NOT NULL REFERENCES workflow_signals(id) ON DELETE RESTRICT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);

CREATE TABLE workflows_awaiting_workflow
(
    awaiter_id                    UUID        NOT NULL PRIMARY KEY,
    workflow_id                   UUID        NOT NULL,
    workflow_instance_key         TEXT        NOT NULL COLLATE "C",
    awaited_workflow_id           UUID        NOT NULL,
    awaited_workflow_instance_key TEXT        NOT NULL COLLATE "C",
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_id, workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT,
    FOREIGN KEY (awaited_workflow_id, awaited_workflow_instance_key) REFERENCES workflow_instance (workflow_id, key) ON DELETE RESTRICT
);
