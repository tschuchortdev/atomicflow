-- Stores signal values for a workflow instance
CREATE TABLE workflow_signals
(
    id                   UUID        NOT NULL,
    workflow_id          UUID        NOT NULL,
    workflow_instance_id UUID        NOT NULL REFERENCES workflow_instance (id) ON DELETE RESTRICT,
    value                BYTEA       NOT NULL,
    expiry               TIMESTAMPTZ, -- null means never expires
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, workflow_id, workflow_instance_id)
);
