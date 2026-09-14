CREATE TABLE workflow_instances (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  input TEXT NOT NULL,
  workflow_version_at_creation BIGINT NOT NULL,
  generation BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_at TIMESTAMPTZ,
  times_executed INT NOT NULL DEFAULT 0,
  terminal_state TEXT,
  terminal_outcome TEXT,
  cancel_requested_at TIMESTAMPTZ,
  lease_owner TEXT,
  fencing_token BIGINT NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ,
  is_accepting_signals BOOLEAN NOT NULL DEFAULT true,
  parent_workflow_id TEXT,
  parent_instance_key TEXT,
  parent_scope TEXT,
  parent_close_policy TEXT,
  inherit_signals TEXT,
  inherit_past_events BOOLEAN NOT NULL DEFAULT false,
  inherited_events_start_sequence_id BIGINT,
  PRIMARY KEY (workflow_id, key, scope)
);

CREATE TABLE workflow_events (
  sequence_id BIGINT PRIMARY KEY,
  event_kind TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  event_key TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SEQUENCE workflow_event_sequence AS BIGINT CACHE 1;

CREATE UNIQUE INDEX workflow_events_timer_unique
  ON workflow_events (workflow_id, key, scope, event_key)
  WHERE event_kind = 'TimerFired';

CREATE INDEX workflow_events_lookup
  ON workflow_events (event_kind, event_key, workflow_id, key, scope, sequence_id);

CREATE TABLE workflow_steps (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  step_id TEXT NOT NULL,
  step_scope_path TEXT NOT NULL DEFAULT '',
  step_version BIGINT NOT NULL,
  step_kind TEXT NOT NULL,
  state_kind TEXT NOT NULL,
  state_payload TEXT NOT NULL,
  input_fingerprints TEXT NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, key, scope, step_id, step_version, step_scope_path),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE TABLE signal_cursor (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  signal_key TEXT NOT NULL,
  sequence_id BIGINT NOT NULL,
  PRIMARY KEY (workflow_id, key, scope, signal_key),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_signal_subscriptions (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  step_id TEXT NOT NULL,
  step_scope_path TEXT NOT NULL DEFAULT '',
  step_version BIGINT NOT NULL,
  leaf_idx INT NOT NULL,
  signal_key TEXT NOT NULL,
  PRIMARY KEY (
    workflow_id, key, scope, step_id, step_version, leaf_idx, signal_key, step_scope_path
  ),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_timer_subscriptions (
  subscription_id UUID PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  step_id TEXT NOT NULL,
  step_scope_path TEXT NOT NULL DEFAULT '',
  step_version BIGINT NOT NULL,
  leaf_idx INT NOT NULL,
  deadline TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE,
  UNIQUE (workflow_id, key, scope, step_id, step_version, leaf_idx, step_scope_path)
);

CREATE INDEX workflow_timer_subscriptions_deadline
  ON workflow_timer_subscriptions (deadline);

CREATE TABLE workflow_completion_subscriptions (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  step_id TEXT NOT NULL,
  step_scope_path TEXT NOT NULL DEFAULT '',
  step_version BIGINT NOT NULL,
  leaf_idx INT NOT NULL,
  completed_workflow_id TEXT NOT NULL,
  completed_key TEXT NOT NULL,
  completed_scope TEXT NOT NULL,
  PRIMARY KEY (
    workflow_id, key, scope, step_id, step_version, leaf_idx,
    completed_workflow_id, completed_key, completed_scope, step_scope_path
  ),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_wakeups (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  scheduled_at TIMESTAMPTZ NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  PRIMARY KEY (workflow_id, key, scope),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_updates (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  update_key TEXT NOT NULL,
  encoded_input TEXT NOT NULL,
  idempotency_key TEXT NOT NULL DEFAULT '',
  result TEXT,
  handled_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, key, scope, update_key, idempotency_key, created_at),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX workflow_updates_idempotency_unique
  ON workflow_updates (workflow_id, key, scope, update_key, idempotency_key)
  WHERE idempotency_key <> '';

CREATE INDEX workflow_updates_await
  ON workflow_updates (workflow_id, key, scope, update_key, created_at)
  WHERE handled_at IS NULL;

CREATE TABLE workflow_update_subscriptions (
  workflow_id TEXT NOT NULL,
  key TEXT NOT NULL,
  scope TEXT NOT NULL,
  step_id TEXT NOT NULL,
  step_scope_path TEXT NOT NULL DEFAULT '',
  step_version BIGINT NOT NULL,
  leaf_idx INT NOT NULL,
  update_key TEXT NOT NULL,
  PRIMARY KEY (
    workflow_id, key, scope, step_id, step_version, leaf_idx, update_key, step_scope_path
  ),
  FOREIGN KEY (workflow_id, key, scope)
    REFERENCES workflow_instances (workflow_id, key, scope)
    ON DELETE CASCADE
);
