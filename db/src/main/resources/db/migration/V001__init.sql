CREATE TABLE workflow_instances (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  input VARCHAR NOT NULL,
  workflow_version_at_creation BIGINT NOT NULL,
  generation BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_run_at TIMESTAMPTZ,
  times_executed INT NOT NULL DEFAULT 0,
  terminal_state VARCHAR,
  terminal_outcome VARCHAR,
  cancel_requested_at TIMESTAMPTZ,
  lease_owner VARCHAR,
  fencing_token BIGINT NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ,
  parent_workflow_id VARCHAR,
  parent_instance_key VARCHAR,
  parent_instance_scope VARCHAR[],
  parent_close_policy VARCHAR,
  inherited_signal_prefixes VARCHAR[] NOT NULL DEFAULT '{}',
  inherit_past_events BOOLEAN NOT NULL DEFAULT false,
  inherited_events_start_sequence_id BIGINT,
  PRIMARY KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
);

COMMENT ON COLUMN workflow_instances.inherited_signal_prefixes IS
  'Which signal keys this instance inherits from its parent, as a set of permitted prefixes: an empty array inherits nothing; a single empty-string element inherits every key; any other array inherits a key when it starts with one of the listed prefixes.';

CREATE TABLE child_close_requests (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  target_prefix VARCHAR[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (workflow_id, workflow_instance_key, workflow_instance_scope, target_prefix),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_events (
  sequence_id BIGINT PRIMARY KEY,
  event_kind VARCHAR NOT NULL,
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  event_key VARCHAR NOT NULL,
  payload VARCHAR NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SEQUENCE workflow_event_sequence AS BIGINT CACHE 1;

CREATE UNIQUE INDEX workflow_events_timer_unique
  ON workflow_events (workflow_id, workflow_instance_key, workflow_instance_scope, event_key)
  WHERE event_kind = 'TimerFired';

CREATE INDEX workflow_events_lookup
  ON workflow_events (event_kind, event_key, workflow_id, workflow_instance_key, workflow_instance_scope, sequence_id);

CREATE TABLE workflow_steps (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  step_id VARCHAR NOT NULL,
  step_scope_within_instance VARCHAR[] NOT NULL DEFAULT '{}',
  step_version BIGINT NOT NULL,
  step_kind VARCHAR NOT NULL,
  state_kind VARCHAR NOT NULL,
  state_payload VARCHAR NOT NULL,
  retry_deadline TIMESTAMPTZ,
  input_fingerprints VARCHAR NOT NULL,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, step_scope_within_instance),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

COMMENT ON COLUMN workflow_steps.step_kind IS
  'The step kind that wrote the row: Step, Await, FirstToRun, or RestartableRegion. A step key (step_id, scope, version) belongs to exactly one step kind.';

CREATE TABLE signal_cursor (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  signal_key VARCHAR NOT NULL,
  sequence_id BIGINT NOT NULL,
  PRIMARY KEY (workflow_id, workflow_instance_key, workflow_instance_scope, signal_key),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_signal_subscriptions (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  step_id VARCHAR NOT NULL,
  step_scope_within_instance VARCHAR[] NOT NULL DEFAULT '{}',
  step_version BIGINT NOT NULL,
  subscriber_key VARCHAR NOT NULL,
  signal_key VARCHAR NOT NULL,
  PRIMARY KEY (
    workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, signal_key, step_scope_within_instance
  ),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_timer_subscriptions (
  timer_id UUID PRIMARY KEY,
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  step_id VARCHAR NOT NULL,
  step_scope_within_instance VARCHAR[] NOT NULL DEFAULT '{}',
  step_version BIGINT NOT NULL,
  subscriber_key VARCHAR NOT NULL,
  deadline TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE,
  UNIQUE (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key, step_scope_within_instance)
);

CREATE INDEX workflow_timer_subscriptions_deadline
  ON workflow_timer_subscriptions (deadline);

CREATE TABLE workflow_completion_subscriptions (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  step_id VARCHAR NOT NULL,
  step_scope_within_instance VARCHAR[] NOT NULL DEFAULT '{}',
  step_version BIGINT NOT NULL,
  subscriber_key VARCHAR NOT NULL,
  completed_workflow_id VARCHAR NOT NULL,
  completed_workflow_instance_key VARCHAR NOT NULL,
  completed_workflow_instance_scope VARCHAR[] NOT NULL,
  PRIMARY KEY (
    workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_version, subscriber_key,
    completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope, step_scope_within_instance
  ),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_wakeups (
  workflow_id VARCHAR NOT NULL,
  workflow_instance_key VARCHAR NOT NULL,
  workflow_instance_scope VARCHAR[] NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  scheduled_at TIMESTAMPTZ NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  PRIMARY KEY (workflow_id, workflow_instance_key, workflow_instance_scope),
  FOREIGN KEY (workflow_id, workflow_instance_key, workflow_instance_scope)
    REFERENCES workflow_instances (workflow_id, workflow_instance_key, workflow_instance_scope)
    ON DELETE CASCADE
);

CREATE TABLE workflow_event_dispatch (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  dispatched_through BIGINT NOT NULL
);

INSERT INTO workflow_event_dispatch (id, dispatched_through) VALUES (1, 0);

CREATE INDEX workflow_completion_subscriptions_await
  ON workflow_completion_subscriptions (completed_workflow_id, completed_workflow_instance_key, completed_workflow_instance_scope);

CREATE INDEX workflow_signal_subscriptions_key
  ON workflow_signal_subscriptions (signal_key);

CREATE INDEX workflow_instances_parent
  ON workflow_instances (parent_workflow_id, parent_instance_key, parent_instance_scope);
