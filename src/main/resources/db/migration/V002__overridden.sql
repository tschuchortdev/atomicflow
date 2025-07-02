alter table step_idempotency
    add is_overridden boolean default false not null;
