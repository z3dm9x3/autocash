-- Stage 1c: stuck marks, cancel author, hourly trace aggregates (FT-11.7, 11.9, 11.10, 11.11).

ALTER TABLE trace_inbound ADD COLUMN stuck_at timestamptz, ADD COLUMN stuck_stage text;
ALTER TABLE trace_module ADD COLUMN stuck_at timestamptz, ADD COLUMN stuck_stage text,
    ADD COLUMN cancelled_by text;
ALTER TABLE outbound_tasks ADD COLUMN stuck_at timestamptz, ADD COLUMN stuck_stage text,
    ADD COLUMN cancelled_by text;

-- Module processing that failed and waits for /retry or /cancel.
CREATE INDEX trace_module_waiting ON trace_module (module, queued_at) WHERE status = 'waiting';
-- Retention and rollups go by time.
CREATE INDEX trace_inbound_accepted ON trace_inbound (accepted_at);
CREATE INDEX trace_module_queued ON trace_module (queued_at);
CREATE INDEX outbound_created ON outbound_tasks (created_at);

-- Detailed traces live N days; these aggregates stay forever (FT-11.11).
CREATE TABLE trace_metrics_hourly (
    hour      timestamptz NOT NULL,
    task_kind text        NOT NULL,  -- inbound | module | outbound
    stage     text        NOT NULL,
    dim       text        NOT NULL,  -- all | source | module | account
    dim_value text        NOT NULL,
    count     bigint      NOT NULL,
    p50       float8,
    p95       float8,
    max       float8,
    PRIMARY KEY (hour, task_kind, stage, dim, dim_value)
);

CREATE TABLE trace_finals_hourly (
    hour        timestamptz NOT NULL,
    task_kind   text        NOT NULL,
    status      text        NOT NULL,
    reason_code text        NOT NULL,  -- '' when the status needs no reason
    dim         text        NOT NULL,
    dim_value   text        NOT NULL,
    count       bigint      NOT NULL,
    PRIMARY KEY (hour, task_kind, status, reason_code, dim, dim_value)
);
