-- Traces, outbound tasks, rule log, processing marks (FT-5.5, section 4.11).

CREATE TABLE reason_codes (
    code    text PRIMARY KEY,
    meaning text NOT NULL,
    status  text NOT NULL
);

-- One row per inbound task with the timestamps of its stages (FT-11.2, FT-11.11).
CREATE TABLE trace_inbound (
    trace_id       uuid PRIMARY KEY,
    ingest_id      bigint,
    account_id     bigint      NOT NULL,
    chat_id        bigint,
    msg_id         bigint,
    version        bigint,
    event          text        NOT NULL,
    accepted_at    timestamptz NOT NULL,
    extracted_at   timestamptz,
    buffered_at    timestamptz,
    in_pg_at       timestamptz,
    sorted_at      timestamptz,
    finished_at    timestamptz,
    msg_type       text,
    status         text        NOT NULL DEFAULT 'in_progress',
    reason_code    text,
    wait_until     timestamptz,
    waited_reason  text,       -- last reason the task was waiting for (e.g. DB_UNAVAILABLE)
    subscribers    int,
    config_version text
);
CREATE INDEX trace_inbound_open ON trace_inbound (accepted_at) WHERE finished_at IS NULL;
CREATE INDEX trace_inbound_msg ON trace_inbound (chat_id, msg_id);

-- One row per processing of a task by a subscribed module.
CREATE TABLE trace_module (
    trace_id    uuid        NOT NULL,
    module      text        NOT NULL,
    ingest_id   bigint,
    queued_at   timestamptz,
    started_at  timestamptz,
    finished_at timestamptz,
    status      text        NOT NULL DEFAULT 'in_progress',
    reason_code text,
    error       text,
    PRIMARY KEY (trace_id, module)
);
CREATE INDEX trace_module_open ON trace_module (module, queued_at) WHERE finished_at IS NULL;

-- Mark of a module: every message with ingest_id <= last_ingest_id is finished (FT-7.7).
CREATE TABLE module_checkpoints (
    module         text PRIMARY KEY,
    last_ingest_id bigint      NOT NULL DEFAULT 0,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbound_tasks (
    id               bigserial PRIMARY KEY,
    trace_id         uuid        NOT NULL UNIQUE,
    parent_trace_id  uuid,
    dedup_key        text        NOT NULL UNIQUE,  -- "event + rule" (FT-8.3)
    module           text,
    rule             text,
    rule_version     text,
    kind             text        NOT NULL,
    priority         int         NOT NULL,
    chat_id          bigint      NOT NULL,
    reply_to         bigint,
    text             text        NOT NULL,
    random_id        bigint      NOT NULL,           -- generated and stored before sending (FT-9.3)
    account_id       bigint,
    stage            text        NOT NULL DEFAULT 'created',
    status           text        NOT NULL DEFAULT 'in_progress',
    reason_code      text,
    last_wait_reason text,
    wait_until       timestamptz,
    attempts         int         NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now(),
    queued_at        timestamptz,
    assigned_at      timestamptz,
    sending_at       timestamptz,
    finished_at      timestamptz,
    expires_at       timestamptz,
    delivered_msg_id bigint,
    config_version   text
);
CREATE INDEX outbound_open ON outbound_tasks (priority, id) WHERE finished_at IS NULL;
CREATE INDEX outbound_parent ON outbound_tasks (parent_trace_id);

CREATE TABLE outbound_attempts (
    id         bigserial PRIMARY KEY,
    task_id    bigint      NOT NULL REFERENCES outbound_tasks (id) ON DELETE CASCADE,
    account_id bigint,
    at         timestamptz NOT NULL DEFAULT now(),
    result     text        NOT NULL,
    error_code text
);
CREATE INDEX outbound_attempts_task ON outbound_attempts (task_id);

CREATE TABLE rule_log (
    id             bigserial PRIMARY KEY,
    at             timestamptz NOT NULL DEFAULT now(),
    trace_id       uuid,
    module         text,
    rule           text        NOT NULL,
    rule_version   text,
    config_version text,
    input          jsonb,
    actions        jsonb,
    tasks          jsonb
);
CREATE INDEX rule_log_trace ON rule_log (trace_id);
