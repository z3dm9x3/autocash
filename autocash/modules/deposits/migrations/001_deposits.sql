-- Module "deposits": its own tables (applied by the core when the module is connected).

-- ---- deposits (FT-З.1..З.11) ---------------------------------------------------------------
CREATE TABLE deposits (
    deposit_id     text PRIMARY KEY,           -- Deposit ID, shared by all kassas (FT-З.3)
    chat_id        bigint      NOT NULL,       -- current kassa
    kassas         jsonb       NOT NULL DEFAULT '[]',  -- history of kassas (FT-З.6)
    request_msg_id bigint,
    tgid           text,
    mel_id         text,
    method         text,
    amount         numeric,
    bonus          numeric,
    total          numeric,
    cashier_id     bigint,
    stage          text        NOT NULL,       -- created | requisites | sent | receipt | final | cancelled
    status         text        NOT NULL DEFAULT 'open',  -- open | success | rejected | cancelled
    stuck          boolean     NOT NULL DEFAULT false,
    marks          jsonb       NOT NULL DEFAULT '[]',     -- OCR warnings and other marks (FT-З.7)
    requisites     jsonb       NOT NULL DEFAULT '[]',     -- masked (invariant 8)
    created_at     timestamptz NOT NULL,
    requisites_at  timestamptz,
    sent_at        timestamptz,
    receipt_at     timestamptz,
    final_at       timestamptz,
    reread_at      timestamptz,                -- last reconciliation re-read (FT-З.10)
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deposits_open ON deposits (chat_id, created_at) WHERE status = 'open';
CREATE INDEX deposits_chat_created ON deposits (chat_id, created_at);
CREATE INDEX deposits_tgid ON deposits (tgid, created_at);
CREATE INDEX deposits_mel ON deposits (mel_id, created_at);

-- Every stage change with the message that caused it; repeat processing adds nothing (FT-8.2).
CREATE TABLE deposit_events (
    deposit_id text        NOT NULL,
    trace_id   uuid        NOT NULL,
    stage      text        NOT NULL,
    chat_id    bigint      NOT NULL,
    msg_id     bigint,
    at         timestamptz NOT NULL,
    data       jsonb       NOT NULL DEFAULT '{}',
    PRIMARY KEY (deposit_id, trace_id, stage)
);

-- Messages that may belong to a deposit not known yet (order across keys is not guaranteed).
CREATE TABLE deposit_pending (
    chat_id    bigint      NOT NULL,
    msg_id     bigint      NOT NULL,
    kind       text        NOT NULL,           -- requisites | sent
    trace_id   uuid        NOT NULL,
    reply_to   bigint,
    tgid       text,
    sender_id  bigint,
    data       jsonb       NOT NULL DEFAULT '{}',
    at         timestamptz NOT NULL,
    used_by    text,
    PRIMARY KEY (chat_id, msg_id, kind)
);
CREATE INDEX deposit_pending_free ON deposit_pending (chat_id, at) WHERE used_by IS NULL;

CREATE TABLE payment_methods (
    chat_id    bigint      NOT NULL,
    name       text        NOT NULL,           -- name without limits (FT-З.2)
    method_id  text,
    enabled    boolean     NOT NULL DEFAULT true,
    min_amount numeric,
    max_amount numeric,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, name)
);

CREATE TABLE payment_method_history (
    id        bigserial PRIMARY KEY,
    chat_id   bigint      NOT NULL,
    method    text        NOT NULL,
    event     text        NOT NULL,            -- on | off | limits
    data      jsonb       NOT NULL DEFAULT '{}',
    trace_id  uuid,
    at        timestamptz NOT NULL,
    UNIQUE (trace_id, event)
);
CREATE INDEX payment_method_history_chat ON payment_method_history (chat_id, method, at);

-- Daily aggregates for reports (FT-З.8, NFT-15): recomputed for the touched day and kassa.
CREATE TABLE deposit_daily (
    day            date    NOT NULL,           -- in report_timezone (NFT-19)
    chat_id        bigint  NOT NULL,
    cashier_id     bigint  NOT NULL DEFAULT 0,  -- 0 = no cashier yet
    method         text    NOT NULL DEFAULT '',
    created        int     NOT NULL,
    success        int     NOT NULL,
    rejected       int     NOT NULL,
    cancelled      int     NOT NULL,
    stuck          int     NOT NULL,
    amount_sum     numeric NOT NULL,
    requisites_n   int     NOT NULL,
    requisites_sec float8  NOT NULL,           -- sum of seconds to requisites
    final_n        int     NOT NULL,
    final_sec      float8  NOT NULL,           -- sum of seconds to the final
    PRIMARY KEY (day, chat_id, cashier_id, method)
);
