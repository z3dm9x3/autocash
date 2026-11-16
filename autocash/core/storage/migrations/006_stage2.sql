-- Stage 2: core actions over entities, modules "players" and "deposits".

-- set_state with a timeout (FT-8.4): when state_until passes, the state becomes timeout_state.
ALTER TABLE entities ADD COLUMN state_until timestamptz, ADD COLUMN timeout_state text;
CREATE INDEX entities_state_until ON entities (state_until) WHERE state_until IS NOT NULL;
CREATE INDEX entity_links_from ON entity_links (from_type, from_key);

-- ---- players (FT-И.1..И.5) ------------------------------------------------------------------
-- A player is a connected group of identifiers (TGID, MelID); player_id is the group's id.
CREATE SEQUENCE players_seq;

CREATE TABLE player_idents (
    kind       text        NOT NULL CHECK (kind IN ('tg', 'mel')),
    value      text        NOT NULL,
    player_id  bigint      NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);
CREATE INDEX player_idents_player ON player_idents (player_id);

-- Every observed TGID <-> MelID pair, with the event that showed it.
CREATE TABLE player_pairs (
    tgid       text        NOT NULL,
    mel_id     text        NOT NULL,
    trace_id   uuid,
    first_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tgid, mel_id)
);

-- Journal of merges (FT-И.3). moved: identifiers that changed player, with their old player_id.
CREATE TABLE player_merges (
    id          bigserial PRIMARY KEY,
    into_player bigint      NOT NULL,
    from_player bigint      NOT NULL,
    reason      jsonb       NOT NULL,
    moved       jsonb       NOT NULL DEFAULT '[]',
    status      text        NOT NULL CHECK (status IN ('done', 'pending', 'rejected', 'split')),
    author      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    decided_at  timestamptz
);
CREATE INDEX player_merges_pending ON player_merges (created_at) WHERE status = 'pending';

CREATE TABLE player_names (
    tgid     text        NOT NULL,
    username text        NOT NULL DEFAULT '',
    name     text        NOT NULL DEFAULT '',
    seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tgid, username, name)
);

-- Sanctions and marks: strike (scam), ban, unban, complaint, request, note (FT-И.5).
CREATE TABLE player_marks (
    id         bigserial PRIMARY KEY,
    kind       text        NOT NULL,
    ident_kind text        NOT NULL,
    ident      text        NOT NULL,
    text       text,
    author     text        NOT NULL,
    trace_id   uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trace_id, kind)
);
CREATE INDEX player_marks_ident ON player_marks (ident_kind, ident);

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
