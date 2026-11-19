-- TS: tickets of players and the messages they are made of (TS_readme.md, D-TS-1..3).
-- A player is a user of Role Manager (rm_users.tg_id); TS keeps no list of its own.

-- A ticket: the player's messages from the first one on. For now a ticket is never closed: finalization
-- comes later, and these rows are to be cleaned then (customer, 2026-11-19).
-- ticket: NULL during the first `burst_s`; D / WW at the first message with triggers of one kind;
-- Q at a message with both kinds (mixed) or when the time ran out without a trigger (none).
CREATE TABLE ts_tickets (
    id              bigserial   PRIMARY KEY,
    tg_id           bigint      NOT NULL,          -- the player (rm_users.tg_id)
    account_id      bigint      NOT NULL,          -- the pool account of the dialog
    state           text        NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'closed')),
    ticket          text        CHECK (ticket IN ('D', 'WW', 'Q')),
    reason          text        CHECK (reason IN ('triggers', 'mixed', 'none')),
    trigger_rules   text[]      NOT NULL DEFAULT '{}',  -- the rules of the message that typed the ticket
    trigger_text    text,                          -- what fired them: "пополнил", "PDF", "tfVQ"
    trigger_msg_id  bigint,
    messages        int         NOT NULL DEFAULT 0,
    first_msg_id    bigint      NOT NULL,
    opened_at       timestamptz NOT NULL,          -- the date of the first message
    decide_by       timestamptz NOT NULL,          -- opened_at + burst_s: no trigger by then -> Q
    classified_at   timestamptz,
    last_msg_at     timestamptz NOT NULL,
    closed_at       timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX ts_tickets_one_open ON ts_tickets (tg_id) WHERE state = 'open';
CREATE INDEX ts_tickets_to_decide ON ts_tickets (decide_by) WHERE ticket IS NULL;
CREATE INDEX ts_tickets_opened ON ts_tickets (opened_at);

-- The messages of a ticket and the rules each fired (no text: it stays in `messages` under its retention).
-- Private message ids are unique per account, so (account_id, msg_id) is the message; a repeat is a no-op.
CREATE TABLE ts_ticket_messages (
    account_id bigint      NOT NULL,
    msg_id     bigint      NOT NULL,
    ticket_id  bigint      NOT NULL REFERENCES ts_tickets (id),
    msg_date   timestamptz NOT NULL,
    media_type text,
    d_rules    text[]      NOT NULL DEFAULT '{}',
    w_rules    text[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (account_id, msg_id)
);
CREATE INDEX ts_ticket_messages_ticket ON ts_ticket_messages (ticket_id, msg_date);
