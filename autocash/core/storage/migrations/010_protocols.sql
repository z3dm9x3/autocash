-- Protocols: instructions between modules (D-RM-12). A run is written in the same transaction as
-- the fact that caused it, once per (protocol, subject); handlers of the modules run after it,
-- in the order of the modules, each until it succeeds.

CREATE TABLE protocol_runs (
    id           bigserial PRIMARY KEY,
    protocol     text        NOT NULL,                 -- e.g. contact.first, contact.second
    subject      bigint      NOT NULL,                 -- e.g. the Telegram id of the user
    data         jsonb       NOT NULL DEFAULT '{}',
    status       text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done', 'failed')),
    done_modules text[]      NOT NULL DEFAULT '{}',
    attempts     int         NOT NULL DEFAULT 0,
    last_error   text,
    next_at      timestamptz NOT NULL DEFAULT now(),
    created_at   timestamptz NOT NULL DEFAULT now(),
    done_at      timestamptz,
    UNIQUE (protocol, subject)
);
CREATE INDEX protocol_runs_pending ON protocol_runs (next_at) WHERE status = 'pending';
