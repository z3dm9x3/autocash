-- Stage 4: loading the history of chats (FT-2.8). Progress per chat survives restarts.
CREATE TABLE backfill_jobs (
    chat_id    bigint PRIMARY KEY,
    status     text        NOT NULL CHECK (status IN ('running', 'paused', 'done', 'error')),
    since      timestamptz NOT NULL,             -- load messages not older than this
    offset_id  bigint      NOT NULL DEFAULT 0,   -- the last loaded id; next: newer ones (0: from `since`)
    loaded     bigint      NOT NULL DEFAULT 0,
    last_error text,
    started_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    done_at    timestamptz
);
