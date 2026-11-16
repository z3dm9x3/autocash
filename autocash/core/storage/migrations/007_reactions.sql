-- Stage 3: timers of the action `schedule` (survive a restart) and the human-pause lookup.

CREATE TABLE timers (
    id         bigserial PRIMARY KEY,
    dedup_key  text        NOT NULL UNIQUE,     -- event + rule + timer: a repeat schedules nothing
    timer      text        NOT NULL,
    fire_at    timestamptz NOT NULL,
    payload    jsonb       NOT NULL DEFAULT '{}',
    trace_id   uuid,
    rule       text,
    created_at timestamptz NOT NULL DEFAULT now(),
    fired_at   timestamptz
);
CREATE INDEX timers_due ON timers (fire_at) WHERE fired_at IS NULL;

-- FT-8.10: "no message of a staff member in this dialog for N minutes".
CREATE INDEX messages_human ON messages (chat_id, msg_date) WHERE origin = 'human';
-- Limits of automatic replies per dialog and per account (NFT-12).
CREATE INDEX outbound_module_chat ON outbound_tasks (module, chat_id, created_at);
