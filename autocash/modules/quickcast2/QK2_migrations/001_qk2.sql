-- QK2: QuickCast2 (QK2_readme.md, D-QK2-1..4). The templates are those of QuickKast (qk_templates).

CREATE TABLE qk2_settings (
    id          int         PRIMARY KEY CHECK (id = 1),
    enabled     boolean     NOT NULL DEFAULT true,
    updated_by  bigint,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
INSERT INTO qk2_settings (id) VALUES (1);

CREATE TABLE qk2_events (
    id              bigserial PRIMARY KEY,
    account_id      bigint      NOT NULL,
    chat_id         bigint      NOT NULL,
    entered         text        NOT NULL,                -- the draft, with its backslash
    kind            text        NOT NULL,                -- template / list / missing_variant
    profile         text,
    trigger         text,
    variant         int,
    draft_date      timestamptz,                         -- the date Telegram gave the draft (1 s precision)
    received_at     timestamptz NOT NULL,                -- the draft update reached the Gateway
    matched_at      timestamptz NOT NULL,                -- the draft hook resolved it
    stored_at       timestamptz,                         -- the outbound task was committed
    sent_at         timestamptz,                         -- Telegram answered the send (the draft is emptied with it)
    send_task_id    bigint,
    sent_msg_id     bigint,
    status          text        NOT NULL,                -- done / refused / failed
    reason          text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX qk2_events_time ON qk2_events (received_at);
