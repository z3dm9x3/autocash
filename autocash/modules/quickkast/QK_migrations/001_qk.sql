-- QK: QuickKast - templates for triggers of the operator in private dialogs (QK_readme.md, D-QK-1..5).

CREATE TABLE qk_settings (
    id             int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled        boolean NOT NULL DEFAULT true,        -- /qk on | /qk off
    active_profile text    NOT NULL DEFAULT 'standard',  -- /qk set NAME
    updated_by     bigint,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
INSERT INTO qk_settings (id) VALUES (1);

CREATE TABLE qk_profiles (
    name       text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX qk_profiles_name_ci ON qk_profiles (lower(name));
INSERT INTO qk_profiles (name) VALUES ('standard');

CREATE TABLE qk_templates (
    profile    text   NOT NULL REFERENCES qk_profiles (name) ON UPDATE CASCADE ON DELETE CASCADE,
    trigger    text   NOT NULL,
    variant    int    NOT NULL,                          -- 1-based, the N of TRIGGER-N
    template   text   NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (profile, trigger, variant)
);

-- x / х / v in a dialog: QK off / on for that account and chat; no row: on.
CREATE TABLE qk_chat_state (
    account_id bigint  NOT NULL,
    chat_id    bigint  NOT NULL,
    enabled    boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, chat_id)
);

-- One row per fired trigger: the times of the express lane (wall clock, microseconds).
CREATE TABLE qk_events (
    id              bigserial PRIMARY KEY,
    account_id      bigint      NOT NULL,
    chat_id         bigint      NOT NULL,
    trigger_msg_id  bigint      NOT NULL,
    entered         text        NOT NULL,                -- the text of the trigger message
    kind            text        NOT NULL,                -- template / list / missing_variant / chat_off / chat_on
    profile         text,
    trigger         text,
    variant         int,
    msg_date        timestamptz,                         -- the date Telegram gave the trigger (1 s precision)
    received_at     timestamptz NOT NULL,                -- the update reached the Gateway
    matched_at      timestamptz NOT NULL,                -- the express hook resolved it
    stored_at       timestamptz,                         -- the outbound tasks were committed
    deleted_at      timestamptz,                         -- Telegram answered the delete of the trigger
    sent_at         timestamptz,                         -- Telegram answered the send of the template
    delete_task_id  bigint,
    send_task_id    bigint,
    sent_msg_id     bigint,
    status          text        NOT NULL,                -- done / partial / refused / failed
    reason          text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX qk_events_time ON qk_events (received_at);
