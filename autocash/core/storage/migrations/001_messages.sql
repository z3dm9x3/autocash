-- Messages, partitioned by month of the message date (FT-5.1, FT-5.4).
-- scope_id = 0 for supergroups/channels (message ids are shared by all accounts),
-- scope_id = account id for private chats and basic groups (ids are per account).
-- version = 0 for a new message, unix time of edit_date for an edit (see docs/decisions.md).
CREATE SEQUENCE messages_ingest_seq;

CREATE TABLE messages (
    ingest_id        bigint      NOT NULL DEFAULT nextval('messages_ingest_seq'),
    scope_id         bigint      NOT NULL,
    chat_id          bigint      NOT NULL,
    msg_id           bigint      NOT NULL,
    version          bigint      NOT NULL,
    msg_date         timestamptz NOT NULL,
    trace_id         uuid        NOT NULL,
    account_id       bigint      NOT NULL,
    event            text        NOT NULL,
    source           text        NOT NULL DEFAULT 'live',
    policy           text        NOT NULL,
    origin           text        NOT NULL,
    sender_id        bigint,
    sender_role      text,
    can_command      boolean     NOT NULL DEFAULT true,
    source_roles     text[]      NOT NULL DEFAULT '{}',
    src_priority     int         NOT NULL DEFAULT 5,
    edit_date        timestamptz,
    received_at      timestamptz NOT NULL,
    reply_to         bigint,
    media_type       text,
    media_ref        text,
    text             text,
    text_hash        text,
    features         jsonb,
    is_command       boolean     NOT NULL DEFAULT false,
    manifest_version text,
    config_version   text,
    dedup_key        text,
    dedup_exact      boolean     NOT NULL DEFAULT true,
    msg_type         text,
    fields           jsonb,
    order_key        text,
    priority         int,
    lane             smallint,
    rule_id          text,
    text_expires_at  timestamptz,
    sorted_at        timestamptz,
    deleted_at       timestamptz,
    PRIMARY KEY (scope_id, chat_id, msg_id, version, msg_date)
) PARTITION BY RANGE (msg_date);

CREATE INDEX messages_ingest ON messages (ingest_id);
CREATE INDEX messages_unsorted ON messages (ingest_id) WHERE sorted_at IS NULL;
CREATE INDEX messages_dedup ON messages (chat_id, dedup_key) WHERE NOT dedup_exact;
CREATE INDEX messages_scope_msg ON messages (scope_id, msg_id);
CREATE INDEX messages_trace ON messages (trace_id);
CREATE INDEX messages_text_ttl ON messages (text_expires_at) WHERE text IS NOT NULL;

-- "Seen" marks: which pool accounts received the message (FT-2.2).
CREATE TABLE message_seen (
    scope_id   bigint      NOT NULL,
    chat_id    bigint      NOT NULL,
    msg_id     bigint      NOT NULL,
    version    bigint      NOT NULL,
    account_id bigint      NOT NULL,
    seen_at    timestamptz NOT NULL,
    PRIMARY KEY (scope_id, chat_id, msg_id, version, account_id)
);

-- chat_id = 0 when Telegram did not report the chat of a deletion.
CREATE TABLE message_deletions (
    scope_id   bigint      NOT NULL,
    chat_id    bigint      NOT NULL,
    msg_id     bigint      NOT NULL,
    account_id bigint      NOT NULL,
    trace_id   uuid        NOT NULL,
    deleted_at timestamptz NOT NULL,
    PRIMARY KEY (scope_id, chat_id, msg_id)
);
