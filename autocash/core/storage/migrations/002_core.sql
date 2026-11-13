-- Core tables (FT-5.5). Pool and membership tables are created now and filled by stage 1b.

CREATE TABLE accounts (
    account_id  bigint PRIMARY KEY,
    name        text        NOT NULL,
    kind        text        NOT NULL CHECK (kind IN ('user', 'bot')),
    state       text        NOT NULL DEFAULT 'disconnected'
                CHECK (state IN ('active', 'paused', 'restricted', 'disconnected', 'lost', 'removed')),
    state_since timestamptz NOT NULL DEFAULT now(),
    added_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE account_state_log (
    id         bigserial PRIMARY KEY,
    account_id bigint      NOT NULL,
    old_state  text,
    new_state  text        NOT NULL,
    reason     text,
    at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE flood_pauses (
    account_id   bigint      NOT NULL,
    method       text        NOT NULL DEFAULT '',  -- '' = whole account
    paused_until timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, method)
);

CREATE TABLE chat_membership (
    chat_id    bigint      NOT NULL,
    account_id bigint      NOT NULL,
    can_send   boolean     NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, account_id)
);

CREATE TABLE duty_assignments (
    duty       text        NOT NULL,
    chat_id    bigint      NOT NULL,
    account_id bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (duty, chat_id)
);

-- Chats seen by the pool, including ones without configuration (policy presence / new chats).
CREATE TABLE sources (
    chat_id      bigint PRIMARY KEY,
    kind         text,
    title        text,
    roles        text[]      NOT NULL DEFAULT '{}',
    policy       text,
    dedup_approx boolean     NOT NULL DEFAULT false,
    known        boolean     NOT NULL DEFAULT false,
    first_seen   timestamptz NOT NULL DEFAULT now(),
    last_seen    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tg_users (
    user_id    bigint PRIMARY KEY,
    username   text,
    name       text,
    first_seen timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tg_user_names (
    id       bigserial PRIMARY KEY,
    user_id  bigint      NOT NULL,
    username text,
    name     text,
    seen_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX tg_user_names_user ON tg_user_names (user_id, seen_at);

-- chat_id = 0: global role (FT-12.1).
CREATE TABLE staff_roles (
    user_id bigint NOT NULL,
    role    text   NOT NULL,
    chat_id bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, role, chat_id)
);

-- Shared entity store: a new entity type needs no migration (FT-5.6).
CREATE TABLE entities (
    entity_type text        NOT NULL,
    entity_key  text        NOT NULL,
    state       text,
    attrs       jsonb       NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_key)
);

CREATE TABLE entity_links (
    from_type  text        NOT NULL,
    from_key   text        NOT NULL,
    to_type    text        NOT NULL,
    to_key     text        NOT NULL,
    relation   text        NOT NULL DEFAULT '',
    attrs      jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (from_type, from_key, to_type, to_key, relation)
);
CREATE INDEX entity_links_to ON entity_links (to_type, to_key);

CREATE TABLE dialog_states (
    account_id bigint      NOT NULL,
    chat_id    bigint      NOT NULL,
    state      text        NOT NULL,
    data       jsonb       NOT NULL DEFAULT '{}',
    expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, chat_id)
);

CREATE TABLE config_versions (
    version   text PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    content   jsonb       NOT NULL
);

CREATE TABLE unknown_counters (
    chat_id bigint NOT NULL,
    day     date   NOT NULL,
    count   bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, day)
);
