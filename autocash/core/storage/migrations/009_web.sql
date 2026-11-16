-- Stage 5: the web admin (FT-13.5). Users log in with a password set by the CLI utility
-- (autocash.cli.webuser); rights come from the configuration (users + permissions).
CREATE TABLE web_users (
    tg_id         bigint PRIMARY KEY,
    password_hash text        NOT NULL,
    disabled      boolean     NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE web_sessions (
    token_hash text PRIMARY KEY,           -- sha256 of the cookie value: a leaked table opens nothing
    tg_id      bigint      NOT NULL REFERENCES web_users (tg_id) ON DELETE CASCADE,
    csrf       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX web_sessions_expires ON web_sessions (expires_at);

-- Every change made through the admin, with its author (FT-13.5, item 5).
CREATE TABLE admin_audit (
    id         bigserial PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT now(),
    author     bigint      NOT NULL,
    action     text        NOT NULL,
    target     text,
    ok         boolean     NOT NULL,
    detail     jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX admin_audit_at ON admin_audit (at DESC);

-- Changes of the configuration: who, from which version to which, what (the YAML diff).
CREATE TABLE config_changes (
    id             bigserial PRIMARY KEY,
    at             timestamptz NOT NULL DEFAULT now(),
    author         bigint      NOT NULL,
    version_before text        NOT NULL,
    version_after  text        NOT NULL,
    summary        text        NOT NULL,
    files          text[]      NOT NULL,
    diff           text        NOT NULL,
    git_commit     text
);
