-- Module "players": its own tables (applied by the core when the module is connected).

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
