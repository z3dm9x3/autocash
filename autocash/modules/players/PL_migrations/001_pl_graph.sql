-- PL: players as a graph of TGID <-> MelID links (PL_readme.md, D-PL-1..4).
-- A link is one observed pair; a player is a connected group of links. There is no size limit: one person
-- may have thousands of MelIDs and TGIDs. Links are never deleted, so any join can be recomputed (split).

CREATE TABLE pl_players (
    player_id     bigserial   PRIMARY KEY,
    created_at    timestamptz NOT NULL DEFAULT now(),
    first_link_id bigint,                         -- the link that made the player (NULL: made by a split)
    merged_into   bigint REFERENCES pl_players (player_id),  -- set when the player was joined into another
    merged_at     timestamptz
);
CREATE INDEX pl_players_merged ON pl_players (merged_into) WHERE merged_into IS NOT NULL;

-- One observed pair. state: active (joins players) | staff (the TGID is staff or a cashier: kept, joins
-- nothing) | disabled (cut by hand). source: who showed the pair (autosup, msg:<type>, rm, manual).
CREATE TABLE pl_links (
    link_id    bigserial   PRIMARY KEY,
    tg_id      bigint      NOT NULL,
    mel_id     text        NOT NULL,
    state      text        NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'staff', 'disabled')),
    source     text        NOT NULL,
    ref        text,                              -- what showed it first: trace id, order id ...
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now(),
    seen_count int         NOT NULL DEFAULT 1,
    changed_by text,                              -- the author of the last state change by hand
    UNIQUE (tg_id, mel_id)
);
CREATE INDEX pl_links_mel ON pl_links (mel_id);

-- An identifier and its current player. Only identifiers of active links are here.
CREATE TABLE pl_nodes (
    kind      text        NOT NULL CHECK (kind IN ('tg', 'mel')),
    value     text        NOT NULL,
    player_id bigint      NOT NULL REFERENCES pl_players (player_id),
    since     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);
CREATE INDEX pl_nodes_player ON pl_nodes (player_id);

-- Journal: join (two players met on a link), split (a cut link broke a player apart), state (a link by hand).
CREATE TABLE pl_journal (
    id          bigserial   PRIMARY KEY,
    kind        text        NOT NULL CHECK (kind IN ('join', 'split', 'state')),
    into_player bigint,
    from_player bigint,
    link_id     bigint,
    moved       int         NOT NULL DEFAULT 0,   -- identifiers that changed player
    data        jsonb       NOT NULL DEFAULT '{}',
    author      text,                             -- NULL: automatic
    at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pl_journal_players ON pl_journal (into_player, from_player);

-- Names and usernames seen for a TGID (FT-И.2), as history.
CREATE TABLE pl_names (
    tg_id    bigint      NOT NULL,
    username text        NOT NULL DEFAULT '',
    name     text        NOT NULL DEFAULT '',
    seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tg_id, username, name)
);

-- Sanctions and marks: strike (scam), ban, unban, complaint, request, note (FT-И.5).
CREATE TABLE pl_marks (
    id         bigserial   PRIMARY KEY,
    kind       text        NOT NULL,
    ident_kind text        NOT NULL CHECK (ident_kind IN ('tg', 'mel')),
    ident      text        NOT NULL,
    text       text,
    author     text        NOT NULL,
    trace_id   uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trace_id, kind)
);
CREATE INDEX pl_marks_ident ON pl_marks (ident_kind, ident);

-- Pairs already known before this module: Autosup dialogs and the RM tables of players (D-RM-14, replaced by
-- D-PL-3). Only the links are copied; the module builds the players from them at start (PL_store.heal).
DO $$
BEGIN
    IF to_regclass('as_dialogs') IS NOT NULL THEN
        INSERT INTO pl_links (tg_id, mel_id, source, ref, first_seen, last_seen)
        SELECT tg_id, mel_id, 'autosup', 'as_dialogs', updated_at, updated_at
        FROM as_dialogs WHERE mel_id IS NOT NULL AND mel_id <> ''
        ON CONFLICT (tg_id, mel_id) DO NOTHING;
    END IF;
    IF to_regclass('rm_player_mel') IS NOT NULL THEN
        INSERT INTO pl_links (tg_id, mel_id, source, ref, first_seen, last_seen)
        SELECT tg_id, mel_id, 'rm:' || coalesce(source, ''), 'rm_player_mel', created_at, created_at
        FROM rm_player_mel WHERE tg_id IS NOT NULL
        ON CONFLICT (tg_id, mel_id) DO NOTHING;
    END IF;
END $$;
