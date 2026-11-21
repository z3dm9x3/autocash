-- players: players as a graph of TGID <-> MelID links (players_readme.md, D-PLAYERS-1..5).
-- A link is one observed pair; a player is a connected group of links. There is no size limit: one person
-- may have thousands of MelIDs and TGIDs. Links are never deleted, so any join can be recomputed (split).
--
-- The module was first released with the prefix "pl" (migration players/001_pl_graph.sql, 2026-11-20). A
-- database made by it keeps its data: the tables, their sequences, indexes and constraints are renamed here
-- (D-PLAYERS-5). On a new database the tables are simply created.

DO $$
DECLARE
    t   text;
    obj record;
BEGIN
    IF to_regclass('pl_links') IS NULL THEN
        RETURN;
    END IF;
    ALTER TABLE pl_players RENAME TO players;
    FOREACH t IN ARRAY ARRAY['links', 'nodes', 'journal', 'names', 'marks'] LOOP
        EXECUTE format('ALTER TABLE pl_%s RENAME TO players_%s', t, t);
    END LOOP;
    -- Indexes (primary and unique keys too) and sequences: "pl_players..." -> "players...", "pl_x" -> "players_x".
    FOR obj IN SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = current_schema() AND c.relkind IN ('i', 'S') AND c.relname LIKE 'pl\_%' LOOP
        EXECUTE format('ALTER %s %I RENAME TO %I', CASE obj.relkind WHEN 'S' THEN 'SEQUENCE' ELSE 'INDEX' END,
                       obj.relname, regexp_replace(regexp_replace(obj.relname, '^pl_players', 'players'),
                                                   '^pl_', 'players_'));
    END LOOP;
    -- Constraints that are not indexes (checks, foreign keys).
    FOR obj IN SELECT con.conname, rel.relname FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = rel.relnamespace
               WHERE n.nspname = current_schema() AND con.conname LIKE 'pl\_%' LOOP
        EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', obj.relname, obj.conname,
                       regexp_replace(regexp_replace(obj.conname, '^pl_players', 'players'), '^pl_', 'players_'));
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS players (
    player_id     bigserial   PRIMARY KEY,
    created_at    timestamptz NOT NULL DEFAULT now(),
    first_link_id bigint,                         -- the link that made the player (NULL: made by a split)
    merged_into   bigint REFERENCES players (player_id),  -- set when the player was joined into another
    merged_at     timestamptz
);
CREATE INDEX IF NOT EXISTS players_merged ON players (merged_into) WHERE merged_into IS NOT NULL;

-- One observed pair. state: active (joins players) | staff (the TGID is staff or a cashier: kept, joins
-- nothing) | disabled (cut by hand). source: who showed the pair (autosup, msg:<type>, history:<kind>, manual).
CREATE TABLE IF NOT EXISTS players_links (
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
CREATE INDEX IF NOT EXISTS players_links_mel ON players_links (mel_id);

-- An identifier and its current player. Only identifiers of active links are here.
CREATE TABLE IF NOT EXISTS players_nodes (
    kind      text        NOT NULL CHECK (kind IN ('tg', 'mel')),
    value     text        NOT NULL,
    player_id bigint      NOT NULL REFERENCES players (player_id),
    since     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, value)
);
CREATE INDEX IF NOT EXISTS players_nodes_player ON players_nodes (player_id);

-- Journal: join (two players met on a link), split (a cut link broke a player apart), state (a link by hand).
CREATE TABLE IF NOT EXISTS players_journal (
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
CREATE INDEX IF NOT EXISTS players_journal_players ON players_journal (into_player, from_player);

-- Names and usernames seen for a TGID (FT-И.2), as history.
CREATE TABLE IF NOT EXISTS players_names (
    tg_id    bigint      NOT NULL,
    username text        NOT NULL DEFAULT '',
    name     text        NOT NULL DEFAULT '',
    seen_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tg_id, username, name)
);

-- Sanctions and marks: strike (scam), ban, unban, complaint, request, note (FT-И.5).
CREATE TABLE IF NOT EXISTS players_marks (
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
CREATE INDEX IF NOT EXISTS players_marks_ident ON players_marks (ident_kind, ident);

-- Pairs already known before this module: Autosup dialogs and the RM tables of players (D-RM-14, replaced by
-- D-PLAYERS-3). Only the links are copied; the module builds the players from them at start
-- (players_store.heal). A repeat copies nothing: the pairs are there.
DO $$
BEGIN
    IF to_regclass('as_dialogs') IS NOT NULL THEN
        INSERT INTO players_links (tg_id, mel_id, source, ref, first_seen, last_seen)
        SELECT tg_id, mel_id, 'autosup', 'as_dialogs', updated_at, updated_at
        FROM as_dialogs WHERE mel_id IS NOT NULL AND mel_id <> ''
        ON CONFLICT (tg_id, mel_id) DO NOTHING;
    END IF;
    IF to_regclass('rm_player_mel') IS NOT NULL THEN
        INSERT INTO players_links (tg_id, mel_id, source, ref, first_seen, last_seen)
        SELECT tg_id, mel_id, 'rm:' || coalesce(source, ''), 'rm_player_mel', created_at, created_at
        FROM rm_player_mel WHERE tg_id IS NOT NULL
        ON CONFLICT (tg_id, mel_id) DO NOTHING;
    END IF;
END $$;
