-- RM: players moved to the module players (D-PLAYERS-3). The RM tables of players (D-RM-14) are dropped, but
-- only when the graph of players is there: its migration (players/001_players_graph.sql) copied their pairs.
-- Without the module players the tables stay and this migration is a no-op (it is not repeated: then drop them
-- by hand). A database where the earlier name of this migration (004_rm_players_to_pl.sql) ran has nothing to
-- drop: a repeat changes nothing.
DO $$
BEGIN
    IF to_regclass('players_links') IS NOT NULL OR to_regclass('pl_links') IS NOT NULL THEN
        ALTER TABLE rm_users DROP COLUMN IF EXISTS player_id;
        DROP TABLE IF EXISTS rm_player_mel;
        DROP TABLE IF EXISTS rm_players;
    END IF;
END $$;
