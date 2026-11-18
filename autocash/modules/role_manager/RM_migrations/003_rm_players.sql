-- RM: players (D-RM-14). One person = one player; several Telegram accounts (rm_users.player_id)
-- and several MelIDs (rm_player_mel). Roles stay per TGID in rm_users. Created empty.

CREATE TABLE rm_players (
    player_id  bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text                                    -- module or author that made the player
);

ALTER TABLE rm_users ADD COLUMN player_id bigint REFERENCES rm_players (player_id);
CREATE INDEX rm_users_player ON rm_users (player_id) WHERE player_id IS NOT NULL;

CREATE TABLE rm_player_mel (
    mel_id     text PRIMARY KEY,                       -- a MelID belongs to one player
    player_id  bigint NOT NULL REFERENCES rm_players (player_id),
    source     text,                                   -- autosup, manual, ...
    tg_id      bigint,                                 -- the Telegram account it came from
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX rm_player_mel_player ON rm_player_mel (player_id);
