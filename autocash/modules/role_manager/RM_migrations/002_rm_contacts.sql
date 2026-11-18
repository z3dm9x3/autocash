-- RM: where a user came from and the contact protocol that was run for them (D-RM-11, D-RM-12).

ALTER TABLE rm_users
    ADD COLUMN source           text NOT NULL DEFAULT 'manual',  -- dm, import:cmd, seed, cli, manual
    ADD COLUMN had_dm           boolean NOT NULL DEFAULT false,  -- a private dialog with a pool account exists
    ADD COLUMN contact_protocol text CHECK (contact_protocol IN ('first', 'second')),
    ADD COLUMN contact_at       timestamptz;
