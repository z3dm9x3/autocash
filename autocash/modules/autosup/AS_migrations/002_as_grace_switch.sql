-- AS: 60 seconds for the user to send the ID before it is asked (D-AS-6); the switch /as on|off (D-AS-7).

ALTER TABLE as_dialogs DROP CONSTRAINT as_dialogs_state_check;
ALTER TABLE as_dialogs ADD CONSTRAINT as_dialogs_state_check
    CHECK (state IN ('grace', 'awaiting_id', 'done', 'expired'));
ALTER TABLE as_dialogs ADD COLUMN contact_at timestamptz;  -- the first message: messages from then count
ALTER TABLE as_dialogs ADD COLUMN ask_at timestamptz;      -- grace ends: no ID by then -> ask
CREATE INDEX as_dialogs_grace ON as_dialogs (ask_at) WHERE state = 'grace';

CREATE TABLE as_settings (
    id         int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled    boolean NOT NULL DEFAULT true,               -- /as on | /as off
    updated_by bigint,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO as_settings (id, enabled) VALUES (1, true);
