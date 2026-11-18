-- AS: where Autosup is with each user (AS_readme.md).

CREATE TABLE as_dialogs (
    tg_id      bigint PRIMARY KEY,                      -- the user (rm_users.tg_id)
    account_id bigint NOT NULL,                         -- the pool account of the private dialog
    contact    text   NOT NULL CHECK (contact IN ('first', 'second')),
    state      text   NOT NULL CHECK (state IN ('awaiting_id', 'done', 'expired')),
    asked_at   timestamptz,                             -- the request "Пришлите ваш ID" was queued
    expires_at timestamptz,                             -- waiting for the ID stops then
    mel_id     text,                                    -- the ID taken
    found_in   text CHECK (found_in IN ('reply', 'history')),
    msg_id     bigint,                                  -- the user's message with the ID
    player_id  bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX as_dialogs_waiting ON as_dialogs (expires_at) WHERE state = 'awaiting_id';
