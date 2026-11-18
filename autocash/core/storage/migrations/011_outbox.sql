-- Outbox (OBX, D-OBX-1..5): actions, binding to an account, router decisions, antiflood waits.

ALTER TABLE outbound_tasks
    ADD COLUMN action            text    NOT NULL DEFAULT 'send' CHECK (action IN ('send', 'pin')),
    ADD COLUMN pin_msg_id        bigint,                         -- action pin: the message to pin
    ADD COLUMN pin_after_send    boolean NOT NULL DEFAULT false, -- send, then pin the sent message
    ADD COLUMN target_type       text,                           -- user / group
    ADD COLUMN binding           text    NOT NULL DEFAULT 'none' CHECK (binding IN ('none', 'dialog', 'explicit')),
    ADD COLUMN router_used       boolean,                        -- OBX_Router chose among several accounts
    ADD COLUMN router_load       jsonb,                          -- account -> writes last minute + queued
    ADD COLUMN antiflood_wait_ms bigint  NOT NULL DEFAULT 0,     -- held back by OBX_Antifloodwaiter
    ADD COLUMN confirmed_at      timestamptz;                    -- Telegram returned the message number
ALTER TABLE outbound_tasks ALTER COLUMN priority SET DEFAULT 5;
-- dialog_account_id is the account the task is bound to (binding dialog or explicit).
