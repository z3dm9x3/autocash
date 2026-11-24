-- AS: working hours of the accounts (D-AS-8).
-- Out of the working hours of the account a player gets the away reply once per non-working period, and the
-- request "Пришлите ваш ID" is not sent: the wait goes on silently (an ID sent later is still taken).

CREATE TABLE as_away (
    tg_id      bigint      NOT NULL,             -- the player
    account_id bigint      NOT NULL,             -- the account he wrote to (its schedule)
    sent_at    timestamptz NOT NULL,             -- the last away reply: one per non-working period
    PRIMARY KEY (tg_id, account_id)
);

ALTER TABLE as_dialogs ADD COLUMN ask_skipped_at timestamptz;  -- out of hours: the request was not sent
