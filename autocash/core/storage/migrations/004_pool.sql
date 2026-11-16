-- Stage 1b: account pool, polling state, gaps, routing of outbound tasks.

ALTER TABLE accounts ADD COLUMN last_error text;

-- pts of a channel/supergroup is the same for every account, so the state is per chat (FT-2.5, FT-2.6).
CREATE TABLE chat_poll_state (
    chat_id    bigint PRIMARY KEY,
    pts        bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Sources where Telegram answered "difference too long": history must be reconciled (FT-2.7).
CREATE TABLE source_gaps (
    id            bigserial PRIMARY KEY,
    chat_id       bigint      NOT NULL,
    detected_at   timestamptz NOT NULL DEFAULT now(),
    pts_from      bigint,
    pts_to        bigint,
    status        text        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'reconciled')),
    reconciled_at timestamptz
);
CREATE INDEX source_gaps_open ON source_gaps (chat_id) WHERE status = 'open';

-- Dialogs where the client blocked the account (USER_IS_BLOCKED): shown to the staff (FT-10.5).
CREATE TABLE dialog_marks (
    account_id bigint      NOT NULL,
    chat_id    bigint      NOT NULL,
    mark       text        NOT NULL,
    at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, chat_id, mark)
);

-- A reply in a private dialog is sent only by the account of that dialog (FT-9.4).
ALTER TABLE outbound_tasks ADD COLUMN dialog_account_id bigint;
