-- Outbox: the action "delete" and tasks that wait for their time (D-OBX-8).
-- delete: remove target_msg_id in chat_id. The account that sent the message is tried first
-- (prefer_account_id), then every other account of the pool in the chat; when none could delete it the task
-- ends not_delivered and the failure goes to the monitor log and to the group logs.
-- not_before: the task is not routed earlier (e.g. "delete the command and its answer in 5 minutes").
-- delete_after_s on a send: after delivery a delete task of the sent message is made, not_before = +delete_after_s.

ALTER TABLE outbound_tasks DROP CONSTRAINT IF EXISTS outbound_tasks_action_check;
ALTER TABLE outbound_tasks
    ADD CONSTRAINT outbound_tasks_action_check CHECK (action IN ('send', 'pin', 'delete')),
    ADD COLUMN target_msg_id     bigint,       -- action delete: the message to delete
    ADD COLUMN not_before        timestamptz,  -- not routed before this time
    ADD COLUMN delete_after_s    float8,       -- send: delete the sent message this long after delivery
    ADD COLUMN prefer_account_id bigint;       -- unbound task: the account to try first (delete: the sender)

INSERT INTO reason_codes (code, meaning, status) VALUES
    ('SCHEDULED', 'задача ждёт своего времени (not_before)', 'waiting'),
    ('DELETE_FAILED', 'сообщение не удалось удалить ни с одного аккаунта', 'not_delivered')
ON CONFLICT (code) DO NOTHING;
