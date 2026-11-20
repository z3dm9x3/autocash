-- TS: withdrawal codes in C-ticket (D-TS-5). The candidates of each message are kept; a ticket typed by a
-- code found in the base of withdrawal codes has the reason 'code'.

ALTER TABLE ts_ticket_messages ADD COLUMN codes text[] NOT NULL DEFAULT '{}';

ALTER TABLE ts_tickets DROP CONSTRAINT ts_tickets_reason_check;
ALTER TABLE ts_tickets ADD CONSTRAINT ts_tickets_reason_check CHECK (reason IN ('triggers', 'mixed', 'none', 'code'));
