-- orders: deposits and withdrawals of the kassas (orders_readme.md, D-ORDERS-1..7).
--
-- Every card of the kassa platform bot is parsed by SQL (orders_parse), stored as a message of its order
-- (orders_deposit_messages / orders_withdrawals) and the deposit is computed from all its messages
-- (orders_deposit_recompute): the result does not depend on the order in which the messages arrive.
-- The history of Cash Monitor Dev is read inside PostgreSQL through dblink (orders_history_step), slowly,
-- by ranges of raw_messages.id; nothing of the archive is copied but the parsed cards.

-- ---- orders ------------------------------------------------------------------------------------------------
CREATE TABLE orders_deposits (
    deposit_id     bigint      PRIMARY KEY,
    tg_id          bigint,
    mel_id         text,
    user_name      text,                       -- name or @username of the card
    user_type      text,                       -- USER, TRUSTED, NEW, CASHIER, ADMIN
    amount         numeric(16, 2),             -- requested: «Сумма пополнения» / «К зачислению» of the request
    receipt_amount numeric(16, 2),             -- the receipt [ДЕПОЗИТ], or the accepted amount (Top Pay)
    method         text,                       -- the payment method without its limits
    chat_id        bigint,                     -- the kassa of the last request card
    chats          bigint[]    NOT NULL DEFAULT '{}',  -- kassas in the order of their cards (redirects)
    status         text        NOT NULL DEFAULT 'open'
                   CHECK (status IN ('open', 'receipt', 'success', 'rejected', 'cancelled')),
    cancel_by      text,                       -- «Инициатор»: пользователь, система, кассир
    cancel_reason  text,                       -- «Причина»
    created_at     timestamptz,                -- the first request card
    receipt_at     timestamptz,
    final_at       timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX orders_deposits_mel ON orders_deposits (mel_id);
CREATE INDEX orders_deposits_tg ON orders_deposits (tg_id);
CREATE INDEX orders_deposits_created ON orders_deposits (created_at);

-- Every card of a deposit: request (a kassa got it), receipt ([ДЕПОЗИТ] with the check), cancel (a separate
-- «❌ Заявка #N отменена»), accepted (Top Pay «✅ Депозит принят #N»). The last version of the text counts.
CREATE TABLE orders_deposit_messages (
    chat_id      bigint      NOT NULL,
    msg_id       bigint      NOT NULL,
    deposit_id   bigint,                       -- NULL: an early Polygon request without «Deposit ID»
    kind         text        NOT NULL CHECK (kind IN ('request', 'receipt', 'cancel', 'accepted')),
    tg_id        bigint,
    mel_id       text,
    sender_id    bigint,
    reply_to     bigint,
    message_date timestamptz NOT NULL,
    edit_date    timestamptz,
    fields       jsonb       NOT NULL,         -- orders_parse
    source       text        NOT NULL,         -- history (the archive of Cash Monitor Dev) | live
    ref          text,                         -- history: raw_messages.id of the archive
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, msg_id)
);
CREATE INDEX orders_deposit_messages_deposit ON orders_deposit_messages (deposit_id);
CREATE INDEX orders_deposit_messages_pair ON orders_deposit_messages (tg_id, mel_id);
CREATE INDEX orders_deposit_messages_updated ON orders_deposit_messages (updated_at);

-- A withdrawal is one card [ВЫВОД]; the result of the code check is written into it by an edit.
CREATE TABLE orders_withdrawals (
    chat_id      bigint      NOT NULL,
    msg_id       bigint      NOT NULL,
    tg_id        bigint,
    mel_id       text,
    user_name    text,
    platform     int,                          -- «Платформа»: the number of the kassa on the platform
    code         text,                         -- «Код» as the player typed it
    amount       numeric(16, 2),
    method       text,
    requisites   text,                         -- as typed, kept as is (AGENTS.md, invariant 8)
    bank         text,
    result       text        NOT NULL DEFAULT 'none'
                 CHECK (result IN ('none', 'success', 'fail', 'bare_ok', 'bare_fail')),
    result_text  text,                         -- «Неуспех: …» without the code
    result_code  bigint,                       -- «(код: N)»
    sender_id    bigint,
    message_date timestamptz NOT NULL,
    edit_date    timestamptz,
    fields       jsonb       NOT NULL,
    source       text        NOT NULL,
    ref          text,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, msg_id)
);
CREATE INDEX orders_withdrawals_mel ON orders_withdrawals (mel_id);
CREATE INDEX orders_withdrawals_tg ON orders_withdrawals (tg_id);
CREATE INDEX orders_withdrawals_date ON orders_withdrawals (message_date);
CREATE INDEX orders_withdrawals_updated ON orders_withdrawals (updated_at);

-- ---- the scan of the history ---------------------------------------------------------------------------------
CREATE TABLE orders_history (
    source          text        PRIMARY KEY,   -- "cash_monitor": public.raw_messages of Cash Monitor Dev
    state           text        NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'paused')),
    last_id         bigint      NOT NULL DEFAULT 0,  -- raw_messages.id scanned up to (inclusive)
    top_id          bigint,                    -- max(raw_messages.id) seen by the last step
    steps           bigint      NOT NULL DEFAULT 0,
    cards           bigint      NOT NULL DEFAULT 0,
    refreshed       bigint      NOT NULL DEFAULT 0,
    started_at      timestamptz NOT NULL DEFAULT now(),
    caught_up_at    timestamptz,               -- the first pass reached the end of the archive
    last_step_at    timestamptz,
    last_refresh_at timestamptz,
    settled_at      timestamptz,               -- users and pairs taken up to this updated_at of the messages
    last_error      text,
    last_error_at   timestamptz
);

-- Chats where the platform bot posted cards: the groups of the kassas.
CREATE TABLE orders_history_chats (
    chat_id  bigint      PRIMARY KEY,
    title    text,
    cards    bigint      NOT NULL DEFAULT 0,
    first_at timestamptz,
    last_at  timestamptz
);

-- Who wrote in which group of the archive: a person writing in a group of a kassa is staff (D-ORDERS-4).
CREATE TABLE orders_history_senders (
    chat_id  bigint      NOT NULL,
    tg_id    bigint      NOT NULL,
    messages bigint      NOT NULL DEFAULT 0,
    first_at timestamptz,
    last_at  timestamptz,
    PRIMARY KEY (chat_id, tg_id)
);

-- ---- parsing -------------------------------------------------------------------------------------------------
-- "36 000.77" / "36 000,77 руб" / "1000.13" -> 36000.77; NULL when there is no number.
CREATE FUNCTION orders_num(t text) RETURNS numeric LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    s text := replace(regexp_replace(coalesce(t, ''), '[^0-9.,]', '', 'g'), ',', '.');
BEGIN
    IF s !~ '^[0-9]+(\.[0-9]+)?$' THEN
        RETURN NULL;
    END IF;
    RETURN round(s::numeric, 2);
END $$;

-- The value of the line "<key> <value>" (the key with its colon), trimmed; NULL without such a line.
CREATE FUNCTION orders_line(t text, key text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT nullif(btrim((regexp_match(t, '(?:^|\n)' || key || '[ \t]*([^\n]*)'))[1]), '')
$$;

-- A card of the platform bot -> its kind and fields; NULL for any other text (knowledge/deposits.md,
-- knowledge/withdrawals.md). media: raw_payload.media._ (a Polygon card with a check is a receipt).
CREATE FUNCTION orders_parse(t text, media text) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    head    text;
    kind    text;
    m       text[];
    line    text;
    tail    text;
    f       jsonb;
    outcome text;
BEGIN
    IF t IS NULL OR t = '' THEN
        RETURN NULL;
    END IF;
    t := replace(t, U&'\2060', '');                       -- WORD JOINER in names of methods
    head := btrim(split_part(t, E'\n', 1));
    IF head LIKE '[ВЫВОД]%' THEN
        kind := 'withdrawal';
    ELSIF head LIKE '[ДЕПОЗИТ]%' THEN
        kind := 'receipt';
    ELSIF head LIKE '[ДЕПОЗИТ ПОЛИГОН]%' AND coalesce(media, '') IN ('MessageMediaDocument', 'MessageMediaPhoto') THEN
        kind := 'receipt';                                -- Polygon: the check comes as a new card
    ELSIF head LIKE '[ДЕПОЗИТ %' THEN
        kind := 'request';
    ELSIF t ~ '^❌ Заявка #[0-9 ]+ отменена' THEN
        kind := 'cancel';
    ELSIF t ~ '^✅ Депозит принят #[0-9]' THEN
        kind := 'accepted';
    ELSE
        RETURN NULL;
    END IF;
    f := jsonb_build_object('kind', kind, 'header', head);
    tail := regexp_replace(rtrim(t, E' \t\n'), '^.*\n', '');   -- the last line

    -- Who and which account (all kinds that have them).
    m := regexp_match(t, '(?:^|\n)Пользователь:[ \t]*([^\n]*)[ \t]+ID:[ \t]*([0-9]+)');
    IF m IS NOT NULL THEN
        f := f || jsonb_build_object('tg_id', m[2], 'name', btrim(m[1]));
        IF btrim(m[1]) ~ '^@[A-Za-z0-9_]+$' THEN
            f := f || jsonb_build_object('username', substr(btrim(m[1]), 2));
        END IF;
    ELSE
        m := regexp_match(t, '(?:^|\n)ID:[ \t]*([0-9]+)');  -- Polygon: «ID: <TGID>»
        IF m IS NOT NULL THEN
            f := f || jsonb_build_object('tg_id', m[1]);
        END IF;
    END IF;
    f := f || jsonb_build_object('user_type', (regexp_match(t, '(?:^|\n)Тип пользователя:[ \t]*([A-Za-z_]+)'))[1],
                                 'mel_id', (regexp_match(t, '(?:^|\n)Номер счёта:[ \t]*\[?([0-9]+)'))[1]);

    IF kind = 'withdrawal' THEN
        f := f || jsonb_build_object(
            'platform', (regexp_match(t, '(?:^|\n)Платформа:[ \t]*([0-9]+)'))[1],
            'code', orders_line(t, 'Код:'),
            'amount', orders_num(orders_line(t, 'Сумма вывода:')),
            'requisites', orders_line(t, 'Реквизиты:'),
            'bank', orders_line(t, 'Банк:'));
        line := orders_line(t, 'Метод оплаты:');
        f := f || jsonb_build_object('method', nullif(btrim(regexp_replace(line,
                 '[ |]*[0-9][0-9 ]*[ ]*-[ ]*[0-9][0-9 ]*$', '')), ''));
        IF t ~ '(?:^|\n)✅ Успех' THEN
            f := f || jsonb_build_object('result', 'success');
        ELSIF t ~ '(?:^|\n)❌ Неуспех' THEN
            line := (regexp_match(t, '(?:^|\n)❌ Неуспех:[ \t]*([^\n]*)'))[1];
            f := f || jsonb_build_object('result', 'fail',
                'result_text', nullif(btrim(regexp_replace(coalesce(line, ''), '\(код:[ ]*[0-9]+\)[ ]*$', '')), ''),
                'result_code', (regexp_match(coalesce(line, ''), '\(код:[ ]*([0-9]+)\)'))[1]);
        ELSIF tail = '✅' THEN
            f := f || jsonb_build_object('result', 'bare_ok');
        ELSIF tail = '❌' THEN
            f := f || jsonb_build_object('result', 'bare_fail');
        ELSE
            f := f || jsonb_build_object('result', 'none');
        END IF;
        RETURN jsonb_strip_nulls(f);
    END IF;

    -- Deposits: the id, the amount, the method, the outcome written into the card.
    m := regexp_match(t, '(?:^|\n)Deposit ID:[ \t]*([0-9][0-9 ]*)');
    IF m IS NULL THEN
        m := regexp_match(t, 'Заявка #([0-9][0-9 ]*[0-9]|[0-9])');
    END IF;
    IF m IS NULL THEN
        m := regexp_match(t, 'Депозит принят #([0-9]+)');
    END IF;
    IF m IS NOT NULL THEN
        f := f || jsonb_build_object('deposit_id', replace(m[1], ' ', ''));
    END IF;
    IF kind = 'accepted' THEN
        RETURN jsonb_strip_nulls(f || jsonb_build_object(
            'outcome', 'success',
            'amount', orders_num((regexp_match(t, '💰[ \t]*([0-9][0-9 .,]*)'))[1]),
            'username', (regexp_match(head, '\|[ \t]*@([A-Za-z0-9_]+)'))[1],
            'txn', (regexp_match(t, 'txn:[ \t]*([0-9A-Za-z_-]+)'))[1]));
    END IF;
    f := f || jsonb_build_object('amount', coalesce(orders_num(orders_line(t, 'Сумма пополнения:')),
                                                    orders_num(orders_line(t, 'К зачислению:'))));
    line := orders_line(t, 'Метод оплаты:');
    IF line IS NOT NULL THEN
        IF line ~ '❌$' THEN
            outcome := 'rejected';                        -- a bare ❌ appended to the last line (Polygon)
        ELSIF line ~ '✅$' THEN
            outcome := 'success';
        END IF;
        line := regexp_replace(line, '[ ]*[✅❌]+$', '');
        f := f || jsonb_build_object('method', nullif(btrim(regexp_replace(line,
                 '[ |]*[0-9][0-9 ]*[ ]*-[ ]*[0-9][0-9 ]*$', '')), ''));
    END IF;
    IF head LIKE '%ПЕРЕНАПРАВЛЕН%' THEN
        f := f || jsonb_build_object('redirected_in', true);
    END IF;
    m := regexp_match(t, '🔄 Депозит перенаправлен на платформу[ \t]*([^\n]*)');
    IF m IS NOT NULL THEN
        f := f || jsonb_build_object('redirected_to', btrim(m[1]));
    END IF;
    IF t ~ '❌ Заявка #[0-9 ]+ отменена' THEN
        m := regexp_match(t, 'Инициатор:[ \t]*([^·\n]*)·[ \t]*Причина:[ \t]*([^\n]*)');
        outcome := CASE WHEN kind = 'receipt' THEN 'rejected' ELSE 'cancelled' END;
        IF m IS NOT NULL THEN
            f := f || jsonb_build_object('cancel_by', btrim(m[1]), 'cancel_reason', btrim(m[2]));
        END IF;
    ELSIF t ~ '(?:^|\n)✅ (Успех|Подтверждено)' THEN
        outcome := 'success';
    ELSIF outcome IS NULL AND tail = '❌' THEN
        outcome := 'rejected';
    ELSIF outcome IS NULL AND tail = '✅' THEN
        outcome := 'success';
    END IF;
    IF kind = 'request' AND outcome IN ('success', 'rejected') THEN
        outcome := NULL;                                  -- a request card is closed only by a cancel
    END IF;
    RETURN jsonb_strip_nulls(f || jsonb_build_object('outcome', outcome));
END $$;

-- ---- writing a card ---------------------------------------------------------------------------------------
-- Stores the card (a later version replaces an earlier one, never the other way) and returns its fields;
-- NULL when the text is not a card. The deposit itself is computed by orders_deposit_recompute.
CREATE FUNCTION orders_apply(p_chat bigint, p_msg bigint, p_sender bigint, p_reply bigint, p_date timestamptz,
                             p_edit timestamptz, p_media text, p_text text, p_source text, p_ref text)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    f jsonb := orders_parse(p_text, p_media);
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    END IF;
    IF f->>'kind' = 'withdrawal' THEN
        INSERT INTO orders_withdrawals AS w (chat_id, msg_id, tg_id, mel_id, user_name, platform, code, amount, method,
                                             requisites, bank, result, result_text, result_code, sender_id,
                                             message_date, edit_date, fields, source, ref)
        VALUES (p_chat, p_msg, (f->>'tg_id')::bigint, f->>'mel_id', f->>'name', (f->>'platform')::int, f->>'code',
                (f->>'amount')::numeric, f->>'method', f->>'requisites', f->>'bank', f->>'result', f->>'result_text',
                (f->>'result_code')::bigint, p_sender, p_date, p_edit, f, p_source, p_ref)
        ON CONFLICT (chat_id, msg_id) DO UPDATE SET
            tg_id = EXCLUDED.tg_id, mel_id = EXCLUDED.mel_id, user_name = EXCLUDED.user_name,
            platform = EXCLUDED.platform, code = EXCLUDED.code, amount = EXCLUDED.amount, method = EXCLUDED.method,
            requisites = EXCLUDED.requisites, bank = EXCLUDED.bank, result = EXCLUDED.result,
            result_text = EXCLUDED.result_text, result_code = EXCLUDED.result_code, edit_date = EXCLUDED.edit_date,
            fields = EXCLUDED.fields, source = EXCLUDED.source, ref = EXCLUDED.ref, updated_at = now()
        WHERE coalesce(EXCLUDED.edit_date, EXCLUDED.message_date) >= coalesce(w.edit_date, w.message_date)
          AND w.fields IS DISTINCT FROM EXCLUDED.fields;
        RETURN f;
    END IF;
    INSERT INTO orders_deposit_messages AS d (chat_id, msg_id, deposit_id, kind, tg_id, mel_id, sender_id, reply_to,
                                              message_date, edit_date, fields, source, ref)
    VALUES (p_chat, p_msg, (f->>'deposit_id')::bigint, f->>'kind', (f->>'tg_id')::bigint, f->>'mel_id', p_sender,
            p_reply, p_date, p_edit, f, p_source, p_ref)
    ON CONFLICT (chat_id, msg_id) DO UPDATE SET
        deposit_id = EXCLUDED.deposit_id, kind = EXCLUDED.kind, tg_id = EXCLUDED.tg_id, mel_id = EXCLUDED.mel_id,
        edit_date = EXCLUDED.edit_date, fields = EXCLUDED.fields, source = EXCLUDED.source, ref = EXCLUDED.ref,
        updated_at = now()
    WHERE coalesce(EXCLUDED.edit_date, EXCLUDED.message_date) >= coalesce(d.edit_date, d.message_date)
      AND d.fields IS DISTINCT FROM EXCLUDED.fields;
    RETURN f;
END $$;

-- The deposit from all its cards. Status: success (a confirmed receipt or «Депозит принят») > rejected (the
-- receipt was declined) > cancelled (the request was cancelled) > receipt (a check, no final) > open.
CREATE FUNCTION orders_deposit_recompute(p_id bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    req      orders_deposit_messages%ROWTYPE;
    rec      orders_deposit_messages%ROWTYPE;
    acc      orders_deposit_messages%ROWTYPE;
    cnl      orders_deposit_messages%ROWTYPE;
    who      orders_deposit_messages%ROWTYPE;
    v_status text;
    v_chats  bigint[];
    v_first  timestamptz;
BEGIN
    IF p_id IS NULL THEN
        RETURN;
    END IF;
    SELECT * INTO req FROM orders_deposit_messages WHERE deposit_id = p_id AND kind = 'request'
        ORDER BY message_date DESC, msg_id DESC LIMIT 1;
    SELECT * INTO rec FROM orders_deposit_messages WHERE deposit_id = p_id AND kind = 'receipt'
        ORDER BY (fields->>'outcome' IS NOT NULL) DESC, message_date DESC LIMIT 1;
    SELECT * INTO acc FROM orders_deposit_messages WHERE deposit_id = p_id AND kind = 'accepted'
        ORDER BY message_date DESC LIMIT 1;
    SELECT * INTO cnl FROM orders_deposit_messages WHERE deposit_id = p_id
        AND (kind = 'cancel' OR (kind = 'request' AND fields->>'outcome' = 'cancelled'))
        ORDER BY message_date DESC LIMIT 1;
    IF req.msg_id IS NULL AND rec.msg_id IS NULL AND acc.msg_id IS NULL AND cnl.msg_id IS NULL THEN
        DELETE FROM orders_deposits WHERE deposit_id = p_id;
        RETURN;
    END IF;
    SELECT array_agg(chat_id ORDER BY first_at), min(first_at) INTO v_chats, v_first
        FROM (SELECT chat_id, min(message_date) AS first_at FROM orders_deposit_messages
              WHERE deposit_id = p_id AND kind = 'request' GROUP BY chat_id) s;
    IF req.msg_id IS NOT NULL THEN   -- the player, the account, the method: of the request, else of the receipt
        who := req;
    ELSE
        who := rec;
    END IF;
    v_status := CASE
        WHEN acc.msg_id IS NOT NULL OR rec.fields->>'outcome' = 'success' THEN 'success'
        WHEN rec.fields->>'outcome' = 'rejected' THEN 'rejected'
        WHEN cnl.msg_id IS NOT NULL THEN 'cancelled'
        WHEN rec.msg_id IS NOT NULL THEN 'receipt'
        ELSE 'open' END;
    INSERT INTO orders_deposits AS o (deposit_id, tg_id, mel_id, user_name, user_type, amount, receipt_amount, method,
                                      chat_id, chats, status, cancel_by, cancel_reason, created_at, receipt_at,
                                      final_at, updated_at)
    VALUES (p_id,
            coalesce(who.tg_id, rec.tg_id), coalesce(who.mel_id, rec.mel_id),
            coalesce(who.fields->>'name', rec.fields->>'name'), coalesce(who.fields->>'user_type', rec.fields->>'user_type'),
            (req.fields->>'amount')::numeric,
            coalesce((acc.fields->>'amount')::numeric, (rec.fields->>'amount')::numeric),
            coalesce(who.fields->>'method', rec.fields->>'method'),
            coalesce(req.chat_id, rec.chat_id, cnl.chat_id, acc.chat_id), coalesce(v_chats, '{}'),
            v_status,
            CASE v_status WHEN 'rejected' THEN rec.fields->>'cancel_by' WHEN 'cancelled' THEN cnl.fields->>'cancel_by' END,
            CASE v_status WHEN 'rejected' THEN rec.fields->>'cancel_reason'
                          WHEN 'cancelled' THEN cnl.fields->>'cancel_reason' END,
            coalesce(v_first, rec.message_date, cnl.message_date, acc.message_date),
            rec.message_date,
            CASE v_status
                WHEN 'success' THEN coalesce(acc.message_date, rec.edit_date, rec.message_date)
                WHEN 'rejected' THEN coalesce(rec.edit_date, rec.message_date)
                WHEN 'cancelled' THEN CASE WHEN cnl.kind = 'cancel' THEN cnl.message_date
                                           ELSE coalesce(cnl.edit_date, cnl.message_date) END
            END,
            now())
    ON CONFLICT (deposit_id) DO UPDATE SET
        tg_id = EXCLUDED.tg_id, mel_id = EXCLUDED.mel_id, user_name = EXCLUDED.user_name,
        user_type = EXCLUDED.user_type, amount = EXCLUDED.amount, receipt_amount = EXCLUDED.receipt_amount,
        method = EXCLUDED.method, chat_id = EXCLUDED.chat_id, chats = EXCLUDED.chats, status = EXCLUDED.status,
        cancel_by = EXCLUDED.cancel_by, cancel_reason = EXCLUDED.cancel_reason, created_at = EXCLUDED.created_at,
        receipt_at = EXCLUDED.receipt_at, final_at = EXCLUDED.final_at, updated_at = now();
END $$;

-- ---- one step of the history scan (through dblink) ---------------------------------------------------------
-- The range (last_id, last_id + p_batch] of public.raw_messages of the archive: the cards of the bots p_bots,
-- not forwarded, are applied; the senders of every message of the range are counted (staff, D-ORDERS-4).
-- p_refresh: the cards of the last p_refresh_days that are still open are read again (their final comes by an
-- edit after the scan). The remote session is read-only with a statement timeout. One transaction: a failed
-- step changes nothing, the next one repeats it.
CREATE FUNCTION orders_history_step(p_conn text, p_source text, p_batch int, p_bots bigint[], p_refresh boolean,
                                    p_refresh_days int, p_timeout_ms int) RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    st        orders_history%ROWTYPE;
    v_top     bigint;
    v_lo      bigint;
    v_hi      bigint;
    r         record;
    f         jsonb;
    v_ids     bigint[] := '{}';
    v_cards   int := 0;
    v_fresh   int := 0;
    v_refs    bigint[];
    v_cols    text := 'SELECT id, chat_id, telegram_message_id, sender_id, reply_to_message_id, message_date, '
                      'edit_date, coalesce(raw_payload->''media''->>''_'', ''''), text FROM public.raw_messages ';
    v_cards_where text;
BEGIN
    INSERT INTO orders_history (source) VALUES (p_source) ON CONFLICT (source) DO NOTHING;
    SELECT * INTO st FROM orders_history WHERE source = p_source FOR UPDATE;
    IF st.state = 'paused' THEN
        RETURN jsonb_build_object('state', 'paused');
    END IF;
    v_cards_where := format('sender_id = ANY(%L::bigint[]) AND raw_payload->>''fwd_from'' IS NULL AND ('
                            'text LIKE ''[ДЕПОЗИТ%%'' OR text LIKE ''[ВЫВОД]%%'' OR text LIKE ''❌ Заявка #%%'' '
                            'OR text LIKE ''✅ Депозит принят%%'')', p_bots::text);
    IF 'orders_history' = ANY(coalesce(dblink_get_connections(), '{}')) THEN
        PERFORM dblink_disconnect('orders_history');      -- left by a step that was cut off
    END IF;
    PERFORM dblink_connect('orders_history', p_conn);
    BEGIN
        PERFORM dblink_exec('orders_history', 'SET default_transaction_read_only = on');
        PERFORM dblink_exec('orders_history', format('SET statement_timeout = %s', p_timeout_ms));
        SELECT t.m INTO v_top FROM dblink('orders_history', 'SELECT max(id) FROM public.raw_messages') AS t(m bigint);
        v_lo := st.last_id;
        v_hi := greatest(v_lo, least(v_lo + p_batch, coalesce(v_top, 0)));
        IF v_hi > v_lo THEN
            FOR r IN SELECT * FROM dblink('orders_history', v_cols || format('WHERE id > %s AND id <= %s AND ', v_lo, v_hi)
                                          || v_cards_where)
                     AS t(id bigint, chat_id bigint, msg_id bigint, sender_id bigint, reply_to bigint,
                          message_date timestamptz, edit_date timestamptz, media text, body text) LOOP
                f := orders_apply(r.chat_id, r.msg_id, r.sender_id, r.reply_to, r.message_date, r.edit_date, r.media,
                                  r.body, 'history', r.id::text);
                CONTINUE WHEN f IS NULL;
                v_cards := v_cards + 1;
                IF f->>'kind' <> 'withdrawal' AND f ? 'deposit_id' THEN
                    v_ids := v_ids || (f->>'deposit_id')::bigint;
                END IF;
                INSERT INTO orders_history_chats AS c (chat_id, cards, first_at, last_at)
                VALUES (r.chat_id, 1, r.message_date, r.message_date)
                ON CONFLICT (chat_id) DO UPDATE SET cards = c.cards + 1, first_at = least(c.first_at, EXCLUDED.first_at),
                                                    last_at = greatest(c.last_at, EXCLUDED.last_at);
            END LOOP;
            INSERT INTO orders_history_senders AS s (chat_id, tg_id, messages, first_at, last_at)
            SELECT * FROM dblink('orders_history', format(
                'SELECT chat_id, sender_id, count(*), min(message_date), max(message_date) FROM public.raw_messages '
                'WHERE id > %s AND id <= %s AND chat_id < 0 AND sender_id > 0 AND NOT (sender_id = ANY(%L::bigint[])) '
                'GROUP BY 1, 2', v_lo, v_hi, p_bots::text))
                AS t(chat_id bigint, tg_id bigint, messages bigint, first_at timestamptz, last_at timestamptz)
            ON CONFLICT (chat_id, tg_id) DO UPDATE SET messages = s.messages + EXCLUDED.messages,
                first_at = least(s.first_at, EXCLUDED.first_at), last_at = greatest(s.last_at, EXCLUDED.last_at);
        END IF;
        IF p_refresh THEN
            SELECT array_agg(x) INTO v_refs FROM (
                SELECT m.ref::bigint AS x FROM orders_deposit_messages m JOIN orders_deposits d USING (deposit_id)
                WHERE m.source = 'history' AND d.status IN ('open', 'receipt') AND m.kind IN ('request', 'receipt')
                  AND m.message_date > now() - make_interval(days => p_refresh_days)
                UNION
                SELECT w.ref::bigint FROM orders_withdrawals w
                WHERE w.source = 'history' AND w.result = 'none'
                  AND w.message_date > now() - make_interval(days => p_refresh_days)
                LIMIT 2000) s;
            IF v_refs IS NOT NULL THEN
                FOR r IN SELECT * FROM dblink('orders_history', v_cols || format('WHERE id = ANY(%L::bigint[])',
                                                                                 v_refs::text))
                         AS t(id bigint, chat_id bigint, msg_id bigint, sender_id bigint, reply_to bigint,
                              message_date timestamptz, edit_date timestamptz, media text, body text) LOOP
                    f := orders_apply(r.chat_id, r.msg_id, r.sender_id, r.reply_to, r.message_date, r.edit_date,
                                      r.media, r.body, 'history', r.id::text);
                    CONTINUE WHEN f IS NULL;
                    v_fresh := v_fresh + 1;
                    IF f->>'kind' <> 'withdrawal' AND f ? 'deposit_id' THEN
                        v_ids := v_ids || (f->>'deposit_id')::bigint;
                    END IF;
                END LOOP;
            END IF;
        END IF;
        IF EXISTS (SELECT 1 FROM orders_history_chats WHERE title IS NULL) THEN
            UPDATE orders_history_chats c SET title = coalesce(t.title, '')
            FROM dblink('orders_history', 'SELECT DISTINCT ON (chat_id) chat_id, title FROM public.chat_registry '
                                          'ORDER BY chat_id, updated_at DESC') AS t(chat_id bigint, title text)
            WHERE c.chat_id = t.chat_id AND c.title IS NULL;
            UPDATE orders_history_chats SET title = '' WHERE title IS NULL;   -- not in the registry: asked once
        END IF;
        PERFORM dblink_disconnect('orders_history');
    EXCEPTION WHEN OTHERS THEN
        PERFORM dblink_disconnect('orders_history');
        RAISE;
    END;
    PERFORM orders_deposit_recompute(d) FROM (SELECT DISTINCT unnest(v_ids) AS d) s;
    UPDATE orders_history SET last_id = v_hi, top_id = v_top, steps = steps + 1, cards = cards + v_cards,
        refreshed = refreshed + v_fresh, last_step_at = now(),
        last_refresh_at = CASE WHEN p_refresh THEN now() ELSE last_refresh_at END,
        caught_up_at = CASE WHEN caught_up_at IS NULL AND v_hi >= coalesce(v_top, 0) THEN now() ELSE caught_up_at END,
        last_error = NULL
    WHERE source = p_source;
    RETURN jsonb_build_object('from', v_lo, 'to', v_hi, 'top', v_top, 'cards', v_cards, 'refreshed', v_fresh,
                              'deposits', cardinality(v_ids), 'caught_up', v_hi >= coalesce(v_top, 0));
END $$;

-- ---- users and players from the cards (D-ORDERS-4, D-ORDERS-5) ------------------------------------------------
-- Run after the first pass reached the end of the archive, then for what changed since the last run:
--   * staff: people who wrote in a group of a kassa, and card users of a staff type (CASHIER): role casher in
--     rm_users, if they have no role or guest/player; support, manager, admin, dev are never changed;
--   * players: the other card users: role player, if they are not in rm_users or are guests;
--   * pairs TGID - MelID of the cards go to players_links: active for players, staff for staff (they join
--     nothing); the module joins them into players (players_store.heal);
--   * names and usernames of the cards go to players_names.
-- p_exclude: the bots and the accounts of the pool: never users of a kassa.
-- Returns {roles: {tg: role} changed, demoted: [tg] (player -> casher: their active links must become staff),
-- links: n, names: n}.
CREATE FUNCTION orders_history_settle(p_source text, p_staff_types text[], p_exclude bigint[])
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
    v_since   timestamptz;
    v_now     timestamptz := now();
    v_staff   bigint[];
    v_roles   jsonb := '{}';
    v_demoted bigint[] := '{}';
    v_links   int := 0;
    v_names   int := 0;
    v_players jsonb;
BEGIN
    SELECT settled_at INTO v_since FROM orders_history WHERE source = p_source FOR UPDATE;
    v_since := coalesce(v_since, '-infinity');
    SELECT array_agg(DISTINCT tg_id) INTO v_staff FROM (
        SELECT s.tg_id FROM orders_history_senders s JOIN orders_history_chats c ON c.chat_id = s.chat_id AND c.cards > 0
        UNION
        SELECT tg_id FROM orders_deposit_messages
        WHERE tg_id IS NOT NULL AND coalesce(fields->>'user_type', '') = ANY(p_staff_types)) x
    WHERE NOT (tg_id = ANY(p_exclude)) AND tg_id NOT IN (SELECT account_id FROM accounts);
    v_staff := coalesce(v_staff, '{}');

    IF to_regclass('rm_users') IS NOT NULL THEN
        -- Staff: casher (the other staff roles are given by hand and stay).
        WITH cand AS (
            SELECT x.tg_id, u.role AS before FROM unnest(v_staff) x(tg_id) LEFT JOIN rm_users u ON u.tg_id = x.tg_id
            WHERE u.tg_id IS NULL OR u.role IN ('guest', 'player')
        ), done AS (
            INSERT INTO rm_users (tg_id, role, source) SELECT tg_id, 'casher', 'history' FROM cand
            ON CONFLICT (tg_id) DO UPDATE SET role = 'casher', updated_at = now()
            RETURNING tg_id
        ), hist AS (
            INSERT INTO rm_history (author, entity, tg_id, before, after)
            SELECT NULL, 'user', c.tg_id, c.before, 'casher' FROM cand c JOIN done USING (tg_id)
            RETURNING tg_id
        )
        SELECT coalesce(jsonb_object_agg(c.tg_id::text, 'casher'), '{}'),
               coalesce(array_agg(c.tg_id) FILTER (WHERE c.before = 'player'), '{}')
        INTO v_roles, v_demoted FROM cand c;

        -- Players: card users that are not staff.
        WITH users AS (
            SELECT DISTINCT ON (tg_id) tg_id, fields->>'username' AS username FROM (
                SELECT tg_id, fields, message_date FROM orders_deposit_messages
                WHERE tg_id IS NOT NULL AND updated_at > v_since
                  AND NOT coalesce(fields->>'user_type', '') = ANY(p_staff_types)
                UNION ALL
                SELECT tg_id, fields, message_date FROM orders_withdrawals WHERE tg_id IS NOT NULL AND updated_at > v_since
            ) a
            WHERE NOT (tg_id = ANY(v_staff)) AND NOT (tg_id = ANY(p_exclude))
            ORDER BY tg_id, message_date DESC
        ), cand AS (
            SELECT p.tg_id, p.username, u.role AS before FROM users p LEFT JOIN rm_users u ON u.tg_id = p.tg_id
            WHERE u.tg_id IS NULL OR u.role = 'guest'
        ), done AS (
            INSERT INTO rm_users (tg_id, role, source, username) SELECT tg_id, 'player', 'history', username FROM cand
            ON CONFLICT (tg_id) DO UPDATE SET role = 'player', updated_at = now(),
                username = coalesce(rm_users.username, EXCLUDED.username)
            RETURNING tg_id
        ), hist AS (
            INSERT INTO rm_history (author, entity, tg_id, before, after)
            SELECT NULL, 'user', c.tg_id, c.before, 'player' FROM cand c JOIN done USING (tg_id)
            RETURNING tg_id
        )
        SELECT coalesce(jsonb_object_agg(c.tg_id::text, 'player'), '{}') INTO v_players FROM cand c;
        v_roles := v_roles || v_players;
    END IF;

    -- Pairs of the cards changed since the last run that players_links does not have yet.
    WITH pairs AS (
        SELECT tg_id, mel_id, min(kind) AS kind, min(chat_id::text || ':' || msg_id::text) AS ref,
               min(message_date) AS first_seen, max(message_date) AS last_seen, count(*) AS n,
               bool_or(coalesce(fields->>'user_type', '') = ANY(p_staff_types)) AS staff_type
        FROM (SELECT tg_id, mel_id, 'dep' AS kind, chat_id, msg_id, message_date, fields FROM orders_deposit_messages
              WHERE tg_id IS NOT NULL AND mel_id IS NOT NULL AND updated_at > v_since
              UNION ALL
              SELECT tg_id, mel_id, 'ww', chat_id, msg_id, message_date, fields FROM orders_withdrawals
              WHERE tg_id IS NOT NULL AND mel_id IS NOT NULL AND updated_at > v_since) a
        GROUP BY tg_id, mel_id
    ), ins AS (
        INSERT INTO players_links (tg_id, mel_id, state, source, ref, first_seen, last_seen, seen_count)
        SELECT p.tg_id, p.mel_id,
               CASE WHEN p.staff_type OR p.tg_id = ANY(v_staff) OR p.tg_id = ANY(p_exclude)
                         OR coalesce(u.role, 'guest') NOT IN ('guest', 'player') THEN 'staff' ELSE 'active' END,
               'history:' || p.kind, p.ref, p.first_seen, p.last_seen, p.n
        FROM pairs p LEFT JOIN rm_users u ON u.tg_id = p.tg_id
        ON CONFLICT (tg_id, mel_id) DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_links FROM ins;

    WITH ins AS (
        INSERT INTO players_names (tg_id, username, name, seen_at)
        SELECT tg_id, coalesce(fields->>'username', ''), coalesce(fields->>'name', ''), max(message_date)
        FROM (SELECT tg_id, fields, message_date FROM orders_deposit_messages
              WHERE tg_id IS NOT NULL AND fields ? 'name' AND updated_at > v_since
              UNION ALL
              SELECT tg_id, fields, message_date FROM orders_withdrawals
              WHERE tg_id IS NOT NULL AND fields ? 'name' AND updated_at > v_since) a
        GROUP BY 1, 2, 3
        ON CONFLICT DO NOTHING
        RETURNING 1
    )
    SELECT count(*) INTO v_names FROM ins;

    UPDATE orders_history SET settled_at = v_now WHERE source = p_source;
    RETURN jsonb_build_object('roles', v_roles, 'demoted', to_jsonb(v_demoted), 'links', v_links, 'names', v_names,
                              'staff', cardinality(v_staff));
END $$;
