-- RM: users, groups, roles and rights (D-RM-7). The source of truth of the Directory of the core.
-- Telegram ids of groups are written without the minus and without the "100" of supergroups.

CREATE TABLE rm_roles (
    code       text PRIMARY KEY,                       -- lower case: guest, admin, other, ...
    scope      text NOT NULL CHECK (scope IN ('user', 'group')),
    title      text NOT NULL,                          -- for people: Guest, Admin, Other, ...
    policy     text CHECK (policy IN ('full', 'meta', 'presence', 'ignore')),  -- groups only
    sort       int  NOT NULL DEFAULT 0,
    UNIQUE (code, scope),
    CHECK ((scope = 'group') = (policy IS NOT NULL))
);

INSERT INTO rm_roles (code, scope, title, policy, sort) VALUES
    ('guest',   'user',  'Guest',   NULL, 1),
    ('player',  'user',  'Player',  NULL, 2),
    ('casher',  'user',  'Casher',  NULL, 3),
    ('support', 'user',  'Support', NULL, 4),
    ('manager', 'user',  'Manager', NULL, 5),
    ('admin',   'user',  'Admin',   NULL, 6),
    ('dev',     'user',  'Dev',     NULL, 7),
    ('other',   'group', 'Other',   'presence', 1),    -- known to exist, not read, no reactions
    ('tech',    'group', 'Tech',    'full', 2),
    ('dop',     'group', 'Dop',     'full', 3),
    ('ww',      'group', 'WW',      'full', 4);

CREATE TABLE rm_users (
    user_id    bigserial PRIMARY KEY,
    tg_id      bigint NOT NULL UNIQUE CHECK (tg_id > 0),
    username   text,                                   -- without @, if the user has one
    role       text NOT NULL DEFAULT 'guest',
    role_scope text NOT NULL DEFAULT 'user' CHECK (role_scope = 'user'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by bigint,                                 -- Telegram id of the author of the change
    FOREIGN KEY (role, role_scope) REFERENCES rm_roles (code, scope)
);

CREATE TABLE rm_groups (
    group_id    bigserial PRIMARY KEY,
    group_tg_id bigint NOT NULL UNIQUE CHECK (group_tg_id > 0),
    title       text,
    role        text NOT NULL DEFAULT 'other',
    role_scope  text NOT NULL DEFAULT 'group' CHECK (role_scope = 'group'),
    casher_id   bigint REFERENCES rm_users (user_id),  -- the cashier of the group: filled later
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    updated_by  bigint,
    FOREIGN KEY (role, role_scope) REFERENCES rm_roles (code, scope)
);

-- Rights belong to users only (role -> command; "*" = every command). Dev may run every command
-- always: that is a rule of the code, not a row here, so Dev can not be locked out.
CREATE TABLE rm_permissions (
    role       text NOT NULL,
    role_scope text NOT NULL DEFAULT 'user' CHECK (role_scope = 'user'),
    command    text NOT NULL,
    PRIMARY KEY (role, command),
    FOREIGN KEY (role, role_scope) REFERENCES rm_roles (code, scope)
);

INSERT INTO rm_permissions (role, command) VALUES ('admin', '*');

CREATE TABLE rm_history (
    id      bigserial PRIMARY KEY,
    at      timestamptz NOT NULL DEFAULT now(),
    author  bigint,                                    -- Telegram id; NULL: the system (import)
    entity  text NOT NULL CHECK (entity IN ('user', 'group')),
    tg_id   bigint NOT NULL,
    before  text,
    after   text
);
CREATE INDEX rm_history_entity ON rm_history (entity, tg_id, at);
