-- OBX fuse (D-OBX-7): after a FloodWait the limits of the account are halved once; reset by hand only.

CREATE TABLE obx_account_limits (
    account_id     bigint PRIMARY KEY,
    factor         numeric NOT NULL DEFAULT 1 CHECK (factor > 0 AND factor <= 1),
    tripped_at     timestamptz,
    tripped_method text,
    tripped_wait_s numeric,
    reset_at       timestamptz,
    reset_by       bigint
);
