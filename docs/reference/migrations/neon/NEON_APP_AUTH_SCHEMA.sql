-- Neon app auth + account membership schema foundation
-- Transitional schema for the Neon Auth rollout.
-- Reuses holdings_accounts as the canonical account entity.

CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS app_users (
    user_id UUID PRIMARY KEY,
    auth_user_id TEXT NOT NULL UNIQUE,
    email CITEXT,
    display_name TEXT,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    default_account_id TEXT NULL REFERENCES holdings_accounts(account_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_users_auth_user_id
    ON app_users (auth_user_id);

CREATE TABLE IF NOT EXISTS account_memberships (
    membership_id UUID PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES holdings_accounts(account_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES app_users(user_id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_account_memberships_role
        CHECK (role IN ('owner', 'member'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_account_memberships_account_user
    ON account_memberships (account_id, user_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_account_memberships_default_per_user
    ON account_memberships (user_id)
    WHERE is_default;

CREATE INDEX IF NOT EXISTS idx_account_memberships_user_account
    ON account_memberships (user_id, account_id);

CREATE INDEX IF NOT EXISTS idx_account_memberships_account_user
    ON account_memberships (account_id, user_id);

ALTER TABLE holdings_accounts
    ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'personal',
    ADD COLUMN IF NOT EXISTS created_by_user_id UUID NULL REFERENCES app_users(user_id),
    ADD COLUMN IF NOT EXISTS default_owner_user_id UUID NULL REFERENCES app_users(user_id),
    ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS last_accessed_at TIMESTAMPTZ NULL;

ALTER TABLE holdings_accounts
    DROP CONSTRAINT IF EXISTS chk_holdings_accounts_account_type;

ALTER TABLE holdings_accounts
    ADD CONSTRAINT chk_holdings_accounts_account_type
        CHECK (account_type IN ('personal', 'shared', 'system'));

CREATE INDEX IF NOT EXISTS idx_holdings_accounts_default_owner
    ON holdings_accounts (default_owner_user_id);

CREATE INDEX IF NOT EXISTS idx_holdings_positions_current_account_updated
    ON holdings_positions_current (account_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_holdings_positions_current_account_ticker
    ON holdings_positions_current (account_id, ticker);

ALTER TABLE holdings_import_batches
    ADD COLUMN IF NOT EXISTS requested_by_user_id UUID NULL REFERENCES app_users(user_id);

CREATE INDEX IF NOT EXISTS idx_holdings_import_batches_account_created
    ON holdings_import_batches (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_holdings_import_batches_requested_by_user
    ON holdings_import_batches (requested_by_user_id, created_at DESC);

ALTER TABLE holdings_position_events
    ADD COLUMN IF NOT EXISTS created_by_user_id UUID NULL REFERENCES app_users(user_id);

CREATE INDEX IF NOT EXISTS idx_holdings_events_account_created_event
    ON holdings_position_events (account_id, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_holdings_events_created_by_user
    ON holdings_position_events (created_by_user_id, created_at DESC);
