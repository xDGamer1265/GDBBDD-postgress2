-- Accounts table
CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(255) PRIMARY KEY,
    argon_token VARCHAR(512) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    token_validated_at TIMESTAMP NULL,
    subscriber BOOLEAN DEFAULT FALSE
);

-- Saves table
CREATE TABLE IF NOT EXISTS saves (
    id BIGSERIAL PRIMARY KEY,
    account_id VARCHAR(255) NOT NULL,
    save_data TEXT NOT NULL,
    level_data TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_account UNIQUE (account_id)
);

-- Memberships table
CREATE TABLE IF NOT EXISTS memberships (
    id SERIAL PRIMARY KEY,
    kofi_transaction_id VARCHAR(255),
    email VARCHAR(255),
    discord_username VARCHAR(255),
    discord_userid VARCHAR(255),
    tier_name VARCHAR(255),
    account_id VARCHAR(255),
    expires_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
