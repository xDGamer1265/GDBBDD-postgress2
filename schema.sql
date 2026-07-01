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
    save_data BYTEA NOT NULL,
    level_data BYTEA NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_account UNIQUE (account_id)
);

-- Chunked save payloads for large Geometry Dash data
CREATE TABLE IF NOT EXISTS save_chunks (
    account_id VARCHAR(255) NOT NULL,
    data_kind VARCHAR(16) NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_data BYTEA NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, data_kind, chunk_index)
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
