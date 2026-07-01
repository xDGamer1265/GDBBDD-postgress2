package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"
)

const backupChunkSize = 1024 * 1024

func ensureSaveChunksMigration() error {
	if DB == nil {
		return fmt.Errorf("DB not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := `CREATE TABLE IF NOT EXISTS save_chunks (
		account_id VARCHAR(255) NOT NULL,
		data_kind VARCHAR(16) NOT NULL,
		chunk_index INTEGER NOT NULL,
		chunk_data BYTEA NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (account_id, data_kind, chunk_index)
	);`
	if _, err := DB.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

func chunkedDataLen(ctx context.Context, db *sql.DB, accountID, kind, fallbackColumn string) (int64, error) {
	var total sql.NullInt64
	var count int64
	err := db.QueryRowContext(ctx,
		Q("SELECT COALESCE(SUM(LENGTH(chunk_data)), 0), COUNT(*) FROM save_chunks WHERE account_id = ? AND data_kind = ?"),
		accountID, kind,
	).Scan(&total, &count)
	if err != nil {
		return 0, err
	}
	if count > 0 {
		return total.Int64, nil
	}

	if fallbackColumn != "save_data" && fallbackColumn != "level_data" {
		return 0, fmt.Errorf("invalid fallback column %q", fallbackColumn)
	}

	query := fmt.Sprintf("SELECT LENGTH(%s) FROM saves WHERE account_id = $1", fallbackColumn)
	var fallback sql.NullInt64
	if err := db.QueryRowContext(ctx, query, accountID).Scan(&fallback); err != nil {
		return 0, err
	}
	if !fallback.Valid {
		return 0, nil
	}
	return fallback.Int64, nil
}

func replaceChunkedData(ctx context.Context, db *sql.DB, accountID, kind, fallbackColumn string, data []byte) error {
	if fallbackColumn != "save_data" && fallbackColumn != "level_data" {
		return fmt.Errorf("invalid fallback column %q", fallbackColumn)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, Q("DELETE FROM save_chunks WHERE account_id = ? AND data_kind = ?"), accountID, kind); err != nil {
		return err
	}

	insert := Q("INSERT INTO save_chunks (account_id, data_kind, chunk_index, chunk_data) VALUES (?, ?, ?, ?)")
	for chunkIndex, start := 0, 0; start < len(data); chunkIndex, start = chunkIndex+1, start+backupChunkSize {
		end := start + backupChunkSize
		if end > len(data) {
			end = len(data)
		}
		if _, err := tx.ExecContext(ctx, insert, accountID, kind, chunkIndex, data[start:end]); err != nil {
			return err
		}
	}

	update := fmt.Sprintf("UPDATE saves SET %s = '\\x', created_at = CURRENT_TIMESTAMP WHERE account_id = $1", fallbackColumn)
	if _, err := tx.ExecContext(ctx, update, accountID); err != nil {
		return err
	}

	return tx.Commit()
}

func writeChunkedData(ctx context.Context, w http.ResponseWriter, db *sql.DB, accountID, kind string) (int64, error) {
	rows, err := db.QueryContext(ctx,
		Q("SELECT chunk_data FROM save_chunks WHERE account_id = ? AND data_kind = ? ORDER BY chunk_index"),
		accountID, kind,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var written int64
	for rows.Next() {
		var chunk []byte
		if err := rows.Scan(&chunk); err != nil {
			return written, err
		}
		n, err := w.Write(chunk)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	if err := rows.Err(); err != nil {
		return written, err
	}
	return written, nil
}
