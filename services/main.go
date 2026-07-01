package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/DumbCaveSpider/GDAlternativeWeb/log"
	_ "github.com/lib/pq"
)

var DB *sql.DB
var authToken string

func main() {
	authToken = os.Getenv("AUTHORIZATION_TOKEN")
	if authToken != "" {
		log.Info("authorization: enabled (token validation required)")
	}

	if err := initGlobalDB(); err != nil {
		log.Error("DB init failed: %v", err)
	} else {
		log.Done("DB check: connected OK")
	}

	if err := ensureAccountsMigration(); err != nil {
		log.Warn("DB migration warning: %v", err)
	}

	// Ensure saves table exists as well
	if err := ensureSavesMigration(); err != nil {
		log.Warn("DB migration warning (saves): %v", err)
	}
	if err := ensureSaveChunksMigration(); err != nil {
		log.Warn("DB migration warning (save chunks): %v", err)
	}
	if DB != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := ensureMembershipsTable(ctx, DB); err != nil {
			log.Warn("DB migration warning (memberships): %v", err)
		}
		cancel()
	}

	http.HandleFunc("/", authMiddleware(func(w http.ResponseWriter, r *http.Request) {
		//log.Debug("pong: %s", r.RemoteAddr)
		//w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		//w.WriteHeader(http.StatusOK)
		//_, _ = w.Write([]byte(""))
	}))

	startCleanupRoutine()

	port := os.Getenv("PORT")
	if port == "" {
		port = "3001"
	}
	addr := ":" + port
	log.Done("starting server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Error("server failed: %v", err)
	}
}

func initGlobalDB() error {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		dbUser := os.Getenv("DB_USER")
		dbPass := os.Getenv("DB_PASS")
		dbHost := os.Getenv("DB_HOST")
		dbPort := os.Getenv("DB_PORT")
		dbName := os.Getenv("DB_NAME")
		if dbUser == "" || dbHost == "" || dbName == "" {
			return fmt.Errorf("missing DB env vars (DATABASE_URL or DB_USER, DB_HOST, DB_NAME required)")
		}
		if dbPort == "" {
			dbPort = "5432"
		}
		sslMode := os.Getenv("DB_SSLMODE")
		if sslMode == "" {
			sslMode = "disable"
		}
		connStr = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s&connect_timeout=30",
			dbUser, dbPass, dbHost, dbPort, dbName, sslMode)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	DB = db
	return nil
}

// Q translates a query from MySQL '?' placeholder format to PostgreSQL '$1, $2, ...' format at runtime.
func Q(query string) string {
	n := 1
	for {
		idx := strings.Index(query, "?")
		if idx == -1 {
			break
		}
		query = query[:idx] + fmt.Sprintf("$%d", n) + query[idx+1:]
		n++
	}
	return query
}

func startCleanupRoutine() {
	go runCleanup()

	// cleanup every 24 hours
	go func() {
		log.Info("cleanup: scheduler started (interval: 24h)")
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			runCleanup()
		}
	}()
}

func runCleanup() {
	log.Debug("cleanup: checking for inactive accounts...")
	if DB == nil {
		log.Error("cleanup: DB not initialized")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var totalDeleted int
	for {
		// Process them in chunks of 500 to prevent large gap locks blockading incoming `saves`
		selectQuery := `SELECT a.account_id 
						FROM accounts a 
						JOIN saves s ON a.account_id = s.account_id 
						WHERE s.created_at < NOW() - INTERVAL '60 days' 
						LIMIT 500`

		rows, err := DB.QueryContext(ctx, selectQuery)
		if err != nil {
			log.Error("cleanup: failed to find inactive accounts: %v", err)
			break
		}

		var accountIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				accountIDs = append(accountIDs, id)
			}
		}
		rows.Close()

		if len(accountIDs) == 0 {
			break
		}

		args := make([]interface{}, len(accountIDs))
		placeholders := make([]string, len(accountIDs))
		for i, id := range accountIDs {
			args[i] = id
			placeholders[i] = "?"
		}
		inClause := strings.Join(placeholders, ",")

		// Using bulk deletes reduces index tree lock congestion,
		// but chunking limits the table-lock impact duration
		deleteSaves := fmt.Sprintf("DELETE FROM saves WHERE account_id IN (%s)", inClause)
		deleteChunks := fmt.Sprintf("DELETE FROM save_chunks WHERE account_id IN (%s)", inClause)
		_, errChunks := DB.ExecContext(ctx, Q(deleteChunks), args...)
		if errChunks != nil {
			log.Warn("cleanup: chunk save_chunks delete error: %v", errChunks)
		}

		_, errSaves := DB.ExecContext(ctx, Q(deleteSaves), args...)
		if errSaves != nil {
			log.Warn("cleanup: chunk saves delete error: %v", errSaves)
		}

		deleteAccounts := fmt.Sprintf("DELETE FROM accounts WHERE account_id IN (%s)", inClause)
		_, errAcc := DB.ExecContext(ctx, Q(deleteAccounts), args...)
		if errAcc != nil {
			log.Warn("cleanup: chunk accounts delete error: %v", errAcc)
		}

		totalDeleted += len(accountIDs)
		time.Sleep(100 * time.Millisecond) // Yield the table briefly
	}

	if totalDeleted > 0 {
		log.Info("cleanup: removed %d inactive rows (accounts+saves)", totalDeleted)
	} else {
		log.Debug("cleanup: no inactive accounts found")
	}

	// Cleanup expired memberships / subscribers
	subQuery := `UPDATE accounts 
				 SET subscriber = FALSE 
				 WHERE subscriber = TRUE 
				 AND NOT EXISTS (
					 SELECT 1 FROM memberships m 
					 WHERE m.account_id = accounts.account_id 
					 AND (m.expires_at > NOW() OR m.expires_at IS NULL)
				 )`
	if resSub, err := DB.ExecContext(ctx, subQuery); err != nil {
		log.Error("cleanup: failed to update expired subscribers: %v", err)
	} else {
		if rows, _ := resSub.RowsAffected(); rows > 0 {
			log.Info("cleanup: removed subscriber status from %d expired accounts", rows)
		}
	}
}

func ensureAccountsMigration() error {
	if DB == nil {
		return fmt.Errorf("DB not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acctCreate := `CREATE TABLE IF NOT EXISTS accounts (
		account_id VARCHAR(255) PRIMARY KEY,
		argon_token VARCHAR(512) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		token_validated_at TIMESTAMP NULL,
		subscriber BOOLEAN DEFAULT FALSE
	);`
	if _, err := DB.ExecContext(ctx, acctCreate); err != nil {
		return err
	}

	if _, err := DB.ExecContext(ctx, "ALTER TABLE accounts ADD COLUMN token_validated_at TIMESTAMP NULL"); err != nil {
		if !strings.Contains(err.Error(), "Duplicate column name") && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	if _, err := DB.ExecContext(ctx, "ALTER TABLE accounts ADD COLUMN subscriber BOOLEAN DEFAULT FALSE"); err != nil {
		if !strings.Contains(err.Error(), "Duplicate column name") && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	return nil
}

func ensureSavesMigration() error {
	if DB == nil {
		return fmt.Errorf("DB not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	createStmt := `CREATE TABLE IF NOT EXISTS saves (
		id BIGSERIAL PRIMARY KEY,
		account_id VARCHAR(255) NOT NULL,
		save_data BYTEA NOT NULL,
		level_data BYTEA NOT NULL DEFAULT '\x'::bytea,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT unique_account UNIQUE (account_id)
	);`

	if _, err := DB.ExecContext(ctx, createStmt); err != nil {
		return err
	}

	if err := ensureByteaColumn(ctx, "save_data"); err != nil {
		return err
	}
	if err := ensureByteaColumn(ctx, "level_data"); err != nil {
		return err
	}

	return nil
}

func ensureByteaColumn(ctx context.Context, column string) error {
	var dataType string
	query := `SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = 'saves'
		  AND column_name = $1`
	if err := DB.QueryRowContext(ctx, query, column).Scan(&dataType); err != nil {
		return err
	}
	if dataType == "bytea" {
		return nil
	}

	dropDefault := fmt.Sprintf("ALTER TABLE saves ALTER COLUMN %s DROP DEFAULT", column)
	if _, err := DB.ExecContext(ctx, dropDefault); err != nil {
		return err
	}

	alter := fmt.Sprintf("ALTER TABLE saves ALTER COLUMN %s TYPE BYTEA USING convert_to(%s::text, 'UTF8')", column, column)
	if _, err := DB.ExecContext(ctx, alter); err != nil {
		return err
	}

	setDefault := fmt.Sprintf("ALTER TABLE saves ALTER COLUMN %s SET DEFAULT '\\x'::bytea", column)
	if _, err := DB.ExecContext(ctx, setDefault); err != nil {
		return err
	}
	return nil
}

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if authToken != "" {
			reqToken := r.Header.Get("Authorization")
			if subtle.ConstantTimeCompare([]byte(reqToken), []byte(authToken)) != 1 {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}
