package caddystorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

const (
	defaultSQLitePath             = "/cache/sqlite/cache.db"
	defaultSQLiteReadConnections  = 4
	maxSQLiteReadConnections      = 16
	defaultSQLiteBusyTimeout      = 5 * time.Second
	defaultSQLiteCleanupInterval  = 5 * time.Minute
	defaultSQLiteJournalSizeLimit = int64(64 << 20)
	sqliteCacheSchemaVersion      = 2
	sqliteCleanupBatchSize        = 1000
	sqliteCleanupMaxBatches       = 8
)

var sqliteDatabases = struct {
	sync.Mutex
	items map[string]*sqliteDatabase
}{items: make(map[string]*sqliteDatabase)}

type sqliteOptions struct {
	path             string
	cacheSizeKiB     int64
	readConnections  int
	busyTimeout      time.Duration
	cleanupInterval  time.Duration
	journalSizeLimit int64
}

type sqliteDatabase struct {
	writer           *sql.DB
	readers          *sql.DB
	path             string
	instanceKey      string
	logger           *zap.Logger
	operationTimeout time.Duration
	cleanupInterval  time.Duration
	stopCleanup      chan struct{}
	closeOnce        sync.Once
	refs             int
}

type sqliteResponseStore struct {
	*sqliteDatabase
	releaseOnce sync.Once
}

type cacheEntry struct {
	BaseKey     string
	VaryKey     string
	VaryHeaders []string
	Status      int
	Header      http.Header
	Body        []byte
	StoredAt    time.Time
	ExpiresAt   time.Time
}

func newSQLiteResponseStore(options sqliteOptions, logger *zap.Logger) (*sqliteResponseStore, error) {
	if options.path == "" {
		options.path = defaultSQLitePath
	}
	if options.readConnections == 0 {
		options.readConnections = defaultSQLiteReadConnections
	}
	if options.readConnections < 1 || options.readConnections > maxSQLiteReadConnections {
		return nil, fmt.Errorf(
			"SQLite cache read_connections must be between 1 and %d",
			maxSQLiteReadConnections,
		)
	}
	if options.busyTimeout == 0 {
		options.busyTimeout = defaultSQLiteBusyTimeout
	}
	if options.busyTimeout < 0 {
		return nil, errors.New("SQLite cache busy_timeout must be positive")
	}
	if options.cleanupInterval == 0 {
		options.cleanupInterval = defaultSQLiteCleanupInterval
	}
	if options.cleanupInterval < 0 {
		return nil, errors.New("SQLite cache cleanup_interval must be positive")
	}
	if options.journalSizeLimit == 0 {
		options.journalSizeLimit = defaultSQLiteJournalSizeLimit
	}
	if options.journalSizeLimit < 0 {
		return nil, errors.New("SQLite cache journal_size_limit must be positive")
	}
	if options.cacheSizeKiB < 0 {
		return nil, errors.New("SQLite cache cache_size_kib must not be negative")
	}

	absolutePath, err := filepath.Abs(options.path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite cache path %q: %w", options.path, err)
	}
	options.path = filepath.Clean(absolutePath)
	instanceKey := fmt.Sprintf(
		"%s|%d|%d|%s|%s|%d",
		options.path,
		options.cacheSizeKiB,
		options.readConnections,
		options.busyTimeout,
		options.cleanupInterval,
		options.journalSizeLimit,
	)

	sqliteDatabases.Lock()
	database := sqliteDatabases.items[instanceKey]
	if database == nil {
		database, err = openSQLiteDatabase(options, instanceKey, logger)
		if err == nil {
			sqliteDatabases.items[instanceKey] = database
		}
	} else {
		database.refs++
	}
	sqliteDatabases.Unlock()
	if err != nil {
		return nil, err
	}

	return &sqliteResponseStore{sqliteDatabase: database}, nil
}

func openSQLiteDatabase(
	options sqliteOptions,
	instanceKey string,
	logger *zap.Logger,
) (*sqliteDatabase, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if err := os.MkdirAll(filepath.Dir(options.path), 0o750); err != nil {
		return nil, fmt.Errorf("create SQLite cache directory: %w", err)
	}

	writer, err := sql.Open("sqlite", sqliteConnectionDSN(options, false))
	if err != nil {
		return nil, fmt.Errorf("open SQLite cache: %w", err)
	}
	// SQLite only supports one writer. Keep exactly one write connection so
	// database/sql queues mutations instead of creating busy-retry storms.
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	if err := writer.Ping(); err != nil {
		writer.Close()
		return nil, fmt.Errorf("connect SQLite cache writer: %w", err)
	}
	if _, err := writer.Exec("PRAGMA journal_mode=WAL"); err != nil {
		writer.Close()
		return nil, fmt.Errorf("enable SQLite WAL mode: %w", err)
	}
	if err := initializeSQLiteSchema(writer); err != nil {
		writer.Close()
		return nil, err
	}

	// WAL permits reads to proceed while the writer commits. Connection-local
	// PRAGMAs are part of the DSN so every lazy database/sql connection gets
	// the same bounded settings.
	readers, err := sql.Open("sqlite", sqliteConnectionDSN(options, true))
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("open SQLite cache readers: %w", err)
	}
	readers.SetMaxOpenConns(options.readConnections)
	readers.SetMaxIdleConns(options.readConnections)
	if err := readers.Ping(); err != nil {
		readers.Close()
		writer.Close()
		return nil, fmt.Errorf("connect SQLite cache readers: %w", err)
	}

	result := &sqliteDatabase{
		writer:           writer,
		readers:          readers,
		path:             options.path,
		instanceKey:      instanceKey,
		logger:           logger,
		operationTimeout: options.busyTimeout,
		cleanupInterval:  options.cleanupInterval,
		stopCleanup:      make(chan struct{}),
		refs:             1,
	}
	if result.cleanupInterval > 0 {
		go result.cleanupLoop()
	}
	return result, nil
}

func initializeSQLiteSchema(database *sql.DB) error {
	var version int
	if err := database.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite cache schema version: %w", err)
	}
	tx, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin SQLite cache schema migration: %w", err)
	}
	defer tx.Rollback()

	// Cache data is disposable. Recreate the table when upgrading from the
	// former Souin key/value layout instead of carrying a migration path for
	// expired response data.
	if version != sqliteCacheSchemaVersion {
		if _, err := tx.Exec("DROP TABLE IF EXISTS cache_entries"); err != nil {
			return fmt.Errorf("reset SQLite cache schema: %w", err)
		}
	}
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS cache_entries (
			base_key TEXT NOT NULL,
			vary_key TEXT NOT NULL,
			vary_headers BLOB NOT NULL,
			status INTEGER NOT NULL,
			headers BLOB NOT NULL,
			body BLOB NOT NULL,
			stored_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			PRIMARY KEY (base_key, vary_key)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS cache_entries_expiry
			ON cache_entries(expires_at);
	`); err != nil {
		return fmt.Errorf("initialize SQLite cache schema: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", sqliteCacheSchemaVersion)); err != nil {
		return fmt.Errorf("write SQLite cache schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite cache schema: %w", err)
	}
	return nil
}

func sqliteConnectionDSN(options sqliteOptions, readOnly bool) string {
	query := make(url.Values)
	query.Set("_busy_timeout", strconv.FormatInt(options.busyTimeout.Milliseconds(), 10))
	if options.cacheSizeKiB > 0 {
		query.Add("_pragma", fmt.Sprintf("cache_size(-%d)", options.cacheSizeKiB))
	}
	query.Add("_pragma", "mmap_size(0)")
	query.Add("_pragma", "temp_store(FILE)")
	if readOnly {
		query.Set("_query_only", "1")
	} else {
		query.Set("_synchronous", "NORMAL")
		query.Add("_pragma", fmt.Sprintf("journal_size_limit(%d)", options.journalSizeLimit))
		query.Add("_pragma", "wal_autocheckpoint(1000)")
	}
	return (&url.URL{Scheme: "file", Path: options.path, RawQuery: query.Encode()}).String()
}

func (store *sqliteResponseStore) Get(
	parent context.Context,
	baseKey string,
	request *http.Request,
) (*cacheEntry, error) {
	ctx, cancel := store.operationContext(parent)
	defer cancel()

	rows, err := store.readers.QueryContext(
		ctx,
		`SELECT vary_key, vary_headers, status, headers, body, stored_at, expires_at
		 FROM cache_entries
		 WHERE base_key = ? AND expires_at > ?`,
		baseKey,
		time.Now().UnixMilli(),
	)
	if err != nil {
		return nil, fmt.Errorf("read SQLite cache entry: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry cacheEntry
		var varyHeadersJSON []byte
		var headersJSON []byte
		var storedAt int64
		var expiresAt int64
		if err := rows.Scan(
			&entry.VaryKey,
			&varyHeadersJSON,
			&entry.Status,
			&headersJSON,
			&entry.Body,
			&storedAt,
			&expiresAt,
		); err != nil {
			return nil, fmt.Errorf("decode SQLite cache entry: %w", err)
		}
		if err := json.Unmarshal(varyHeadersJSON, &entry.VaryHeaders); err != nil {
			return nil, fmt.Errorf("decode SQLite cache Vary fields: %w", err)
		}
		if entry.VaryKey != requestVaryKey(request, entry.VaryHeaders) {
			continue
		}
		if err := json.Unmarshal(headersJSON, &entry.Header); err != nil {
			return nil, fmt.Errorf("decode SQLite cache headers: %w", err)
		}
		entry.BaseKey = baseKey
		entry.StoredAt = time.UnixMilli(storedAt)
		entry.ExpiresAt = time.UnixMilli(expiresAt)
		return &entry, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite cache entries: %w", err)
	}
	return nil, nil
}

func (store *sqliteResponseStore) Put(parent context.Context, entry *cacheEntry) error {
	varyHeadersJSON, err := json.Marshal(entry.VaryHeaders)
	if err != nil {
		return fmt.Errorf("encode SQLite cache Vary fields: %w", err)
	}
	headersJSON, err := json.Marshal(entry.Header)
	if err != nil {
		return fmt.Errorf("encode SQLite cache headers: %w", err)
	}

	ctx, cancel := store.operationContext(parent)
	defer cancel()
	tx, err := store.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite cache write: %w", err)
	}
	defer tx.Rollback()

	// A response changing its Vary fields invalidates variants created under
	// the old dimensions; otherwise an older variant could match first.
	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM cache_entries WHERE base_key = ? AND vary_headers <> ?",
		entry.BaseKey,
		varyHeadersJSON,
	); err != nil {
		return fmt.Errorf("remove obsolete SQLite cache variants: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO cache_entries(
			base_key, vary_key, vary_headers, status, headers, body, stored_at, expires_at
		 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(base_key, vary_key) DO UPDATE SET
			vary_headers = excluded.vary_headers,
			status = excluded.status,
			headers = excluded.headers,
			body = excluded.body,
			stored_at = excluded.stored_at,
			expires_at = excluded.expires_at`,
		entry.BaseKey,
		entry.VaryKey,
		varyHeadersJSON,
		entry.Status,
		headersJSON,
		entry.Body,
		entry.StoredAt.UnixMilli(),
		entry.ExpiresAt.UnixMilli(),
	); err != nil {
		return fmt.Errorf("store SQLite cache entry: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite cache write: %w", err)
	}
	return nil
}

func (store *sqliteResponseStore) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, store.operationTimeout)
}

func (database *sqliteDatabase) cleanupLoop() {
	ticker := time.NewTicker(database.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-database.stopCleanup:
			return
		case <-ticker.C:
			database.deleteExpiredEntries()
		}
	}
}

func (database *sqliteDatabase) deleteExpiredEntries() {
	for batch := 0; batch < sqliteCleanupMaxBatches; batch++ {
		ctx, cancel := context.WithTimeout(context.Background(), database.operationTimeout)
		result, err := database.writer.ExecContext(
			ctx,
			`DELETE FROM cache_entries
			 WHERE (base_key, vary_key) IN (
				SELECT base_key, vary_key FROM cache_entries
				WHERE expires_at <= ?
				ORDER BY expires_at
				LIMIT ?
			 )`,
			time.Now().UnixMilli(),
			sqliteCleanupBatchSize,
		)
		cancel()
		if err != nil {
			database.logger.Error("clean expired SQLite cache entries", zap.Error(err))
			return
		}
		removed, err := result.RowsAffected()
		if err != nil || removed < sqliteCleanupBatchSize {
			return
		}
	}
}

func (store *sqliteResponseStore) Close() error {
	var closeErr error
	store.releaseOnce.Do(func() {
		sqliteDatabases.Lock()
		store.refs--
		remaining := store.refs
		if remaining == 0 {
			delete(sqliteDatabases.items, store.instanceKey)
		}
		sqliteDatabases.Unlock()
		if remaining > 0 {
			return
		}

		store.closeOnce.Do(func() {
			close(store.stopCleanup)
			closeErr = errors.Join(store.readers.Close(), store.writer.Close())
		})
	})
	return closeErr
}
