package caddystorage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/pierrec/lz4/v4"
	_ "modernc.org/sqlite"
)

const (
	defaultSQLitePath                = "souin-cache.db"
	defaultSQLiteCacheSizeKiB        = int64(4 << 10)
	defaultSQLiteReadConnections     = 4
	maxSQLiteReadConnections         = 16
	defaultSQLiteBusyTimeout         = 5 * time.Second
	defaultSQLiteCleanupInterval     = 5 * time.Minute
	defaultSQLiteMappingScanInterval = time.Minute
	defaultSQLiteJournalSizeLimit    = int64(64 << 20)
	sqliteCleanupBatchSize           = 1000
	sqliteCleanupMaxBatches          = 8
)

var sqliteDatabases = struct {
	sync.Mutex
	items map[string]*sqliteDatabase
}{items: make(map[string]*sqliteDatabase)}

type sqliteOptions struct {
	path                string
	cacheSizeKiB        int64
	readConnections     int
	busyTimeout         time.Duration
	cleanupInterval     time.Duration
	mappingScanInterval time.Duration
	journalSizeLimit    int64
}

type sqliteDatabase struct {
	writer              *sql.DB
	readers             *sql.DB
	path                string
	instanceKey         string
	logger              core.Logger
	operationTimeout    time.Duration
	cleanupInterval     time.Duration
	mappingScanInterval time.Duration
	stopCleanup         chan struct{}
	closeOnce           sync.Once
	closed              atomic.Bool
	refs                int
	mappingScanMu       sync.Mutex
	nextMappingScan     time.Time
}

type sqliteStorer struct {
	*sqliteDatabase
	stale       time.Duration
	name        string
	uuid        string
	releaseOnce sync.Once
}

func newSQLiteStorer(
	provider core.CacheProvider,
	logger core.Logger,
	stale time.Duration,
	name string,
	uuid func(sqliteOptions) string,
) (*sqliteStorer, error) {
	options, err := parseSQLiteOptions(provider)
	if err != nil {
		return nil, err
	}

	absolutePath, err := filepath.Abs(options.path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite cache path %q: %w", options.path, err)
	}
	options.path = filepath.Clean(absolutePath)
	instanceKey := fmt.Sprintf(
		"%s|%d|%d|%s|%s|%s|%d",
		options.path,
		options.cacheSizeKiB,
		options.readConnections,
		options.busyTimeout,
		options.cleanupInterval,
		options.mappingScanInterval,
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

	return &sqliteStorer{
		sqliteDatabase: database,
		stale:          stale,
		name:           name,
		uuid:           uuid(options),
	}, nil
}

func openSQLiteDatabase(options sqliteOptions, instanceKey string, logger core.Logger) (*sqliteDatabase, error) {
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

	if _, err := writer.Exec(`
		CREATE TABLE IF NOT EXISTS cache_entries (
			key TEXT PRIMARY KEY NOT NULL,
			value BLOB NOT NULL,
			expires_at INTEGER NOT NULL DEFAULT 0
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS cache_entries_expiry
			ON cache_entries(expires_at)
			WHERE expires_at > 0;
	`); err != nil {
		writer.Close()
		return nil, fmt.Errorf("initialize SQLite cache schema: %w", err)
	}

	// WAL permits reads to proceed while the writer commits. Give reads a
	// small pool, and apply connection-local PRAGMAs through the DSN so every
	// lazily opened database/sql connection receives the same memory limits.
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
		writer:              writer,
		readers:             readers,
		path:                options.path,
		instanceKey:         instanceKey,
		logger:              logger,
		operationTimeout:    options.busyTimeout,
		cleanupInterval:     options.cleanupInterval,
		mappingScanInterval: options.mappingScanInterval,
		stopCleanup:         make(chan struct{}),
		refs:                1,
	}
	if result.cleanupInterval > 0 {
		go result.cleanupLoop()
	}
	return result, nil
}

func sqliteConnectionDSN(options sqliteOptions, readOnly bool) string {
	query := make(url.Values)
	query.Set("_busy_timeout", strconv.FormatInt(options.busyTimeout.Milliseconds(), 10))
	query.Add("_pragma", fmt.Sprintf("cache_size(-%d)", options.cacheSizeKiB))
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

func parseSQLiteOptions(provider core.CacheProvider) (sqliteOptions, error) {
	options := sqliteOptions{
		path:                configuredSQLitePath(provider),
		cacheSizeKiB:        defaultSQLiteCacheSizeKiB,
		readConnections:     defaultSQLiteReadConnections,
		busyTimeout:         defaultSQLiteBusyTimeout,
		cleanupInterval:     defaultSQLiteCleanupInterval,
		mappingScanInterval: defaultSQLiteMappingScanInterval,
		journalSizeLimit:    defaultSQLiteJournalSizeLimit,
	}
	if options.path == "" {
		options.path = defaultSQLitePath
	}

	configuration, _ := provider.Configuration.(map[string]interface{})
	var err error
	if value, found := lookupConfiguration(configuration, "cache_size_kib", "CacheSizeKiB"); found {
		options.cacheSizeKiB, err = positiveInt64(value, "cache_size_kib")
		if err != nil {
			return options, err
		}
	}
	if value, found := lookupConfiguration(configuration, "read_connections", "ReadConnections"); found {
		var parsed int64
		parsed, err = positiveInt64(value, "read_connections")
		if err != nil {
			return options, err
		}
		if parsed > maxSQLiteReadConnections {
			return options, fmt.Errorf(
				"SQLite cache read_connections must not exceed %d",
				maxSQLiteReadConnections,
			)
		}
		options.readConnections = int(parsed)
	}
	if value, found := lookupConfiguration(configuration, "busy_timeout", "BusyTimeout"); found {
		options.busyTimeout, err = positiveDuration(value, "busy_timeout")
		if err != nil {
			return options, err
		}
	}
	if value, found := lookupConfiguration(configuration, "cleanup_interval", "CleanupInterval"); found {
		options.cleanupInterval, err = positiveDuration(value, "cleanup_interval")
		if err != nil {
			return options, err
		}
	}
	if value, found := lookupConfiguration(configuration, "mapping_scan_interval", "MappingScanInterval"); found {
		options.mappingScanInterval, err = positiveDuration(value, "mapping_scan_interval")
		if err != nil {
			return options, err
		}
	}
	if value, found := lookupConfiguration(configuration, "journal_size_limit", "JournalSizeLimit"); found {
		options.journalSizeLimit, err = positiveInt64(value, "journal_size_limit")
		if err != nil {
			return options, err
		}
	}
	return options, nil
}

func configuredSQLitePath(provider core.CacheProvider) string {
	if path := strings.TrimSpace(provider.Path); path != "" {
		return path
	}
	configuration, _ := provider.Configuration.(map[string]interface{})
	if value, found := lookupConfiguration(configuration, "path", "Path"); found {
		if path, ok := value.(string); ok {
			return strings.TrimSpace(path)
		}
	}
	return ""
}

func configuredSimpleFSSize(provider core.CacheProvider) int64 {
	configuration, _ := provider.Configuration.(map[string]interface{})
	value, found := lookupConfiguration(configuration, "size")
	if !found {
		return 0
	}
	size, err := int64Value(value)
	if err != nil || size < 0 {
		return 0
	}
	return size
}

func lookupConfiguration(configuration map[string]interface{}, names ...string) (interface{}, bool) {
	for _, name := range names {
		if value, found := configuration[name]; found && value != nil {
			return value, true
		}
	}
	return nil, false
}

func positiveInt64(value interface{}, field string) (int64, error) {
	parsed, err := int64Value(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("SQLite cache %s must be a positive integer", field)
	}
	return parsed, nil
}

func int64Value(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, errors.New("integer overflows int64")
		}
		return int64(typed), nil
	case float64:
		if typed != float64(int64(typed)) {
			return 0, errors.New("not an integer")
		}
		return int64(typed), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer type %T", value)
	}
}

func positiveDuration(value interface{}, field string) (time.Duration, error) {
	var duration time.Duration
	var err error
	switch typed := value.(type) {
	case string:
		duration, err = time.ParseDuration(strings.TrimSpace(typed))
	default:
		var milliseconds int64
		milliseconds, err = int64Value(value)
		duration = time.Duration(milliseconds) * time.Millisecond
	}
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("SQLite cache %s must be a positive duration", field)
	}
	return duration, nil
}

func (provider *sqliteStorer) Name() string {
	return provider.name
}

func (provider *sqliteStorer) Uuid() string {
	return provider.uuid
}

func (provider *sqliteStorer) Init() error {
	return nil
}

func (provider *sqliteStorer) Get(key string) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()

	var value []byte
	var expiresAt int64
	err := provider.readers.QueryRowContext(
		ctx,
		"SELECT value, expires_at FROM cache_entries WHERE key = ?",
		key,
	).Scan(&value, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		provider.logger.Errorf("read SQLite cache key %s: %v", key, err)
		return nil
	}
	if expiresAt > 0 && expiresAt <= time.Now().UnixMilli() {
		provider.deleteKey(ctx, key)
		return nil
	}
	return value
}

func (provider *sqliteStorer) Set(key string, value []byte, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()
	if err := upsertSQLiteEntry(ctx, provider.writer, key, value, expiration(duration)); err != nil {
		provider.logger.Errorf("write SQLite cache key %s: %v", key, err)
		return err
	}
	return nil
}

func (provider *sqliteStorer) Delete(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()
	provider.deleteKey(ctx, key)
}

func (provider *sqliteStorer) deleteKey(ctx context.Context, key string) {
	if _, err := provider.writer.ExecContext(ctx, "DELETE FROM cache_entries WHERE key = ?", key); err != nil {
		provider.logger.Errorf("delete SQLite cache key %s: %v", key, err)
	}
}

func (provider *sqliteStorer) MapKeys(prefix string) map[string]string {
	// Souin v1.7.7 leaks its eviction goroutine across handler cleanup. Once
	// the final module reference closes this database, make those old workers
	// harmless and quiet until the process exits.
	if provider.closed.Load() {
		return map[string]string{}
	}
	if prefix == core.MappingKeyPrefix && !provider.beginMappingScan() {
		return map[string]string{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()
	query := `SELECT key, value FROM cache_entries
		WHERE key >= ? AND key < ? AND (expires_at = 0 OR expires_at > ?)`
	upper := prefixUpperBound(prefix)
	rows, err := provider.readers.QueryContext(ctx, query, prefix, upper, time.Now().UnixMilli())
	if err != nil {
		provider.logger.Errorf("scan SQLite cache prefix %s: %v", prefix, err)
		provider.resetMappingScanAfterError(prefix)
		return map[string]string{}
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			provider.logger.Errorf("decode SQLite cache prefix %s: %v", prefix, err)
			continue
		}
		result[strings.TrimPrefix(key, prefix)] = string(value)
	}
	if err := rows.Err(); err != nil {
		provider.logger.Errorf("iterate SQLite cache prefix %s: %v", prefix, err)
	}
	return result
}

func (provider *sqliteStorer) beginMappingScan() bool {
	now := time.Now()
	provider.mappingScanMu.Lock()
	defer provider.mappingScanMu.Unlock()
	if now.Before(provider.nextMappingScan) {
		return false
	}
	provider.nextMappingScan = now.Add(provider.mappingScanInterval)
	return true
}

func (provider *sqliteStorer) resetMappingScanAfterError(prefix string) {
	if prefix != core.MappingKeyPrefix {
		return
	}
	provider.mappingScanMu.Lock()
	provider.nextMappingScan = time.Time{}
	provider.mappingScanMu.Unlock()
}

func prefixUpperBound(prefix string) string {
	if prefix == "" {
		return string([]byte{0xff})
	}
	bytes := []byte(prefix)
	for index := len(bytes) - 1; index >= 0; index-- {
		if bytes[index] != 0xff {
			bytes[index]++
			return string(bytes[:index+1])
		}
	}
	return prefix + string([]byte{0xff})
}

func (provider *sqliteStorer) ListKeys() []string {
	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()
	rows, err := provider.readers.QueryContext(
		ctx,
		`SELECT value FROM cache_entries
		 WHERE key >= ? AND key < ? AND (expires_at = 0 OR expires_at > ?)`,
		core.MappingKeyPrefix,
		prefixUpperBound(core.MappingKeyPrefix),
		time.Now().UnixMilli(),
	)
	if err != nil {
		provider.logger.Errorf("list SQLite cache keys: %v", err)
		return nil
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			continue
		}
		mapping, err := core.DecodeMapping(value)
		if err != nil {
			continue
		}
		for _, item := range mapping.GetMapping() {
			keys = append(keys, item.GetRealKey())
		}
	}
	return keys
}

func (provider *sqliteStorer) DeleteMany(pattern string) {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()

	rows, err := provider.readers.QueryContext(ctx, "SELECT key FROM cache_entries")
	if err != nil {
		provider.logger.Errorf("list SQLite cache keys for deletion: %v", err)
		return
	}
	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil && compiled.MatchString(key) {
			keys = append(keys, key)
		}
	}
	rows.Close()
	if len(keys) == 0 {
		return
	}

	tx, err := provider.writer.BeginTx(ctx, nil)
	if err != nil {
		provider.logger.Errorf("begin SQLite cache deletion: %v", err)
		return
	}
	statement, err := tx.PrepareContext(ctx, "DELETE FROM cache_entries WHERE key = ?")
	if err != nil {
		tx.Rollback()
		provider.logger.Errorf("prepare SQLite cache deletion: %v", err)
		return
	}
	for _, key := range keys {
		if _, err := statement.ExecContext(ctx, key); err != nil {
			statement.Close()
			tx.Rollback()
			provider.logger.Errorf("delete SQLite cache key %s: %v", key, err)
			return
		}
	}
	statement.Close()
	if err := tx.Commit(); err != nil {
		provider.logger.Errorf("commit SQLite cache deletion: %v", err)
	}
}

func (provider *sqliteStorer) GetMultiLevel(
	key string,
	request *http.Request,
	validator *core.Revalidator,
) (fresh *http.Response, stale *http.Response) {
	mapping := provider.Get(core.MappingKeyPrefix + key)
	if len(mapping) == 0 {
		return nil, nil
	}
	fresh, stale, err := core.MappingElection(provider, mapping, request, validator, provider.logger)
	if err != nil {
		provider.logger.Errorf("select SQLite cache mapping %s: %v", key, err)
	}
	return fresh, stale
}

func (provider *sqliteStorer) SetMultiLevel(
	baseKey string,
	variedKey string,
	value []byte,
	variedHeaders http.Header,
	etag string,
	duration time.Duration,
	realKey string,
) error {
	compressed := new(bytes.Buffer)
	writer := lz4.NewWriter(compressed)
	if _, err := writer.Write(value); err != nil {
		writer.Close()
		return fmt.Errorf("compress SQLite cache value %s: %w", variedKey, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("finish SQLite cache compression %s: %w", variedKey, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), provider.operationTimeout)
	defer cancel()
	tx, err := provider.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite cache write: %w", err)
	}
	defer tx.Rollback()

	if err := upsertSQLiteEntry(
		ctx,
		tx,
		variedKey,
		compressed.Bytes(),
		expiration(duration+provider.stale),
	); err != nil {
		return fmt.Errorf("store SQLite cache value %s: %w", variedKey, err)
	}

	mappingKey := core.MappingKeyPrefix + baseKey
	var mapping []byte
	var mappingExpiry int64
	err = tx.QueryRowContext(
		ctx,
		"SELECT value, expires_at FROM cache_entries WHERE key = ?",
		mappingKey,
	).Scan(&mapping, &mappingExpiry)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read SQLite cache mapping %s: %w", mappingKey, err)
	}
	if mappingExpiry > 0 && mappingExpiry <= time.Now().UnixMilli() {
		mapping = nil
	}

	now := time.Now()
	mapping, err = core.MappingUpdater(
		variedKey,
		mapping,
		provider.logger,
		now,
		now.Add(duration),
		now.Add(duration+provider.stale),
		variedHeaders,
		etag,
		realKey,
	)
	if err != nil {
		return fmt.Errorf("update SQLite cache mapping %s: %w", mappingKey, err)
	}
	if err := upsertSQLiteEntry(ctx, tx, mappingKey, mapping, 0); err != nil {
		return fmt.Errorf("store SQLite cache mapping %s: %w", mappingKey, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite cache write: %w", err)
	}
	return nil
}

type sqliteExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func upsertSQLiteEntry(
	ctx context.Context,
	execer sqliteExecer,
	key string,
	value []byte,
	expiresAt int64,
) error {
	_, err := execer.ExecContext(
		ctx,
		`INSERT INTO cache_entries(key, value, expires_at)
		 VALUES(?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			expires_at = excluded.expires_at`,
		key,
		value,
		expiresAt,
	)
	return err
}

func expiration(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return time.Now().Add(duration).UnixMilli()
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
			`DELETE FROM cache_entries WHERE key IN (
				SELECT key FROM cache_entries
				WHERE expires_at > 0 AND expires_at <= ?
				ORDER BY expires_at
				LIMIT ?
			)`,
			time.Now().UnixMilli(),
			sqliteCleanupBatchSize,
		)
		cancel()
		if err != nil {
			database.logger.Errorf("clean expired SQLite cache entries: %v", err)
			return
		}
		removed, err := result.RowsAffected()
		if err != nil || removed < sqliteCleanupBatchSize {
			return
		}
	}
}

func (provider *sqliteStorer) Reset() error {
	var resetErr error
	provider.releaseOnce.Do(func() {
		sqliteDatabases.Lock()
		provider.refs--
		remaining := provider.refs
		if remaining == 0 {
			delete(sqliteDatabases.items, provider.instanceKey)
		}
		sqliteDatabases.Unlock()
		if remaining > 0 {
			return
		}

		provider.closeOnce.Do(func() {
			provider.closed.Store(true)
			close(provider.stopCleanup)
			resetErr = errors.Join(provider.readers.Close(), provider.writer.Close())
		})
	})
	return resetErr
}

var _ core.Storer = (*sqliteStorer)(nil)
