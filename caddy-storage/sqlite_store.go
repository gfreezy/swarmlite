package caddystorage

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	_ "modernc.org/sqlite"
)

const (
	defaultSQLitePath             = "/cache/native-v1/cache.db"
	defaultSQLiteReadConnections  = 4
	maxSQLiteReadConnections      = 16
	defaultSQLiteBusyTimeout      = 5 * time.Second
	defaultSQLiteCleanupInterval  = 5 * time.Minute
	defaultSQLiteJournalSizeLimit = int64(64 << 20)
	defaultSQLiteMaxSizeBytes     = int64(1 << 30)
	defaultSQLiteLowWaterPercent  = 90
	defaultCacheHitSampleRatio    = uint64(32)
	defaultCacheAccessInterval    = 5 * time.Minute
	sqliteCacheSchemaVersion      = 5
	// A 128-bit SHA-256 prefix keeps cache identities compact while retaining
	// negligible collision probability at HTTP-cache scale.
	cacheKeySize            = 16
	sqliteCleanupBatchSize  = 1000
	sqliteCleanupMaxBatches = 8
	sqliteEvictionBatchSize = 512
	cacheTouchQueueSize     = 4096
	cacheTouchBatchSize     = 256
	cacheTouchFlushInterval = time.Second
	cacheTouchBloomWords    = 8192 // 64 KiB / 524,288 bits
	cacheTouchBloomHashes   = 4
)

var sqliteDatabases = struct {
	sync.Mutex
	items map[string]*sqliteDatabase
}{items: make(map[string]*sqliteDatabase)}

type sqliteOptions struct {
	path             string
	maxSizeBytes     int64
	lowWaterPercent  int
	hitSampleRatio   uint64
	accessInterval   time.Duration
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
	maxSizeBytes     int64
	lowWaterBytes    int64
	hitSampleRatio   uint64
	accessInterval   time.Duration
	touchQueue       chan cacheKey
	touchBloom       cacheTouchBloom
	touchSequence    atomic.Uint64
	evict            chan struct{}
	stopCleanup      chan struct{}
	background       sync.WaitGroup
	closeOnce        sync.Once
	refs             int
}

type sqliteResponseStore struct {
	*sqliteDatabase
	releaseOnce sync.Once
}

type cacheKey [cacheKeySize]byte

type cacheTouchBloom struct {
	sync.Mutex
	words [cacheTouchBloomWords]uint64
}

type cacheEntry struct {
	Key         cacheKey
	BaseKey     cacheKey
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
	if options.maxSizeBytes == 0 {
		options.maxSizeBytes = defaultSQLiteMaxSizeBytes
	}
	if options.maxSizeBytes < 0 {
		return nil, errors.New("SQLite cache max_size_bytes must be positive")
	}
	if options.lowWaterPercent == 0 {
		options.lowWaterPercent = defaultSQLiteLowWaterPercent
	}
	if options.lowWaterPercent < 1 || options.lowWaterPercent >= 100 {
		return nil, errors.New("SQLite cache low_water_percent must be between 1 and 99")
	}
	if options.hitSampleRatio == 0 {
		options.hitSampleRatio = defaultCacheHitSampleRatio
	}
	if options.accessInterval == 0 {
		options.accessInterval = defaultCacheAccessInterval
	}
	if options.accessInterval < 0 {
		return nil, errors.New("SQLite cache access_update_interval must be positive")
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
		"%s|%d|%d|%d|%s|%d|%d|%s|%s|%d",
		options.path,
		options.maxSizeBytes,
		options.lowWaterPercent,
		options.hitSampleRatio,
		options.accessInterval,
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
		maxSizeBytes:     options.maxSizeBytes,
		lowWaterBytes:    percentageBytes(options.maxSizeBytes, options.lowWaterPercent),
		hitSampleRatio:   options.hitSampleRatio,
		accessInterval:   options.accessInterval,
		touchQueue:       make(chan cacheKey, cacheTouchQueueSize),
		evict:            make(chan struct{}, 1),
		stopCleanup:      make(chan struct{}),
		refs:             1,
	}
	result.background.Add(1)
	go result.cleanupLoop()
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

	// Cache data is disposable. Recreate the tables when upgrading from the
	// former Souin key/value layout instead of carrying a migration path for
	// expired response data.
	if version != sqliteCacheSchemaVersion {
		if _, err := tx.Exec(`
			DROP TABLE IF EXISTS cache_entries;
			DROP TABLE IF EXISTS cache_resources;
			DROP TABLE IF EXISTS cache_access;
			DROP TABLE IF EXISTS cache_usage;
		`); err != nil {
			return fmt.Errorf("reset SQLite cache schema: %w", err)
		}
	}
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS cache_resources (
			base_key BLOB PRIMARY KEY NOT NULL,
			vary_headers BLOB NOT NULL,
			generation INTEGER NOT NULL,
			expires_at INTEGER NOT NULL
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS cache_resources_expiry
			ON cache_resources(expires_at);
		CREATE TABLE IF NOT EXISTS cache_entries (
			cache_key BLOB PRIMARY KEY NOT NULL,
			status INTEGER NOT NULL,
			headers BLOB NOT NULL,
			body BLOB NOT NULL,
			stored_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS cache_entries_expiry
			ON cache_entries(expires_at);
		CREATE TABLE IF NOT EXISTS cache_access (
			cache_key BLOB PRIMARY KEY NOT NULL,
			accessed_at INTEGER NOT NULL,
			size_bytes INTEGER NOT NULL
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS cache_access_lru
			ON cache_access(accessed_at, cache_key);
		CREATE TABLE IF NOT EXISTS cache_usage (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			total_bytes INTEGER NOT NULL,
			entry_count INTEGER NOT NULL
		);
		INSERT OR IGNORE INTO cache_usage(id, total_bytes, entry_count)
			VALUES(1, 0, 0);
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
	baseKey cacheKey,
	request *http.Request,
) (*cacheEntry, error) {
	ctx, cancel := store.operationContext(parent)
	defer cancel()
	tx, err := store.readers.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin SQLite cache read: %w", err)
	}
	defer tx.Rollback()

	var varyHeadersJSON []byte
	var generation int64
	err = tx.QueryRowContext(
		ctx,
		`SELECT vary_headers, generation
		 FROM cache_resources
		 WHERE base_key = ? AND expires_at > ?`,
		baseKey[:],
		time.Now().UnixMilli(),
	).Scan(&varyHeadersJSON, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read SQLite cache resource: %w", err)
	}
	if generation < 1 {
		return nil, fmt.Errorf("decode SQLite cache resource generation: %d", generation)
	}
	var varyHeaders []string
	if err := json.Unmarshal(varyHeadersJSON, &varyHeaders); err != nil {
		return nil, fmt.Errorf("decode SQLite cache Vary fields: %w", err)
	}

	key := responseCacheKey(baseKey, generation, request, varyHeaders)
	entry := cacheEntry{
		Key:         key,
		BaseKey:     baseKey,
		VaryHeaders: varyHeaders,
	}
	var headersJSON []byte
	var storedAt int64
	var expiresAt int64
	err = tx.QueryRowContext(
		ctx,
		`SELECT status, headers, body, stored_at, expires_at
		 FROM cache_entries
		 WHERE cache_key = ? AND expires_at > ?`,
		key[:],
		time.Now().UnixMilli(),
	).Scan(
		&entry.Status,
		&headersJSON,
		&entry.Body,
		&storedAt,
		&expiresAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read SQLite cache entry: %w", err)
	}
	if err := json.Unmarshal(headersJSON, &entry.Header); err != nil {
		return nil, fmt.Errorf("decode SQLite cache headers: %w", err)
	}
	entry.StoredAt = time.UnixMilli(storedAt)
	entry.ExpiresAt = time.UnixMilli(expiresAt)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit SQLite cache read: %w", err)
	}
	store.recordCacheHit(key)
	return &entry, nil
}

func (store *sqliteResponseStore) Put(
	parent context.Context,
	request *http.Request,
	entry *cacheEntry,
) error {
	varyHeadersJSON, err := json.Marshal(entry.VaryHeaders)
	if err != nil {
		return fmt.Errorf("encode SQLite cache Vary fields: %w", err)
	}
	headersJSON, err := json.Marshal(entry.Header)
	if err != nil {
		return fmt.Errorf("encode SQLite cache headers: %w", err)
	}
	entrySize := cacheEntrySizeBytes(headersJSON, entry.Body)
	if entrySize > store.maxSizeBytes {
		return nil
	}

	ctx, cancel := store.operationContext(parent)
	defer cancel()
	tx, err := store.writer.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite cache write: %w", err)
	}
	defer tx.Rollback()

	// One resource has one current Vary policy. A policy change advances its
	// generation, making entries under the former dimensions unreachable until
	// ordinary expiry cleanup removes them.
	var generation int64
	err = tx.QueryRowContext(
		ctx,
		`INSERT INTO cache_resources(base_key, vary_headers, generation, expires_at)
		 VALUES(?, ?, 1, ?)
		 ON CONFLICT(base_key) DO UPDATE SET
			generation = cache_resources.generation +
				CASE WHEN cache_resources.vary_headers = excluded.vary_headers THEN 0 ELSE 1 END,
			vary_headers = excluded.vary_headers,
			expires_at = max(cache_resources.expires_at, excluded.expires_at)
		 RETURNING generation`,
		entry.BaseKey[:],
		varyHeadersJSON,
		entry.ExpiresAt.UnixMilli(),
	).Scan(&generation)
	if err != nil {
		return fmt.Errorf("store SQLite cache resource: %w", err)
	}
	entry.Key = responseCacheKey(entry.BaseKey, generation, request, entry.VaryHeaders)
	var formerSize int64
	entryExists := true
	err = tx.QueryRowContext(
		ctx,
		"SELECT size_bytes FROM cache_access WHERE cache_key = ?",
		entry.Key[:],
	).Scan(&formerSize)
	if errors.Is(err, sql.ErrNoRows) {
		entryExists = false
		formerSize = 0
	} else if err != nil {
		return fmt.Errorf("read SQLite cache entry size: %w", err)
	}
	var totalBytes int64
	if err := tx.QueryRowContext(
		ctx,
		"SELECT total_bytes FROM cache_usage WHERE id = 1",
	).Scan(&totalBytes); err != nil {
		return fmt.Errorf("read SQLite cache usage: %w", err)
	}
	projectedBytes := totalBytes - formerSize + entrySize
	if projectedBytes > store.maxSizeBytes && entrySize > formerSize {
		store.signalEviction()
		return nil
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO cache_entries(
			cache_key, status, headers, body, stored_at, expires_at
		 ) VALUES(?, ?, ?, ?, ?, ?)
		 ON CONFLICT(cache_key) DO UPDATE SET
			status = excluded.status,
			headers = excluded.headers,
			body = excluded.body,
			stored_at = excluded.stored_at,
			expires_at = excluded.expires_at`,
		entry.Key[:],
		entry.Status,
		headersJSON,
		entry.Body,
		entry.StoredAt.UnixMilli(),
		entry.ExpiresAt.UnixMilli(),
	); err != nil {
		return fmt.Errorf("store SQLite cache entry: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO cache_access(cache_key, accessed_at, size_bytes)
		 VALUES(?, ?, ?)
		 ON CONFLICT(cache_key) DO UPDATE SET
			accessed_at = excluded.accessed_at,
			size_bytes = excluded.size_bytes`,
		entry.Key[:],
		entry.StoredAt.UnixMilli(),
		entrySize,
	); err != nil {
		return fmt.Errorf("store SQLite cache access metadata: %w", err)
	}
	entryCountDelta := 0
	if !entryExists {
		entryCountDelta = 1
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE cache_usage
		 SET total_bytes = ?, entry_count = entry_count + ?
		 WHERE id = 1`,
		projectedBytes,
		entryCountDelta,
	); err != nil {
		return fmt.Errorf("update SQLite cache usage: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite cache write: %w", err)
	}
	return nil
}

func cacheEntrySizeBytes(headersJSON, body []byte) int64 {
	// The capacity is a stable logical payload limit rather than the SQLite
	// file size, whose WAL and reusable pages vary independently.
	return int64(cacheKeySize + len(headersJSON) + len(body))
}

func percentageBytes(value int64, percent int) int64 {
	// Split the calculation so an allowed near-MaxInt64 capacity cannot
	// overflow before division.
	return value/100*int64(percent) + value%100*int64(percent)/100
}

func (store *sqliteResponseStore) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, store.operationTimeout)
}

func (database *sqliteDatabase) recordCacheHit(key cacheKey) {
	if database.touchSequence.Add(1)%database.hitSampleRatio != 0 {
		return
	}
	database.enqueueCacheTouch(key)
}

func (database *sqliteDatabase) enqueueCacheTouch(key cacheKey) {
	if !database.touchBloom.markIfNew(key) {
		return
	}
	select {
	case database.touchQueue <- key:
	default:
		// Access metadata is advisory. Dropping a touch under load can only make
		// this entry look slightly older during a later approximate-LRU pass.
	}
}

func (bloom *cacheTouchBloom) markIfNew(key cacheKey) bool {
	bloom.Lock()
	defer bloom.Unlock()
	hash1 := binary.LittleEndian.Uint64(key[:8])
	hash2 := binary.LittleEndian.Uint64(key[8:]) | 1
	alreadyPresent := true
	for index := uint64(0); index < cacheTouchBloomHashes; index++ {
		bit := (hash1 + index*hash2) % (cacheTouchBloomWords * 64)
		word := bit / 64
		mask := uint64(1) << (bit % 64)
		if bloom.words[word]&mask == 0 {
			alreadyPresent = false
			bloom.words[word] |= mask
		}
	}
	return !alreadyPresent
}

func (bloom *cacheTouchBloom) reset() {
	bloom.Lock()
	clear(bloom.words[:])
	bloom.Unlock()
}

func (database *sqliteDatabase) cleanupLoop() {
	defer database.background.Done()
	cleanupTicker := time.NewTicker(database.cleanupInterval)
	defer cleanupTicker.Stop()
	touchTicker := time.NewTicker(cacheTouchFlushInterval)
	defer touchTicker.Stop()
	bloomTicker := time.NewTicker(database.accessInterval)
	defer bloomTicker.Stop()
	for {
		select {
		case <-database.stopCleanup:
			database.flushAllCacheTouches()
			return
		case <-touchTicker.C:
			database.flushCacheTouches()
		case <-bloomTicker.C:
			database.touchBloom.reset()
		case <-cleanupTicker.C:
			database.flushCacheTouches()
			database.deleteExpiredEntries()
			database.enforceCapacity(false)
		case <-database.evict:
			database.deleteExpiredEntries()
			database.enforceCapacity(true)
		}
	}
}

func (database *sqliteDatabase) flushAllCacheTouches() {
	for len(database.touchQueue) > 0 {
		if !database.flushCacheTouches() {
			return
		}
	}
}

func (database *sqliteDatabase) flushCacheTouches() bool {
	keys := make([]cacheKey, 0, cacheTouchBatchSize)
	for len(keys) < cacheTouchBatchSize {
		select {
		case key := <-database.touchQueue:
			keys = append(keys, key)
		default:
			if len(keys) == 0 {
				return true
			}
			goto update
		}
	}

update:
	now := time.Now().UnixMilli()
	arguments := make([]any, 0, len(keys)+2)
	arguments = append(arguments, now, now-database.accessInterval.Milliseconds())
	for index := range keys {
		arguments = append(arguments, keys[index][:])
	}
	query := `UPDATE cache_access
		 SET accessed_at = ?
		 WHERE accessed_at < ? AND cache_key IN (` + placeholders(len(keys)) + `)`
	ctx, cancel := context.WithTimeout(context.Background(), database.operationTimeout)
	_, err := database.writer.ExecContext(ctx, query, arguments...)
	cancel()
	if err != nil {
		database.logger.Error("update SQLite cache access metadata", zap.Error(err))
		return false
	}
	return true
}

func (database *sqliteDatabase) deleteExpiredEntries() {
	now := time.Now().UnixMilli()
	for batch := 0; batch < sqliteCleanupMaxBatches; batch++ {
		removed, _, err := database.deleteEntryBatch(
			`SELECT access.cache_key, access.size_bytes
			 FROM cache_entries AS entry
			 JOIN cache_access AS access USING(cache_key)
			 WHERE entry.expires_at <= ?
			 ORDER BY entry.expires_at
			 LIMIT ?`,
			0,
			now,
			sqliteCleanupBatchSize,
		)
		if err != nil {
			database.logger.Error("clean expired SQLite cache entries", zap.Error(err))
			return
		}
		if removed < sqliteCleanupBatchSize {
			break
		}
	}
	database.deleteExpiredRows(
		"resources",
		`DELETE FROM cache_resources
		 WHERE base_key IN (
			SELECT base_key FROM cache_resources
			WHERE expires_at <= ?
			ORDER BY expires_at
			LIMIT ?
		 )`,
	)
}

func (database *sqliteDatabase) enforceCapacity(force bool) {
	totalBytes, err := database.cacheUsage()
	if err != nil {
		database.logger.Error("read SQLite cache usage for eviction", zap.Error(err))
		return
	}
	if (!force && totalBytes <= database.maxSizeBytes) ||
		(force && totalBytes <= database.lowWaterBytes) {
		return
	}
	for batch := 0; batch < sqliteCleanupMaxBatches && totalBytes > database.lowWaterBytes; batch++ {
		removed, removedBytes, err := database.deleteEntryBatch(
			`SELECT cache_key, size_bytes
			 FROM cache_access
			 ORDER BY accessed_at, cache_key
			 LIMIT ?`,
			totalBytes-database.lowWaterBytes,
			sqliteEvictionBatchSize,
		)
		if err != nil {
			database.logger.Error("evict SQLite cache entries by LRU", zap.Error(err))
			return
		}
		if removed == 0 {
			return
		}
		totalBytes = max(int64(0), totalBytes-removedBytes)
	}
	if totalBytes > database.lowWaterBytes {
		database.signalEviction()
	}
}

func (database *sqliteDatabase) cacheUsage() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), database.operationTimeout)
	defer cancel()
	var totalBytes int64
	if err := database.writer.QueryRowContext(
		ctx,
		"SELECT total_bytes FROM cache_usage WHERE id = 1",
	).Scan(&totalBytes); err != nil {
		return 0, err
	}
	return totalBytes, nil
}

func (database *sqliteDatabase) deleteEntryBatch(
	selection string,
	stopAfterBytes int64,
	selectionArguments ...any,
) (int, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), database.operationTimeout)
	defer cancel()
	tx, err := database.writer.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, selection, selectionArguments...)
	if err != nil {
		return 0, 0, err
	}
	keys := make([]cacheKey, 0, sqliteCleanupBatchSize)
	var removedBytes int64
	for rows.Next() {
		var encodedKey []byte
		var sizeBytes int64
		if err := rows.Scan(&encodedKey, &sizeBytes); err != nil {
			rows.Close()
			return 0, 0, err
		}
		if len(encodedKey) != cacheKeySize {
			rows.Close()
			return 0, 0, fmt.Errorf("invalid SQLite cache key length %d", len(encodedKey))
		}
		var key cacheKey
		copy(key[:], encodedKey)
		keys = append(keys, key)
		removedBytes += sizeBytes
		if stopAfterBytes > 0 && removedBytes >= stopAfterBytes {
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, 0, err
	}
	if len(keys) == 0 {
		return 0, 0, nil
	}
	arguments := make([]any, 0, len(keys))
	for index := range keys {
		arguments = append(arguments, keys[index][:])
	}
	for _, table := range []string{"cache_entries", "cache_access"} {
		if _, err := tx.ExecContext(
			ctx,
			"DELETE FROM "+table+" WHERE cache_key IN ("+placeholders(len(keys))+")",
			arguments...,
		); err != nil {
			return 0, 0, err
		}
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE cache_usage
		 SET total_bytes = max(0, total_bytes - ?),
		     entry_count = max(0, entry_count - ?)
		 WHERE id = 1`,
		removedBytes,
		len(keys),
	); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return len(keys), removedBytes, nil
}

func placeholders(count int) string {
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func (database *sqliteDatabase) signalEviction() {
	select {
	case database.evict <- struct{}{}:
	default:
	}
}

func (database *sqliteDatabase) deleteExpiredRows(kind, query string) {
	for batch := 0; batch < sqliteCleanupMaxBatches; batch++ {
		ctx, cancel := context.WithTimeout(context.Background(), database.operationTimeout)
		result, err := database.writer.ExecContext(
			ctx,
			query,
			time.Now().UnixMilli(),
			sqliteCleanupBatchSize,
		)
		cancel()
		if err != nil {
			database.logger.Error("clean expired SQLite cache "+kind, zap.Error(err))
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
			store.background.Wait()
			closeErr = errors.Join(store.readers.Close(), store.writer.Close())
		})
	})
	return closeErr
}
