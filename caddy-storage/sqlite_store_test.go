package caddystorage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func openTestSQLiteStore(t *testing.T, path string) *sqliteResponseStore {
	t.Helper()
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:            path,
		cleanupInterval: time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testCacheEntry(request *http.Request, baseKey, body string, ttl time.Duration) *cacheEntry {
	now := time.Now()
	varyHeaders := []string{"Accept-Language"}
	return &cacheEntry{
		BaseKey:     testCacheKey(baseKey),
		VaryHeaders: varyHeaders,
		Status:      http.StatusOK,
		Header:      http.Header{"Content-Type": []string{"text/plain"}},
		Body:        []byte(body),
		StoredAt:    now,
		ExpiresAt:   now.Add(ttl),
	}
}

func testCacheKey(value string) cacheKey {
	sum := sha256.Sum256([]byte(value))
	var key cacheKey
	copy(key[:], sum[:])
	return key
}

func TestSQLiteResponseStorePersistsResponsesAndExpiresTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	request.Header.Set("Accept-Language", "en")

	store := openTestSQLiteStore(t, path)
	if err := store.Put(context.Background(), request, testCacheEntry(request, "persistent", "value", time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), request, testCacheEntry(request, "expiring", "short", 20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(context.Background(), testCacheKey("persistent"), request)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || string(entry.Body) != "value" {
		t.Fatalf("unexpected persistent entry %#v", entry)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entry, err = store.Get(context.Background(), testCacheKey("expiring"), request)
		if err != nil {
			t.Fatal(err)
		}
		if entry == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if entry != nil {
		t.Fatal("expired response is still visible")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestSQLiteStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	entry, err = reopened.Get(context.Background(), testCacheKey("persistent"), request)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || string(entry.Body) != "value" {
		t.Fatalf("response did not survive reopen: %#v", entry)
	}
}

func TestSQLiteResponseStoreSelectsVaryVariant(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })

	english := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	english.Header.Set("Accept-Language", "en")
	french := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	french.Header.Set("Accept-Language", "fr")
	if err := store.Put(context.Background(), english, testCacheEntry(english, "base", "hello", time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), french, testCacheEntry(french, "base", "bonjour", time.Minute)); err != nil {
		t.Fatal(err)
	}

	for request, expected := range map[*http.Request]string{english: "hello", french: "bonjour"} {
		entry, err := store.Get(context.Background(), testCacheKey("base"), request)
		if err != nil {
			t.Fatal(err)
		}
		if entry == nil || string(entry.Body) != expected {
			t.Fatalf("unexpected variant for %q: %#v", expected, entry)
		}
	}
}

func TestSQLiteResponseStoreAdvancesGenerationWhenVaryPolicyChanges(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })
	baseKey := testCacheKey("base")

	english := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	english.Header.Set("Accept-Language", "en")
	french := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	french.Header.Set("Accept-Language", "fr")
	if err := store.Put(context.Background(), english, testCacheEntry(english, "base", "hello", time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), french, testCacheEntry(french, "base", "bonjour", time.Minute)); err != nil {
		t.Fatal(err)
	}

	var generation int64
	if err := store.writer.QueryRow(
		"SELECT generation FROM cache_resources WHERE base_key = ?",
		baseKey[:],
	).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 1 {
		t.Fatalf("same Vary policy unexpectedly advanced generation to %d", generation)
	}

	compressed := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	compressed.Header.Set("Accept-Encoding", "gzip")
	compressedEntry := testCacheEntry(compressed, "base", "compressed", time.Minute)
	compressedEntry.VaryHeaders = []string{"Accept-Encoding"}
	if err := store.Put(context.Background(), compressed, compressedEntry); err != nil {
		t.Fatal(err)
	}

	var varyHeadersJSON string
	if err := store.writer.QueryRow(
		"SELECT generation, vary_headers FROM cache_resources WHERE base_key = ?",
		baseKey[:],
	).Scan(&generation, &varyHeadersJSON); err != nil {
		t.Fatal(err)
	}
	if generation != 2 {
		t.Fatalf("changed Vary policy did not advance generation: %d", generation)
	}
	if varyHeadersJSON != `["Accept-Encoding"]` {
		t.Fatalf("unexpected current Vary metadata %q", varyHeadersJSON)
	}

	entry, err := store.Get(context.Background(), baseKey, compressed)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || string(entry.Body) != "compressed" {
		t.Fatalf("unexpected current generation entry: %#v", entry)
	}
	entry, err = store.Get(context.Background(), baseKey, english)
	if err != nil {
		t.Fatal(err)
	}
	if entry != nil {
		t.Fatalf("former Vary generation remained reachable: %#v", entry)
	}

	var resources int
	var entries int
	if err := store.writer.QueryRow("SELECT COUNT(*) FROM cache_resources").Scan(&resources); err != nil {
		t.Fatal(err)
	}
	if err := store.writer.QueryRow("SELECT COUNT(*) FROM cache_entries").Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if resources != 1 || entries != 3 {
		t.Fatalf("unexpected normalized row counts: resources=%d entries=%d", resources, entries)
	}
}

func TestSQLiteResponseStorePersistsCompactBinaryKeys(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	request.Header.Set("Accept-Language", "en")
	entry := testCacheEntry(request, "base", "value", time.Minute)
	if err := store.Put(context.Background(), request, entry); err != nil {
		t.Fatal(err)
	}

	var entryType string
	var entryLength int
	if err := store.writer.QueryRow(`
		SELECT typeof(cache_key), length(cache_key)
		FROM cache_entries
	`).Scan(&entryType, &entryLength); err != nil {
		t.Fatal(err)
	}
	var baseType string
	var baseLength int
	if err := store.writer.QueryRow(`
		SELECT typeof(base_key), length(base_key)
		FROM cache_resources
	`).Scan(&baseType, &baseLength); err != nil {
		t.Fatal(err)
	}
	if entryType != "blob" || entryLength != cacheKeySize {
		t.Fatalf("unexpected stored cache key %s/%d", entryType, entryLength)
	}
	if baseType != "blob" || baseLength != cacheKeySize {
		t.Fatalf("unexpected stored base key %s/%d", baseType, baseLength)
	}
	if entry.Key == (cacheKey{}) {
		t.Fatal("store did not assign the final cache key")
	}
}

func TestSQLiteResponseStoreCleansExpiredEntriesAndResources(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)

	if err := store.Put(context.Background(), request, testCacheEntry(request, "expired", "old", -time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), request, testCacheEntry(request, "live", "new", time.Minute)); err != nil {
		t.Fatal(err)
	}
	store.deleteExpiredEntries()

	for table, expected := range map[string]int{
		"cache_entries":   1,
		"cache_resources": 1,
		"cache_access":    1,
	} {
		var rows int
		if err := store.writer.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != expected {
			t.Fatalf("unexpected %s rows after cleanup: %d", table, rows)
		}
	}
	var entryCount int
	if err := store.writer.QueryRow(
		"SELECT entry_count FROM cache_usage WHERE id = 1",
	).Scan(&entryCount); err != nil {
		t.Fatal(err)
	}
	if entryCount != 1 {
		t.Fatalf("unexpected cache usage entry count after cleanup: %d", entryCount)
	}
}

func TestSQLiteResponseStoreKeepsAccessMetadataSeparateFromResponseBodies(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	entry := testCacheEntry(request, "base", "value", time.Hour)
	if err := store.Put(context.Background(), request, entry); err != nil {
		t.Fatal(err)
	}

	var accessedAt int64
	var sizeBytes int64
	if err := store.writer.QueryRow(
		"SELECT accessed_at, size_bytes FROM cache_access WHERE cache_key = ?",
		entry.Key[:],
	).Scan(&accessedAt, &sizeBytes); err != nil {
		t.Fatal(err)
	}
	headerJSON := []byte(`{"Content-Type":["text/plain"]}`)
	expectedSize := cacheEntrySizeBytes(headerJSON, entry.Body)
	if sizeBytes != expectedSize {
		t.Fatalf("unexpected logical entry size %d, want %d", sizeBytes, expectedSize)
	}
	var totalBytes int64
	var entryCount int
	if err := store.writer.QueryRow(
		"SELECT total_bytes, entry_count FROM cache_usage WHERE id = 1",
	).Scan(&totalBytes, &entryCount); err != nil {
		t.Fatal(err)
	}
	if totalBytes != sizeBytes || entryCount != 1 {
		t.Fatalf("unexpected cache usage bytes=%d entries=%d", totalBytes, entryCount)
	}
	var responseAccessColumnCount int
	if err := store.writer.QueryRow(`
		SELECT COUNT(*) FROM pragma_table_info('cache_entries')
		WHERE name = 'accessed_at'
	`).Scan(&responseAccessColumnCount); err != nil {
		t.Fatal(err)
	}
	if responseAccessColumnCount != 0 {
		t.Fatal("accessed_at was stored on the response-body table")
	}

	oldAccess := time.Now().Add(-time.Hour).UnixMilli()
	if _, err := store.writer.Exec(
		"UPDATE cache_access SET accessed_at = ? WHERE cache_key = ?",
		oldAccess,
		entry.Key[:],
	); err != nil {
		t.Fatal(err)
	}
	store.enqueueCacheTouch(entry.Key)
	if !store.flushCacheTouches() {
		t.Fatal("cache touch flush failed")
	}
	if err := store.writer.QueryRow(
		"SELECT accessed_at FROM cache_access WHERE cache_key = ?",
		entry.Key[:],
	).Scan(&accessedAt); err != nil {
		t.Fatal(err)
	}
	if accessedAt <= oldAccess {
		t.Fatalf("access metadata was not refreshed: %d", accessedAt)
	}
}

func TestCacheTouchBloomDeduplicatesAndResets(t *testing.T) {
	var bloom cacheTouchBloom
	key := testCacheKey("touch")
	if !bloom.markIfNew(key) {
		t.Fatal("first Bloom insertion was treated as a duplicate")
	}
	if bloom.markIfNew(key) {
		t.Fatal("Bloom filter did not deduplicate the same cache key")
	}
	bloom.reset()
	if !bloom.markIfNew(key) {
		t.Fatal("Bloom reset did not make the cache key eligible again")
	}
}

func TestSQLiteResponseStoreEvictsApproximatelyLeastRecentlyUsed(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	entries := make([]*cacheEntry, 0, 3)
	for index, name := range []string{"oldest", "middle", "newest"} {
		entry := testCacheEntry(request, name, strings.Repeat(name, 8), time.Hour)
		if err := store.Put(context.Background(), request, entry); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
		if _, err := store.writer.Exec(
			"UPDATE cache_access SET accessed_at = ? WHERE cache_key = ?",
			time.Now().Add(time.Duration(index-3)*time.Hour).UnixMilli(),
			entry.Key[:],
		); err != nil {
			t.Fatal(err)
		}
	}
	totalBytes, err := store.cacheUsage()
	if err != nil {
		t.Fatal(err)
	}
	store.maxSizeBytes = totalBytes - 1
	store.lowWaterBytes = percentageBytes(store.maxSizeBytes, defaultSQLiteLowWaterPercent)
	store.enforceCapacity(true)

	for index, entry := range entries {
		var present int
		if err := store.writer.QueryRow(
			"SELECT EXISTS(SELECT 1 FROM cache_entries WHERE cache_key = ?)",
			entry.Key[:],
		).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if index == 0 && present != 0 {
			t.Fatal("oldest cache entry survived LRU eviction")
		}
		if index > 0 && present != 1 {
			t.Fatalf("newer cache entry %d was unexpectedly evicted", index)
		}
	}
}

func TestSQLiteResponseStoreSkipsEntryLargerThanCapacity(t *testing.T) {
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:            filepath.Join(t.TempDir(), "cache.db"),
		maxSizeBytes:    32,
		cleanupInterval: time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	if err := store.Put(
		context.Background(),
		request,
		testCacheEntry(request, "oversized", strings.Repeat("x", 64), time.Hour),
	); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"cache_entries", "cache_resources", "cache_access"} {
		var rows int
		if err := store.writer.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("oversized response created %d rows in %s", rows, table)
		}
	}
}

func TestSQLiteResponseStoreUsesBoundedWALConnectionPools(t *testing.T) {
	store := openTestSQLiteStore(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = store.Close() })

	var journalMode string
	if err := store.writer.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("unexpected journal mode %q", journalMode)
	}
	var mmapSize int64
	if err := store.writer.QueryRow("PRAGMA mmap_size").Scan(&mmapSize); err != nil {
		t.Fatal(err)
	}
	if mmapSize != 0 {
		t.Fatalf("unexpected mmap size %d", mmapSize)
	}
	if maximum := store.writer.Stats().MaxOpenConnections; maximum != 1 {
		t.Fatalf("unexpected writer connection limit %d", maximum)
	}
	if maximum := store.readers.Stats().MaxOpenConnections; maximum != defaultSQLiteReadConnections {
		t.Fatalf("unexpected reader connection limit %d", maximum)
	}

	connections := make([]*sql.Conn, 0, defaultSQLiteReadConnections)
	for range defaultSQLiteReadConnections {
		connection, err := store.readers.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for index, connection := range connections {
		var queryOnly int
		if err := connection.QueryRowContext(context.Background(), "PRAGMA query_only").Scan(&queryOnly); err != nil {
			t.Fatal(err)
		}
		if queryOnly != 1 {
			t.Fatalf("reader connection %d is not query-only: %d", index, queryOnly)
		}
	}
}

func TestSQLiteResponseStoreHonorsExplicitPageCacheSize(t *testing.T) {
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:            filepath.Join(t.TempDir(), "cache.db"),
		cacheSizeKiB:    1024,
		cleanupInterval: time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var cacheSize int64
	if err := store.writer.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatal(err)
	}
	if cacheSize != -1024 {
		t.Fatalf("unexpected explicit page cache size %d", cacheSize)
	}
}

func TestSQLiteResponseStoreHonorsCapacityAndAccessTuning(t *testing.T) {
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:            filepath.Join(t.TempDir(), "cache.db"),
		maxSizeBytes:    10_000,
		lowWaterPercent: 80,
		hitSampleRatio:  1,
		accessInterval:  2 * time.Minute,
		cleanupInterval: time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if store.maxSizeBytes != 10_000 || store.lowWaterBytes != 8_000 {
		t.Fatalf(
			"unexpected configured capacity max=%d low=%d",
			store.maxSizeBytes,
			store.lowWaterBytes,
		)
	}
	if store.hitSampleRatio != 1 || store.accessInterval != 2*time.Minute {
		t.Fatalf(
			"unexpected access tuning ratio=%d interval=%s",
			store.hitSampleRatio,
			store.accessInterval,
		)
	}
}

func TestSQLiteResponseStoreRejectsInvalidCapacityTuning(t *testing.T) {
	for name, options := range map[string]sqliteOptions{
		"low water zero after default is not invalid": {
			path: filepath.Join(t.TempDir(), "default.db"),
		},
		"low water one hundred": {
			path:            filepath.Join(t.TempDir(), "low.db"),
			lowWaterPercent: 100,
		},
		"negative access interval": {
			path:           filepath.Join(t.TempDir(), "interval.db"),
			accessInterval: -time.Second,
		},
	} {
		store, err := newSQLiteResponseStore(options, zap.NewNop())
		if name == "low water zero after default is not invalid" {
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			_ = store.Close()
			continue
		}
		if err == nil {
			_ = store.Close()
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestSQLiteResponseStoreKeepsSharedDatabaseOpenUntilLastRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	first := openTestSQLiteStore(t, path)
	second := openTestSQLiteStore(t, path)
	if first.sqliteDatabase != second.sqliteDatabase {
		t.Fatal("store instances did not reuse the same SQLite database")
	}
	request := httptest.NewRequest(http.MethodGet, "https://example.test/shared", nil)
	if err := first.Put(context.Background(), request, testCacheEntry(request, "shared", "value", time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	entry, err := second.Get(context.Background(), testCacheKey("shared"), request)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || string(entry.Body) != "value" {
		t.Fatalf("first release closed the shared database: %#v", entry)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteResponseStoreReplacesTheDisposableSouinSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE cache_entries (
			key TEXT PRIMARY KEY NOT NULL,
			value BLOB NOT NULL,
			expires_at INTEGER NOT NULL DEFAULT 0
		) WITHOUT ROWID;
		INSERT INTO cache_entries(key, value, expires_at) VALUES('souin-key', X'01', 0);
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store := openTestSQLiteStore(t, path)
	t.Cleanup(func() { _ = store.Close() })
	var version int
	if err := store.writer.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != sqliteCacheSchemaVersion {
		t.Fatalf("unexpected migrated schema version %d", version)
	}
	var rows int
	if err := store.writer.QueryRow("SELECT COUNT(*) FROM cache_entries").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("disposable Souin cache rows survived schema replacement: %d", rows)
	}
}

func TestSQLiteResponseStoreReplacesTheTextKeySchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		PRAGMA user_version = 2;
		CREATE TABLE cache_entries (
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
		INSERT INTO cache_entries(
			base_key, vary_key, vary_headers, status, headers, body, stored_at, expires_at
		) VALUES(
			'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
			X'5B5D', 200, X'7B7D', X'01', 1, 2
		);
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store := openTestSQLiteStore(t, path)
	t.Cleanup(func() { _ = store.Close() })
	var rows int
	if err := store.writer.QueryRow("SELECT COUNT(*) FROM cache_entries").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("text-key cache rows survived schema replacement: %d", rows)
	}
	for table, column := range map[string]string{
		"cache_entries":   "cache_key",
		"cache_resources": "base_key",
		"cache_access":    "cache_key",
	} {
		var kind string
		if err := store.writer.QueryRow(
			"SELECT type FROM pragma_table_info(?) WHERE name = ?",
			table,
			column,
		).Scan(&kind); err != nil {
			t.Fatal(err)
		}
		if kind != "BLOB" {
			t.Fatalf("unexpected %s.%s column type %q", table, column, kind)
		}
	}
}
