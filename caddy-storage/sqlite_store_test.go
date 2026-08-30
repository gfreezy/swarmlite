package caddystorage

import (
	"context"
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
		BaseKey:     baseKey,
		VaryKey:     requestVaryKey(request, varyHeaders),
		VaryHeaders: varyHeaders,
		Status:      http.StatusOK,
		Header:      http.Header{"Content-Type": []string{"text/plain"}},
		Body:        []byte(body),
		StoredAt:    now,
		ExpiresAt:   now.Add(ttl),
	}
}

func TestSQLiteResponseStorePersistsResponsesAndExpiresTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	request.Header.Set("Accept-Language", "en")

	store := openTestSQLiteStore(t, path)
	if err := store.Put(context.Background(), testCacheEntry(request, "persistent", "value", time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), testCacheEntry(request, "expiring", "short", 20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(context.Background(), "persistent", request)
	if err != nil {
		t.Fatal(err)
	}
	if entry == nil || string(entry.Body) != "value" {
		t.Fatalf("unexpected persistent entry %#v", entry)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		entry, err = store.Get(context.Background(), "expiring", request)
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
	entry, err = reopened.Get(context.Background(), "persistent", request)
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
	if err := store.Put(context.Background(), testCacheEntry(english, "base", "hello", time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), testCacheEntry(french, "base", "bonjour", time.Minute)); err != nil {
		t.Fatal(err)
	}

	for request, expected := range map[*http.Request]string{english: "hello", french: "bonjour"} {
		entry, err := store.Get(context.Background(), "base", request)
		if err != nil {
			t.Fatal(err)
		}
		if entry == nil || string(entry.Body) != expected {
			t.Fatalf("unexpected variant for %q: %#v", expected, entry)
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

func TestSQLiteResponseStoreKeepsSharedDatabaseOpenUntilLastRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	first := openTestSQLiteStore(t, path)
	second := openTestSQLiteStore(t, path)
	if first.sqliteDatabase != second.sqliteDatabase {
		t.Fatal("store instances did not reuse the same SQLite database")
	}
	request := httptest.NewRequest(http.MethodGet, "https://example.test/shared", nil)
	if err := first.Put(context.Background(), testCacheEntry(request, "shared", "value", time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	entry, err := second.Get(context.Background(), "shared", request)
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
