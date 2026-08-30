package caddystorage

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
	"go.uber.org/zap"
)

func openTestSQLiteStorer(t *testing.T, path string) *sqliteStorer {
	t.Helper()
	storer, err := newSQLiteStorer(
		core.CacheProvider{
			Path: path,
			Configuration: map[string]interface{}{
				"cleanup_interval":      "1h",
				"mapping_scan_interval": "1m",
			},
		},
		zap.NewNop().Sugar(),
		30*time.Second,
		"SQLITE",
		func(options sqliteOptions) string { return options.path },
	)
	if err != nil {
		t.Fatal(err)
	}
	return storer
}

func TestSQLiteStorerPersistsValuesAndExpiresTTL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	storer := openTestSQLiteStorer(t, path)
	if err := storer.Set("persistent", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storer.Set("expiring", []byte("short"), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if actual := string(storer.Get("persistent")); actual != "value" {
		t.Fatalf("unexpected persistent value %q", actual)
	}

	deadline := time.Now().Add(time.Second)
	for len(storer.Get("expiring")) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if actual := storer.Get("expiring"); actual != nil {
		t.Fatalf("expired value is still present: %q", actual)
	}

	if err := storer.Reset(); err != nil {
		t.Fatal(err)
	}
	reopened := openTestSQLiteStorer(t, path)
	t.Cleanup(func() { _ = reopened.Reset() })
	if actual := string(reopened.Get("persistent")); actual != "value" {
		t.Fatalf("value did not survive reopen: %q", actual)
	}
}

func TestSQLiteStorerRoundTripsSouinMultiLevelResponse(t *testing.T) {
	storer := openTestSQLiteStorer(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = storer.Reset() })

	upstream := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		Body:       io.NopCloser(strings.NewReader("cached body")),
	}
	dumped, err := httputil.DumpResponse(upstream, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := storer.SetMultiLevel(
		"base-key",
		"varied-key",
		dumped,
		http.Header{},
		"",
		time.Minute,
		"https://example.test/cached",
	); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	fresh, stale := storer.GetMultiLevel("base-key", request, &core.Revalidator{})
	if stale != nil {
		t.Fatal("fresh response was unexpectedly returned as stale")
	}
	if fresh == nil {
		t.Fatal("fresh response was not returned")
	}
	body, err := io.ReadAll(fresh.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "cached body" {
		t.Fatalf("unexpected cached response body %q", body)
	}
	if keys := storer.ListKeys(); len(keys) != 1 || keys[0] != "https://example.test/cached" {
		t.Fatalf("unexpected cache key listing %#v", keys)
	}
}

func TestSQLiteStorerCoalescesDuplicateMappingScans(t *testing.T) {
	storer := openTestSQLiteStorer(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = storer.Reset() })
	if err := storer.Set(core.MappingKeyPrefix+"one", []byte("mapping"), 0); err != nil {
		t.Fatal(err)
	}

	first := storer.MapKeys(core.MappingKeyPrefix)
	if first["one"] != "mapping" {
		t.Fatalf("unexpected first mapping scan %#v", first)
	}
	if second := storer.MapKeys(core.MappingKeyPrefix); len(second) != 0 {
		t.Fatalf("duplicate mapping scan was not coalesced: %#v", second)
	}
}

func TestSQLiteStorerUsesBoundedWALConnectionPools(t *testing.T) {
	storer := openTestSQLiteStorer(t, filepath.Join(t.TempDir(), "cache.db"))
	t.Cleanup(func() { _ = storer.Reset() })

	var journalMode string
	if err := storer.writer.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("unexpected journal mode %q", journalMode)
	}
	var cacheSize int64
	if err := storer.writer.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatal(err)
	}
	if cacheSize != -defaultSQLiteCacheSizeKiB {
		t.Fatalf("unexpected page cache size %d", cacheSize)
	}
	var mmapSize int64
	if err := storer.writer.QueryRow("PRAGMA mmap_size").Scan(&mmapSize); err != nil {
		t.Fatal(err)
	}
	if mmapSize != 0 {
		t.Fatalf("unexpected mmap size %d", mmapSize)
	}

	if maximum := storer.writer.Stats().MaxOpenConnections; maximum != 1 {
		t.Fatalf("unexpected writer connection limit %d", maximum)
	}
	if maximum := storer.readers.Stats().MaxOpenConnections; maximum != defaultSQLiteReadConnections {
		t.Fatalf("unexpected reader connection limit %d", maximum)
	}

	connections := make([]*sql.Conn, 0, defaultSQLiteReadConnections)
	for range defaultSQLiteReadConnections {
		connection, err := storer.readers.Conn(context.Background())
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
		var readerCacheSize int64
		if err := connection.QueryRowContext(context.Background(), "PRAGMA cache_size").Scan(&readerCacheSize); err != nil {
			t.Fatal(err)
		}
		if readerCacheSize != -defaultSQLiteCacheSizeKiB {
			t.Fatalf("unexpected reader %d page cache size %d", index, readerCacheSize)
		}
	}
}

func TestSQLiteStorerKeepsSharedDatabaseOpenUntilLastRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	first := openTestSQLiteStorer(t, path)
	second := openTestSQLiteStorer(t, path)
	if first.sqliteDatabase != second.sqliteDatabase {
		t.Fatal("storer instances did not reuse the same SQLite database")
	}
	if err := first.Set("shared", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if err := first.Reset(); err != nil {
		t.Fatal(err)
	}
	if actual := string(second.Get("shared")); actual != "value" {
		t.Fatalf("first release closed the shared database: %q", actual)
	}
	if err := second.Reset(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteSimpleFSBridgeMatchesCacheHandlerUUID(t *testing.T) {
	provider := core.CacheProvider{
		Path: "/cache/sqlite/cache.db",
		Configuration: map[string]interface{}{
			"size": 12,
		},
	}
	if actual := configuredSQLitePath(provider); actual != "/cache/sqlite/cache.db" {
		t.Fatalf("unexpected configured path %q", actual)
	}
	if actual := configuredSimpleFSSize(provider); actual != 12 {
		t.Fatalf("unexpected compatibility size %d", actual)
	}
}
