package caddystorage

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

const benchmarkCacheBodySize = 64 << 10

func openBenchmarkSQLiteStore(b *testing.B) *sqliteResponseStore {
	b.Helper()
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:            filepath.Join(b.TempDir(), "cache.db"),
		maxSizeBytes:    8 << 30,
		cleanupInterval: time.Hour,
	}, zap.NewNop())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	return store
}

func benchmarkCacheEntry(index uint64, body []byte) *cacheEntry {
	var key cacheKey
	binary.LittleEndian.PutUint64(key[:8], index)
	now := time.Now()
	return &cacheEntry{
		BaseKey:   key,
		Status:    http.StatusOK,
		Header:    http.Header{"Content-Type": []string{"application/json"}},
		Body:      body,
		StoredAt:  now,
		ExpiresAt: now.Add(time.Hour),
	}
}

func BenchmarkSQLiteResponseStorePutNew(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	body := make([]byte, benchmarkCacheBodySize)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := uint64(1); b.Loop(); index++ {
		if err := store.Put(context.Background(), request, benchmarkCacheEntry(index, body)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLiteResponseStorePutReplace(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	body := make([]byte, benchmarkCacheBodySize)
	entry := benchmarkCacheEntry(1, body)
	if err := store.Put(context.Background(), request, entry); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		now := time.Now()
		entry.StoredAt = now
		entry.ExpiresAt = now.Add(time.Hour)
		if err := store.Put(context.Background(), request, entry); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLiteResponseStorePutRejectedAtCapacity(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	body := make([]byte, benchmarkCacheBodySize)
	if err := store.Put(context.Background(), request, benchmarkCacheEntry(1, body)); err != nil {
		b.Fatal(err)
	}
	usage, err := store.cacheUsage()
	if err != nil {
		b.Fatal(err)
	}
	store.maxSizeBytes = usage
	store.lowWaterBytes = usage
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := uint64(2); b.Loop(); index++ {
		if err := store.Put(context.Background(), request, benchmarkCacheEntry(index, body)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLiteResponseStoreGetHit(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	entry := benchmarkCacheEntry(1, make([]byte, benchmarkCacheBodySize))
	if err := store.Put(context.Background(), request, entry); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(entry.Body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		loaded, err := store.Get(context.Background(), entry.BaseKey, request)
		if err != nil {
			b.Fatal(err)
		}
		if loaded == nil {
			b.Fatal("cache entry disappeared")
		}
	}
}

func BenchmarkSQLiteResponseStoreGetRandom(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	body := make([]byte, benchmarkCacheBodySize)
	const entries = 4096
	keys := make([]cacheKey, entries)
	for index := range entries {
		entry := benchmarkCacheEntry(uint64(index+1), body)
		if err := store.Put(context.Background(), request, entry); err != nil {
			b.Fatal(err)
		}
		keys[index] = entry.BaseKey
	}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	var sequence uint64
	for b.Loop() {
		// Walk the populated database in a deterministic pseudo-random order so
		// the benchmark exceeds SQLite's small per-connection page cache.
		sequence = sequence*6364136223846793005 + 1442695040888963407
		key := keys[(sequence>>32)%entries]
		loaded, err := store.Get(context.Background(), key, request)
		if err != nil {
			b.Fatal(err)
		}
		if loaded == nil {
			b.Fatal("cache entry disappeared")
		}
	}
}

func BenchmarkSQLiteResponseStoreTurnover(b *testing.B) {
	store := openBenchmarkSQLiteStore(b)
	request := httptest.NewRequest(http.MethodGet, "https://example.test/cached", nil)
	body := make([]byte, benchmarkCacheBodySize)
	entrySize := cacheEntrySizeBytes(
		[]byte(`{"Content-Type":["application/json"]}`),
		body,
	)
	store.maxSizeBytes = 32 << 20
	store.lowWaterBytes = 16 << 20
	var sequence uint64
	fill := func() {
		for store.usageBytes.Load()+entrySize <= store.maxSizeBytes {
			sequence++
			if err := store.Put(context.Background(), request, benchmarkCacheEntry(sequence, body)); err != nil {
				b.Fatal(err)
			}
		}
	}
	fill()
	b.ResetTimer()
	for b.Loop() {
		store.enforceCapacity(true)
		fill()
	}
	b.StopTimer()
	if _, err := store.writer.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}
	var pageCount int64
	var freePages int64
	var pageSize int64
	if err := store.writer.QueryRow("PRAGMA page_count").Scan(&pageCount); err != nil {
		b.Fatal(err)
	}
	if err := store.writer.QueryRow("PRAGMA freelist_count").Scan(&freePages); err != nil {
		b.Fatal(err)
	}
	if err := store.writer.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(pageCount*pageSize), "physical-B")
	b.ReportMetric(float64(freePages*pageSize), "free-B")
}
