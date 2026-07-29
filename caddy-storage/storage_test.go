package caddystorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
)

func TestModuleAcceptsControllerSetGeneration(t *testing.T) {
	var module Module
	if err := json.Unmarshal([]byte(`{
		"controllers":["http://10.0.0.2:8080"],
		"controller_set_generation":42
	}`), &module); err != nil {
		t.Fatal(err)
	}
	if module.ControllerSetGeneration != 42 {
		t.Fatalf("unexpected controller set generation %d", module.ControllerSetGeneration)
	}
}

func TestLocalStorageSurvivesCoordinatorFailure(t *testing.T) {
	storage := newStorage(
		t.TempDir(),
		[]string{"http://127.0.0.1:1"},
		"test-token",
		20*time.Millisecond,
		30*time.Second,
	)
	ctx := context.Background()
	if err := storage.Store(ctx, "certificates/example.crt", []byte("certificate")); err != nil {
		t.Fatal(err)
	}
	value, err := storage.Load(ctx, "certificates/example.crt")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "certificate" {
		t.Fatalf("unexpected local value %q", value)
	}
	if err := storage.Delete(ctx, "certificates"); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Load(ctx, "certificates/example.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected local deletion, got %v", err)
	}
}

func TestRemoteLoadPopulatesAuthoritativeLocalStorage(t *testing.T) {
	value := []byte("shared certificate")
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/kv" {
			http.NotFound(response, request)
			return
		}
		if key := request.URL.Query().Get("key"); key != "caddy/certificates/example.crt" {
			t.Fatalf("unexpected namespaced key %q", key)
		}
		json.NewEncoder(response).Encode(objectResponse{
			Key:         request.URL.Query().Get("key"),
			ValueBase64: base64.StdEncoding.EncodeToString(value),
			Version: cacheVersion{
				PhysicalUnixMS: 100,
				ReplicaID:      "remote-caddy",
			},
			ModifiedAtUnixMS: 100,
			Size:             int64(len(value)),
		})
	}))

	root := t.TempDir()
	storage := newStorage(root, []string{server.URL}, "test-token", time.Second, 30*time.Second)
	loaded, err := storage.Load(context.Background(), "certificates/example.crt")
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded) != string(value) {
		t.Fatalf("unexpected remote value %q", loaded)
	}
	server.Close()
	loaded, err = storage.Load(context.Background(), "certificates/example.crt")
	if err != nil || string(loaded) != string(value) {
		t.Fatalf("remote value was not persisted locally: %q, %v", loaded, err)
	}
}

func TestBusyDistributedLockDoesNotFallBackToLocal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/kv/locks/acquire" {
			http.NotFound(response, request)
			return
		}
		json.NewEncoder(response).Encode(lockAcquireResponse{Status: "busy"})
	}))
	defer server.Close()

	root := t.TempDir()
	storage := newStorage(root, []string{server.URL}, "test-token", time.Second, 30*time.Second)
	locked, err := storage.TryLock(context.Background(), "issue-example")
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("busy distributed lock unexpectedly fell back to a local lock")
	}
	local := &certmagic.FileStorage{Path: root}
	locked, err = local.TryLock(context.Background(), "issue-example")
	if err != nil || !locked {
		t.Fatalf("local lock should remain free: locked=%v err=%v", locked, err)
	}
	if err := local.Unlock(context.Background(), "issue-example"); err != nil {
		t.Fatal(err)
	}
}

func TestUnavailableCoordinatorFallsBackToLocalLock(t *testing.T) {
	root := t.TempDir()
	first := newStorage(root, []string{"http://127.0.0.1:1"}, "test-token", 20*time.Millisecond, 30*time.Second)
	second := newStorage(root, []string{"http://127.0.0.1:1"}, "test-token", 20*time.Millisecond, 30*time.Second)
	ctx := context.Background()
	locked, err := first.TryLock(ctx, "issue-example")
	if err != nil || !locked {
		t.Fatalf("first local fallback failed: locked=%v err=%v", locked, err)
	}
	locked, err = second.TryLock(ctx, "issue-example")
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("second local fallback acquired an already-held lock")
	}
	if err := first.Unlock(ctx, "issue-example"); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderRedirectKeepsOriginalQuery(t *testing.T) {
	redirected, err := redirectedURL(
		"http://10.0.0.22:8080/v1/kv",
		"/v1/kv",
		url.Values{"key": []string{"caddy/certificates/example.crt"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(redirected)
	if err != nil {
		t.Fatal(err)
	}
	if key := parsed.Query().Get("key"); key != "caddy/certificates/example.crt" {
		t.Fatalf("redirect lost key query: %q", key)
	}
}
