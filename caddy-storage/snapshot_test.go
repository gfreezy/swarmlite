package caddystorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type memoryKV struct {
	mu      sync.Mutex
	objects map[string]string
}

func newMemoryKVServer(t *testing.T) (*memoryKV, *httptest.Server) {
	t.Helper()
	state := &memoryKV{objects: make(map[string]string)}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/kv" {
			http.NotFound(response, request)
			return
		}
		switch request.Method {
		case http.MethodPut:
			var payload putRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode PUT: %v", err)
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			state.mu.Lock()
			state.objects[payload.Key] = payload.ValueBase64
			state.mu.Unlock()
			response.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			key := request.URL.Query().Get("key")
			state.mu.Lock()
			value, ok := state.objects[key]
			state.mu.Unlock()
			if !ok {
				http.NotFound(response, request)
				return
			}
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				t.Errorf("invalid test value: %v", err)
			}
			json.NewEncoder(response).Encode(objectResponse{
				Key:         key,
				ValueBase64: value,
				Size:        int64(len(decoded)),
			})
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return state, server
}

func snapshotStorage(root, controller string) *storage {
	return newStorage(
		root,
		controller,
		"test-token",
		"gateway-a",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
}

func TestCertificateSnapshotPushesExactManifestAndRestoresBlankGateway(t *testing.T) {
	state, server := newMemoryKVServer(t)
	defer server.Close()
	ctx := context.Background()

	blue := snapshotStorage(t.TempDir(), server.URL)
	if err := blue.local.Store(ctx, "certificates/example.crt", []byte("certificate")); err != nil {
		t.Fatal(err)
	}
	if err := blue.local.Store(ctx, "certificates/example.key", []byte("private-key")); err != nil {
		t.Fatal(err)
	}
	if err := blue.local.Store(ctx, "instance.uuid", []byte("blue-instance")); err != nil {
		t.Fatal(err)
	}
	if err := blue.local.Store(ctx, "locks/issuance.lock", []byte("ephemeral")); err != nil {
		t.Fatal(err)
	}
	result, err := blue.pushCertificateSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Objects != 2 || result.Bytes != int64(len("certificate")+len("private-key")) {
		t.Fatalf("unexpected push result: %+v", result)
	}
	if err := blue.Store(ctx, "certificates/blocked.crt", []byte("blocked")); err == nil {
		t.Fatal("snapshot barrier did not quiesce certificate writes")
	}
	blue.quiesced.Store(false)
	if err := blue.Store(ctx, "certificates/after-resume.crt", []byte("resumed")); err != nil {
		t.Fatalf("certificate writes did not resume: %v", err)
	}

	state.mu.Lock()
	manifestValue := state.objects[certificateManifestKey("gateway-a")]
	state.objects["caddy/certificates/stale.crt"] = base64.StdEncoding.EncodeToString([]byte("stale"))
	state.mu.Unlock()
	manifestBytes, err := base64.StdEncoding.DecodeString(manifestValue)
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if _, exists := manifest["format_version"]; exists {
		t.Fatal("certificate manifest must not introduce a format version")
	}

	green := snapshotStorage(t.TempDir(), server.URL)
	if err := green.local.Store(ctx, "certificates/local-stale.crt", []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if err := green.local.Store(ctx, "instance.uuid", []byte("green-instance")); err != nil {
		t.Fatal(err)
	}
	restored, err := green.restoreCertificateSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restored != result {
		t.Fatalf("restore summary differs: push=%+v restore=%+v", result, restored)
	}
	for key, expected := range map[string]string{
		"certificates/example.crt": "certificate",
		"certificates/example.key": "private-key",
	} {
		value, err := green.local.Load(ctx, key)
		if err != nil || string(value) != expected {
			t.Fatalf("unexpected restored %s: %q, %v", key, value, err)
		}
	}
	if green.local.Exists(ctx, "certificates/local-stale.crt") {
		t.Fatal("restore retained a local object that was absent from the manifest")
	}
	if green.local.Exists(ctx, "certificates/stale.crt") {
		t.Fatal("restore imported a stale Controller object that was absent from the manifest")
	}
	instanceID, err := green.local.Load(ctx, "instance.uuid")
	if err != nil || string(instanceID) != "green-instance" {
		t.Fatalf("restore replaced the candidate's instance ID: %q, %v", instanceID, err)
	}
	if green.local.Exists(ctx, "locks/issuance.lock") {
		t.Fatal("restore imported an ephemeral lock file")
	}
}

func TestCertificateSnapshotRestoreRejectsCorruptControllerObject(t *testing.T) {
	state, server := newMemoryKVServer(t)
	defer server.Close()
	ctx := context.Background()
	blue := snapshotStorage(t.TempDir(), server.URL)
	if err := blue.local.Store(ctx, "certificates/example.crt", []byte("certificate")); err != nil {
		t.Fatal(err)
	}
	if _, err := blue.pushCertificateSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	state.objects["caddy/certificates/example.crt"] = base64.StdEncoding.EncodeToString([]byte("corrupt"))
	state.mu.Unlock()

	green := snapshotStorage(t.TempDir(), server.URL)
	if _, err := green.restoreCertificateSnapshot(ctx); err == nil {
		t.Fatal("corrupt certificate object unexpectedly passed integrity verification")
	}
}
