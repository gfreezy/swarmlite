package caddystorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"
)

func TestModuleAcceptsSingleController(t *testing.T) {
	var module Module
	if err := json.Unmarshal([]byte(`{
		"controller":"http://10.0.0.2:8080"
	}`), &module); err != nil {
		t.Fatal(err)
	}
	if module.Controller != "http://10.0.0.2:8080" {
		t.Fatalf("unexpected controller %q", module.Controller)
	}
}

func TestModuleUsesManagedGatewayIdentityForLegacyConfig(t *testing.T) {
	t.Setenv(gatewayIDEnv, "gateway-a")
	configured, err := (Module{Root: t.TempDir()}).CertMagicStorage()
	if err != nil {
		t.Fatal(err)
	}
	actual, ok := configured.(*storage)
	if !ok {
		t.Fatalf("unexpected storage type %T", configured)
	}
	if actual.gatewayID != "gateway-a" {
		t.Fatalf("unexpected implicit Gateway identity %q", actual.gatewayID)
	}
}

func TestLocalStorageSurvivesCoordinatorFailure(t *testing.T) {
	storage := newStorage(
		t.TempDir(),
		"http://127.0.0.1:1",
		"test-token",
		"",
		20*time.Millisecond,
		2*time.Second,
		time.Minute,
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
			Key:              request.URL.Query().Get("key"),
			ValueBase64:      base64.StdEncoding.EncodeToString(value),
			ModifiedAtUnixMS: 100,
			Size:             int64(len(value)),
		})
	}))

	root := t.TempDir()
	storage := newStorage(
		root,
		server.URL,
		"test-token",
		"",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
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
		var payload lockAcquireRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.Name != "caddy/locks/issue_cert_example.com" {
			t.Fatalf("unexpected global lock name %q", payload.Name)
		}
		json.NewEncoder(response).Encode(lockAcquireResponse{Status: "busy"})
	}))
	defer server.Close()

	root := t.TempDir()
	storage := newStorage(
		root,
		server.URL,
		"test-token",
		"",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	locked, err := storage.TryLock(context.Background(), "issue_cert_example.com")
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("busy distributed lock unexpectedly fell back to a local lock")
	}
	local := &certmagic.FileStorage{Path: root}
	locked, err = local.TryLock(context.Background(), "issue_cert_example.com")
	if err != nil || !locked {
		t.Fatalf("local lock should remain free: locked=%v err=%v", locked, err)
	}
	if err := local.Unlock(context.Background(), "issue_cert_example.com"); err != nil {
		t.Fatal(err)
	}
}

func TestUnavailableCoordinatorFallsBackToLocalLock(t *testing.T) {
	root := t.TempDir()
	first := newStorage(
		root,
		"http://127.0.0.1:1",
		"test-token",
		"",
		20*time.Millisecond,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	second := newStorage(
		root,
		"http://127.0.0.1:1",
		"test-token",
		"",
		20*time.Millisecond,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
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

func TestCertificateLockSkipsGatewayThatDoesNotOwnHostname(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		json.NewEncoder(response).Encode(lockAcquireResponse{Status: "acquired"})
	}))
	defer server.Close()

	root := t.TempDir()
	storage := newStorage(
		root,
		server.URL,
		"0123456789abcdef",
		"gateway-a",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	storage.ownerProbe = func(context.Context, string) (string, error) {
		return "gateway-b", nil
	}

	locked, err := storage.TryLock(context.Background(), "issue_cert_duoshuo2.example")
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("non-owner Gateway unexpectedly acquired the certificate lock")
	}
	if requests.Load() != 0 {
		t.Fatalf("non-owner Gateway contacted the coordinator %d times", requests.Load())
	}
	if err := storage.Lock(context.Background(), "issue_cert_duoshuo2.example"); !errors.Is(err, errNotGatewayOwner) {
		t.Fatalf("expected owner mismatch, got %v", err)
	}
	local := &certmagic.FileStorage{Path: root}
	locked, err = local.TryLock(context.Background(), "issue_cert_duoshuo2.example")
	if err != nil || !locked {
		t.Fatalf("non-owner must not use the local fallback: locked=%v err=%v", locked, err)
	}
	if err := local.Unlock(context.Background(), "issue_cert_duoshuo2.example"); err != nil {
		t.Fatal(err)
	}
}

func TestConfirmedGatewayOwnerFallsBackWhenCoordinatorFails(t *testing.T) {
	storage := newStorage(
		t.TempDir(),
		"http://127.0.0.1:1",
		"0123456789abcdef",
		"gateway-a",
		20*time.Millisecond,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	storage.ownerProbe = func(context.Context, string) (string, error) {
		return "gateway-a", nil
	}

	locked, err := storage.TryLock(context.Background(), "issue_cert_duoshuo1.example")
	if err != nil || !locked {
		t.Fatalf("confirmed owner did not use local fallback: locked=%v err=%v", locked, err)
	}
	if err := storage.Unlock(context.Background(), "issue_cert_duoshuo1.example"); err != nil {
		t.Fatal(err)
	}
}

func TestCertificateOwnerProbeUsesRecentObservationOnFailure(t *testing.T) {
	storage := newStorage(
		t.TempDir(),
		"",
		"0123456789abcdef",
		"gateway-a",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	var calls atomic.Int32
	storage.ownerProbe = func(context.Context, string) (string, error) {
		if calls.Add(1) == 1 {
			return "gateway-a", nil
		}
		return "", errors.New("probe unavailable")
	}

	for attempt := 0; attempt < 2; attempt++ {
		eligible, err := storage.certificateLockEligible(
			context.Background(),
			"issue_cert_duoshuo1.example",
		)
		if err != nil || !eligible {
			t.Fatalf("attempt %d did not use the valid owner observation: eligible=%v err=%v", attempt+1, eligible, err)
		}
	}
}

func TestCertificateOwnerProbeFailsWithoutObservation(t *testing.T) {
	storage := newStorage(
		t.TempDir(),
		"",
		"0123456789abcdef",
		"gateway-a",
		time.Second,
		2*time.Second,
		time.Minute,
		30*time.Second,
	)
	storage.ownerProbe = func(context.Context, string) (string, error) {
		return "", errors.New("probe unavailable")
	}

	locked, err := storage.TryLock(context.Background(), "issue_cert_duoshuo1.example")
	if locked || err == nil {
		t.Fatalf("missing owner observation must prevent issuance: locked=%v err=%v", locked, err)
	}
}

func TestGatewayProbeHandlerReturnsAuthenticatedIdentity(t *testing.T) {
	token := []byte("0123456789abcdef")
	nonce, err := newProbeNonce()
	if err != nil {
		t.Fatal(err)
	}
	handler := GatewayProbeHandler{gatewayID: "gateway-a", token: token}
	request := httptest.NewRequest(
		http.MethodGet,
		gatewayProbePath+"?nonce="+nonce,
		nil,
	)
	request.Host = "Duoshuo1.Example:80"
	response := httptest.NewRecorder()
	if err := handler.ServeHTTP(response, request, nil); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", response.Code, response.Body.String())
	}
	var payload gatewayProbeResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.GatewayID != "gateway-a" || payload.Hostname != "duoshuo1.example" || payload.Nonce != nonce {
		t.Fatalf("unexpected probe response %#v", payload)
	}
	if expected := signGatewayProbe(token, payload); payload.Signature != expected {
		t.Fatalf("invalid probe signature %q", payload.Signature)
	}
}

func TestGatewayOwnerProbeVerifiesTheReachedGateway(t *testing.T) {
	token := "0123456789abcdef"
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Scheme != "http" || request.URL.Host != "duoshuo1.example" || request.URL.Path != gatewayProbePath {
			t.Fatalf("unexpected probe URL %s", request.URL)
		}
		payload := gatewayProbeResponse{
			GatewayID: "gateway-a",
			Hostname:  request.URL.Hostname(),
			Nonce:     request.URL.Query().Get("nonce"),
		}
		payload.Signature = signGatewayProbe([]byte(token), payload)
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(encoded))),
			Request:    request,
		}, nil
	})}
	probe := gatewayOwnerProbeWithClient(token, client)
	owner, err := probe(context.Background(), "Duoshuo1.Example.")
	if err != nil {
		t.Fatal(err)
	}
	if owner != "gateway-a" {
		t.Fatalf("unexpected Gateway owner %q", owner)
	}
}

func TestGatewayOwnerProbeRejectsForgedIdentity(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		payload := gatewayProbeResponse{
			GatewayID: "gateway-b",
			Hostname:  request.URL.Hostname(),
			Nonce:     request.URL.Query().Get("nonce"),
			Signature: "forged",
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(string(encoded))),
			Request:    request,
		}, nil
	})}
	probe := gatewayOwnerProbeWithClient("0123456789abcdef", client)
	if _, err := probe(context.Background(), "duoshuo2.example"); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("expected forged identity to be rejected, got %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
