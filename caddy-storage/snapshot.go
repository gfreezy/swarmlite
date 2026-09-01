package caddystorage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"sort"
	"strings"

	"github.com/caddyserver/caddy/v2"
)

const certificateManifestPrefix = "swarmlite/gateway-certificates"

type certificateSnapshotObject struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// certificateSnapshotManifest deliberately has no format version. It is an
// exact manifest for the Gateway's current CertMagic files, not a durable
// schema that Gateway versions need to migrate.
type certificateSnapshotManifest struct {
	GatewayID string                      `json:"gateway_id"`
	Objects   []certificateSnapshotObject `json:"objects"`
}

type certificateSnapshotResult struct {
	GatewayID string `json:"gateway_id"`
	Objects   int    `json:"objects"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
}

type storageAdmin struct{}

func (storageAdmin) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "admin.api.swarmlite_storage",
		New: func() caddy.Module { return new(storageAdmin) },
	}
}

func (storageAdmin) Routes() []caddy.AdminRoute {
	return []caddy.AdminRoute{
		{
			Pattern: "/swarmlite/storage/push",
			Handler: caddy.AdminHandlerFunc(handleCertificatePush),
		},
		{
			Pattern: "/swarmlite/storage/restore",
			Handler: caddy.AdminHandlerFunc(handleCertificateRestore),
		},
		{
			Pattern: "/swarmlite/storage/resume",
			Handler: caddy.AdminHandlerFunc(handleCertificateResume),
		},
	}
}

func handleCertificatePush(w http.ResponseWriter, r *http.Request) error {
	return handleCertificateSnapshot(w, r, (*storage).pushCertificateSnapshot)
}

func handleCertificateRestore(w http.ResponseWriter, r *http.Request) error {
	return handleCertificateSnapshot(w, r, (*storage).restoreCertificateSnapshot)
}

func handleCertificateResume(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: errors.New("method not allowed")}
	}
	configured := activeStorage.Load()
	if configured == nil {
		return caddy.APIError{HTTPStatus: http.StatusServiceUnavailable, Err: errors.New("Swarmlite storage is not configured")}
	}
	configured.quiesced.Store(false)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func handleCertificateSnapshot(
	w http.ResponseWriter,
	r *http.Request,
	action func(*storage, context.Context) (certificateSnapshotResult, error),
) error {
	if r.Method != http.MethodPost {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: errors.New("method not allowed")}
	}
	configured := activeStorage.Load()
	if configured == nil {
		return caddy.APIError{HTTPStatus: http.StatusServiceUnavailable, Err: errors.New("Swarmlite storage is not configured")}
	}
	result, err := action(configured, r.Context())
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errRemoteNotFound) {
			status = http.StatusNotFound
		}
		return caddy.APIError{HTTPStatus: status, Err: err}
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(result)
}

func (s *storage) pushCertificateSnapshot(ctx context.Context) (certificateSnapshotResult, error) {
	if !s.coordinator.configured() {
		return certificateSnapshotResult{}, errRemoteUnavailable
	}
	if strings.TrimSpace(s.gatewayID) == "" {
		return certificateSnapshotResult{}, errors.New("Swarmlite Gateway ID is empty")
	}

	s.dataMu.Lock()
	defer s.dataMu.Unlock()

	keys, err := s.local.List(ctx, "", true)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return certificateSnapshotResult{}, fmt.Errorf("list local certificate data: %w", err)
	}
	sort.Strings(keys)
	manifest := certificateSnapshotManifest{GatewayID: s.gatewayID}
	var totalBytes int64
	for _, key := range keys {
		if !certificateSnapshotKey(key) {
			continue
		}
		info, err := s.local.Stat(ctx, key)
		if err != nil {
			return certificateSnapshotResult{}, fmt.Errorf("stat local certificate object %q: %w", key, err)
		}
		if !info.IsTerminal {
			continue
		}
		value, err := s.local.Load(ctx, key)
		if err != nil {
			return certificateSnapshotResult{}, fmt.Errorf("load local certificate object %q: %w", key, err)
		}
		digest := sha256.Sum256(value)
		encoded := base64.StdEncoding.EncodeToString(value)
		remoteCtx, cancel := s.remoteContextFrom(ctx)
		err = s.coordinator.putRaw(remoteCtx, putRequest{
			Key:         namespacedKey(key),
			ValueBase64: encoded,
		})
		cancel()
		if err != nil {
			return certificateSnapshotResult{}, fmt.Errorf("push certificate object %q: %w", key, err)
		}
		remoteCtx, cancel = s.remoteContextFrom(ctx)
		remote, verifyErr := s.coordinator.objectRaw(remoteCtx, namespacedKey(key))
		cancel()
		if verifyErr != nil || remote.ValueBase64 != encoded {
			if verifyErr != nil {
				return certificateSnapshotResult{}, fmt.Errorf("verify certificate object %q: %w", key, verifyErr)
			}
			return certificateSnapshotResult{}, fmt.Errorf("verify certificate object %q: content mismatch", key)
		}
		manifest.Objects = append(manifest.Objects, certificateSnapshotObject{
			Key:    key,
			Size:   int64(len(value)),
			SHA256: hex.EncodeToString(digest[:]),
		})
		totalBytes += int64(len(value))
	}

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return certificateSnapshotResult{}, fmt.Errorf("encode certificate manifest: %w", err)
	}
	manifestValue := base64.StdEncoding.EncodeToString(manifestBytes)
	remoteCtx, cancel := s.remoteContextFrom(ctx)
	err = s.coordinator.putRaw(remoteCtx, putRequest{
		Key:         certificateManifestKey(s.gatewayID),
		ValueBase64: manifestValue,
	})
	cancel()
	if err != nil {
		return certificateSnapshotResult{}, fmt.Errorf("push certificate manifest: %w", err)
	}
	remoteCtx, cancel = s.remoteContextFrom(ctx)
	verifiedManifest, verifyErr := s.coordinator.objectRaw(remoteCtx, certificateManifestKey(s.gatewayID))
	cancel()
	if verifyErr != nil || verifiedManifest.ValueBase64 != manifestValue {
		if verifyErr != nil {
			return certificateSnapshotResult{}, fmt.Errorf("verify certificate manifest: %w", verifyErr)
		}
		return certificateSnapshotResult{}, errors.New("verify certificate manifest: content mismatch")
	}
	s.quiesced.Store(true)
	return certificateSnapshotSummary(manifest, totalBytes), nil
}

func (s *storage) restoreCertificateSnapshot(ctx context.Context) (certificateSnapshotResult, error) {
	if !s.coordinator.configured() {
		return certificateSnapshotResult{}, errRemoteUnavailable
	}
	if strings.TrimSpace(s.gatewayID) == "" {
		return certificateSnapshotResult{}, errors.New("Swarmlite Gateway ID is empty")
	}

	remoteCtx, cancel := s.remoteContextFrom(ctx)
	remoteManifest, err := s.coordinator.objectRaw(remoteCtx, certificateManifestKey(s.gatewayID))
	cancel()
	if err != nil {
		return certificateSnapshotResult{}, fmt.Errorf("load certificate manifest: %w", err)
	}
	manifestBytes, err := base64.StdEncoding.DecodeString(remoteManifest.ValueBase64)
	if err != nil {
		return certificateSnapshotResult{}, fmt.Errorf("decode certificate manifest value: %w", err)
	}
	var manifest certificateSnapshotManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return certificateSnapshotResult{}, fmt.Errorf("decode certificate manifest: %w", err)
	}
	if manifest.GatewayID != s.gatewayID {
		return certificateSnapshotResult{}, errors.New("certificate manifest belongs to a different Gateway")
	}

	s.dataMu.Lock()
	defer s.dataMu.Unlock()
	var totalBytes int64
	seen := make(map[string]struct{}, len(manifest.Objects))
	localKeys, err := s.local.List(ctx, "", true)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return certificateSnapshotResult{}, fmt.Errorf("list staged certificate data: %w", err)
	}
	for _, key := range localKeys {
		info, statErr := s.local.Stat(ctx, key)
		if statErr != nil {
			return certificateSnapshotResult{}, fmt.Errorf("stat staged certificate object %q: %w", key, statErr)
		}
		if info.IsTerminal && certificateSnapshotKey(key) {
			if deleteErr := s.local.Delete(ctx, key); deleteErr != nil {
				return certificateSnapshotResult{}, fmt.Errorf("clear staged certificate object %q: %w", key, deleteErr)
			}
		}
	}
	for _, object := range manifest.Objects {
		if !certificateSnapshotKey(object.Key) {
			return certificateSnapshotResult{}, fmt.Errorf("certificate manifest contains invalid key %q", object.Key)
		}
		if _, ok := seen[object.Key]; ok {
			return certificateSnapshotResult{}, fmt.Errorf("certificate manifest repeats key %q", object.Key)
		}
		seen[object.Key] = struct{}{}
		remoteCtx, cancel := s.remoteContextFrom(ctx)
		remote, loadErr := s.coordinator.objectRaw(remoteCtx, namespacedKey(object.Key))
		cancel()
		if loadErr != nil {
			return certificateSnapshotResult{}, fmt.Errorf("load certificate object %q: %w", object.Key, loadErr)
		}
		value, err := base64.StdEncoding.DecodeString(remote.ValueBase64)
		if err != nil {
			return certificateSnapshotResult{}, fmt.Errorf("decode certificate object %q: %w", object.Key, err)
		}
		digest := sha256.Sum256(value)
		if int64(len(value)) != object.Size || hex.EncodeToString(digest[:]) != object.SHA256 {
			return certificateSnapshotResult{}, fmt.Errorf("certificate object %q failed integrity verification", object.Key)
		}
		if err := s.local.Store(ctx, object.Key, value); err != nil {
			return certificateSnapshotResult{}, fmt.Errorf("restore certificate object %q: %w", object.Key, err)
		}
		totalBytes += int64(len(value))
	}
	return certificateSnapshotSummary(manifest, totalBytes), nil
}

func certificateManifestKey(gatewayID string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(gatewayID)))
	return certificateManifestPrefix + "/" + hex.EncodeToString(digest[:]) + "/manifest"
}

func certificateSnapshotKey(key string) bool {
	key = strings.TrimSpace(key)
	return key != "" &&
		key != "instance.uuid" &&
		key != "last_clean.json" &&
		key != "locks" &&
		!strings.HasPrefix(key, "locks/")
}

func certificateSnapshotSummary(manifest certificateSnapshotManifest, totalBytes int64) certificateSnapshotResult {
	digest := sha256.New()
	for _, object := range manifest.Objects {
		fmt.Fprintf(digest, "%s\n%d\n%s\n", object.Key, object.Size, object.SHA256)
	}
	return certificateSnapshotResult{
		GatewayID: manifest.GatewayID,
		Objects:   len(manifest.Objects),
		Bytes:     totalBytes,
		SHA256:    hex.EncodeToString(digest.Sum(nil)),
	}
}

var (
	_ caddy.Module      = (*storageAdmin)(nil)
	_ caddy.AdminRouter = (*storageAdmin)(nil)
)
