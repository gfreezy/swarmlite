package caddystorage

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"
)

type storage struct {
	local       *certmagic.FileStorage
	coordinator *coordinator
	timeout     time.Duration
	lockLease   time.Duration
	ownerID     string
	gatewayID   string
	ownerProbe  gatewayOwnerProbe
	ownerTTL    time.Duration

	locksMu  sync.Mutex
	locks    map[string]*heldLock
	ownersMu sync.Mutex
	owners   map[string]ownerObservation
}

type ownerObservation struct {
	gatewayID string
	expiresAt time.Time
}

type heldLock struct {
	remote       bool
	fencingToken uint64
	stopRenewal  chan struct{}
	stopOnce     sync.Once
}

func newStorage(
	root string,
	controller string,
	token string,
	gatewayID string,
	timeout time.Duration,
	probeTimeout time.Duration,
	ownerTTL time.Duration,
	lockLease time.Duration,
) *storage {
	ownerID := randomID()
	return &storage{
		local:       &certmagic.FileStorage{Path: root},
		coordinator: newCoordinator(controller, token, timeout),
		timeout:     timeout,
		lockLease:   lockLease,
		ownerID:     ownerID,
		gatewayID:   strings.TrimSpace(gatewayID),
		ownerProbe:  newGatewayOwnerProbe(token, probeTimeout),
		ownerTTL:    ownerTTL,
		locks:       make(map[string]*heldLock),
		owners:      make(map[string]ownerObservation),
	}
}

func (s *storage) Store(ctx context.Context, key string, value []byte) error {
	if err := s.local.Store(ctx, key, value); err != nil {
		return err
	}
	request := putRequest{
		Key:         key,
		ValueBase64: base64.StdEncoding.EncodeToString(value),
	}
	remoteCtx, cancel := s.remoteContext()
	defer cancel()
	_ = s.coordinator.put(remoteCtx, request)
	return nil
}

func (s *storage) Load(ctx context.Context, key string) ([]byte, error) {
	value, err := s.local.Load(ctx, key)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return value, err
	}
	remoteCtx, cancel := s.remoteContextFrom(ctx)
	defer cancel()
	object, remoteErr := s.coordinator.object(remoteCtx, key)
	if remoteErr != nil {
		return nil, fs.ErrNotExist
	}
	value, err = base64.StdEncoding.DecodeString(object.ValueBase64)
	if err != nil {
		return nil, fmt.Errorf("decode Swarmlite cache value: %w", err)
	}
	if err := s.local.Store(ctx, key, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (s *storage) Delete(ctx context.Context, key string) error {
	if err := s.local.Delete(ctx, key); err != nil {
		return err
	}
	remoteCtx, cancel := s.remoteContext()
	defer cancel()
	_ = s.coordinator.delete(remoteCtx, deleteRequest{
		Key:       key,
		Recursive: true,
	})
	return nil
}

func (s *storage) Exists(ctx context.Context, key string) bool {
	if s.local.Exists(ctx, key) {
		return true
	}
	remoteCtx, cancel := s.remoteContextFrom(ctx)
	defer cancel()
	_, err := s.coordinator.stat(remoteCtx, key)
	return err == nil
}

func (s *storage) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	localKeys, localErr := s.local.List(ctx, path, recursive)
	remoteCtx, cancel := s.remoteContextFrom(ctx)
	defer cancel()
	remoteKeys, remoteErr := s.coordinator.list(remoteCtx, path, recursive)
	if localErr != nil && !errors.Is(localErr, fs.ErrNotExist) {
		return nil, localErr
	}
	if remoteErr != nil && localErr != nil {
		return nil, fs.ErrNotExist
	}
	keys := append(localKeys, remoteKeys...)
	sort.Strings(keys)
	keys = uniqueStrings(keys)
	return keys, nil
}

func (s *storage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	info, err := s.local.Stat(ctx, key)
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return info, err
	}
	remoteCtx, cancel := s.remoteContextFrom(ctx)
	defer cancel()
	remote, remoteErr := s.coordinator.stat(remoteCtx, key)
	if remoteErr != nil {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}
	return certmagic.KeyInfo{
		Key:        remote.Key,
		Modified:   time.UnixMilli(remote.ModifiedAtUnixMS),
		Size:       remote.Size,
		IsTerminal: remote.IsValue,
	}, nil
}

func (s *storage) Lock(ctx context.Context, name string) error {
	if err := s.ensureLockIsFree(name); err != nil {
		return err
	}
	eligible, err := s.certificateLockEligible(ctx, name)
	if err != nil {
		return err
	}
	if !eligible {
		return fmt.Errorf("%w for lock %q", errNotGatewayOwner, name)
	}
	for s.coordinator.configured() {
		remoteCtx, cancel := s.remoteContextFrom(ctx)
		response, err := s.coordinator.acquire(remoteCtx, s.acquireRequest(name))
		cancel()
		if err != nil {
			break
		}
		if response.Status == "acquired" && response.FencingToken != nil {
			s.rememberRemoteLock(name, *response.FencingToken)
			return nil
		}
		if response.Status != "busy" {
			break
		}
		wait := 500 * time.Millisecond
		if response.RetryAfterMillis != nil {
			wait = time.Duration(*response.RetryAfterMillis) * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		eligible, err = s.certificateLockEligible(ctx, name)
		if err != nil {
			return err
		}
		if !eligible {
			return fmt.Errorf("%w for lock %q", errNotGatewayOwner, name)
		}
	}
	if err := s.local.Lock(ctx, name); err != nil {
		return err
	}
	s.locksMu.Lock()
	s.locks[name] = &heldLock{}
	s.locksMu.Unlock()
	return nil
}

func (s *storage) TryLock(ctx context.Context, name string) (bool, error) {
	if err := s.ensureLockIsFree(name); err != nil {
		return false, err
	}
	eligible, err := s.certificateLockEligible(ctx, name)
	if err != nil || !eligible {
		return false, err
	}
	if s.coordinator.configured() {
		remoteCtx, cancel := s.remoteContextFrom(ctx)
		response, err := s.coordinator.acquire(remoteCtx, s.acquireRequest(name))
		cancel()
		if err == nil {
			switch response.Status {
			case "acquired":
				if response.FencingToken == nil {
					return false, errors.New("Swarmlite lock response omitted fencing_token")
				}
				s.rememberRemoteLock(name, *response.FencingToken)
				return true, nil
			case "busy":
				return false, nil
			}
		}
	}
	locked, err := s.local.TryLock(ctx, name)
	if locked && err == nil {
		s.locksMu.Lock()
		s.locks[name] = &heldLock{}
		s.locksMu.Unlock()
	}
	return locked, err
}

func (s *storage) Unlock(ctx context.Context, name string) error {
	s.locksMu.Lock()
	held, ok := s.locks[name]
	if ok {
		delete(s.locks, name)
	}
	s.locksMu.Unlock()
	if !ok {
		return fmt.Errorf("lock %q is not held", name)
	}
	if !held.remote {
		return s.local.Unlock(ctx, name)
	}
	held.stopOnce.Do(func() { close(held.stopRenewal) })
	remoteCtx, cancel := s.remoteContext()
	defer cancel()
	// Swarmlite is an optimization. A failed release expires by lease and must
	// never make Caddy's local storage report a failure.
	_ = s.coordinator.release(remoteCtx, lockMutationRequest{
		Name:         name,
		OwnerID:      s.ownerID,
		FencingToken: held.fencingToken,
	})
	return nil
}

func (s *storage) String() string {
	return "SwarmliteStorage:" + s.local.Path
}

func (s *storage) rememberRemoteLock(name string, token uint64) {
	held := &heldLock{
		remote:       true,
		fencingToken: token,
		stopRenewal:  make(chan struct{}),
	}
	s.locksMu.Lock()
	s.locks[name] = held
	s.locksMu.Unlock()
	go s.renewLock(name, held)
}

func (s *storage) renewLock(name string, held *heldLock) {
	interval := s.lockLease / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	leaseMillis := uint64(s.lockLease / time.Millisecond)
	for {
		select {
		case <-held.stopRenewal:
			return
		case <-ticker.C:
			eligible, err := s.certificateLockEligible(context.Background(), name)
			if err != nil || !eligible {
				return
			}
			ctx, cancel := s.remoteContext()
			_ = s.coordinator.renew(ctx, lockMutationRequest{
				Name:         name,
				OwnerID:      s.ownerID,
				FencingToken: held.fencingToken,
				LeaseMillis:  &leaseMillis,
			})
			cancel()
		}
	}
}

func (s *storage) certificateLockEligible(ctx context.Context, name string) (bool, error) {
	hostname, ok := strings.CutPrefix(name, "issue_cert_")
	if !ok || hostname == "" || strings.HasPrefix(hostname, "*.") || s.gatewayID == "" {
		return true, nil
	}
	owner, err := s.ownerProbe(ctx, hostname)
	if err == nil {
		s.ownersMu.Lock()
		s.owners[hostname] = ownerObservation{
			gatewayID: owner,
			expiresAt: time.Now().Add(s.ownerTTL),
		}
		s.ownersMu.Unlock()
		return owner == s.gatewayID, nil
	}

	now := time.Now()
	s.ownersMu.Lock()
	observation, cached := s.owners[hostname]
	if cached && !observation.expiresAt.After(now) {
		delete(s.owners, hostname)
		cached = false
	}
	s.ownersMu.Unlock()
	if cached {
		return observation.gatewayID == s.gatewayID, nil
	}
	return false, fmt.Errorf("determine Gateway owner for %q: %w", hostname, err)
}

func (s *storage) acquireRequest(name string) lockAcquireRequest {
	return lockAcquireRequest{
		Name:        name,
		OwnerID:     s.ownerID,
		LeaseMillis: uint64(s.lockLease / time.Millisecond),
	}
}

func (s *storage) ensureLockIsFree(name string) error {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if _, exists := s.locks[name]; exists {
		return fmt.Errorf("lock %q is already held by this storage instance", name)
	}
	return nil
}

func (s *storage) remoteContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
}

func (s *storage) remoteContextFrom(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, s.timeout)
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("caddy-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value[:])
}

func uniqueStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	output := values[:1]
	for _, value := range values[1:] {
		if value != output[len(output)-1] {
			output = append(output, value)
		}
	}
	return output
}

var (
	_ certmagic.Storage   = (*storage)(nil)
	_ certmagic.TryLocker = (*storage)(nil)
)
