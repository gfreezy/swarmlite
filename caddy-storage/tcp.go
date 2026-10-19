package caddystorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

const (
	tcpHealthInterval  = 2 * time.Second
	tcpProbeTimeout    = time.Second
	tcpDialTimeout     = 3 * time.Second
	tcpConnectionLimit = 1024
)

// Bound probe concurrency across overlapping configs as well as routes.
var tcpProbes = make(chan struct{}, 32)
var tcpApps struct {
	sync.RWMutex
	active []*TCPApp
}

func init() {
	caddy.RegisterModule(TCPApp{})
	caddy.RegisterModule(tcpAdmin{})
}

// TCPApp treats listener/upstream failures as route status, never app startup
// errors: HTTP must remain usable when a TCP port is occupied or backends die.
type TCPApp struct {
	Routes []TCPRoute `json:"routes,omitempty"`
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
	routes []*tcpRouteRuntime
	stop   sync.Once
	wg     sync.WaitGroup
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	slots  chan struct{}
}

type TCPRoute struct {
	ID        string   `json:"id"`
	Listen    string   `json:"listen"`
	Upstreams []string `json:"upstreams"`
}

type TCPRouteStatus struct {
	ID        string             `json:"id"`
	Listen    string             `json:"listen"`
	Listening bool               `json:"listening"`
	Error     string             `json:"error,omitempty"`
	Backends  []TCPBackendStatus `json:"backends"`
}

type TCPBackendStatus struct {
	Address string `json:"address"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

type tcpRouteRuntime struct {
	app      *TCPApp
	config   TCPRoute
	address  caddy.NetworkAddress
	blocked  string
	mu       sync.RWMutex
	listener net.Listener
	err      string
	backends []*tcpUpstream
	next     atomic.Uint64
}

type tcpUpstream struct {
	mu       sync.RWMutex
	address  string
	healthy  bool
	failures int
	err      string
}

func (TCPApp) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "swarmlite_tcp", New: func() caddy.Module { return new(TCPApp) }}
}

func (a *TCPApp) Provision(ctx caddy.Context) error {
	a.ctx, a.cancel = context.WithCancel(ctx)
	a.logger = ctx.Logger()
	a.conns = make(map[net.Conn]struct{})
	a.slots = make(chan struct{}, tcpConnectionLimit)
	// Reusable Caddy sockets must not accidentally share HTTP/admin traffic.
	protected := map[uint]bool{80: true, 443: true, 2019: true, 2020: true, 2021: true}
	if app, err := ctx.AppIfConfigured("http"); err == nil {
		for _, server := range app.(*caddyhttp.App).Servers {
			for _, listen := range server.Listen {
				address, err := caddy.ParseNetworkAddress(listen)
				if err == nil {
					for port := address.StartPort; port <= address.EndPort; port++ {
						protected[port] = true
					}
				}
			}
		}
	}
	ids, ports := make(map[string]bool), make(map[uint]bool)
	for _, route := range a.Routes {
		address, err := caddy.ParseNetworkAddress(route.Listen)
		if err != nil || address.Network != "tcp" || address.StartPort == 0 || address.StartPort != address.EndPort {
			return fmt.Errorf("TCP route %q must listen on one numeric TCP port", route.ID)
		}
		if route.ID == "" || ids[route.ID] || ports[address.StartPort] {
			return fmt.Errorf("TCP route IDs and listen ports must be non-empty and unique")
		}
		ids[route.ID], ports[address.StartPort] = true, true
		runtime := &tcpRouteRuntime{app: a, config: route, address: address}
		if protected[address.StartPort] {
			runtime.blocked = "TCP listen port conflicts with an HTTP or reserved administration port"
		}
		seen := make(map[string]bool)
		for _, upstream := range route.Upstreams {
			host, port, err := net.SplitHostPort(upstream)
			number, parseErr := strconv.ParseUint(port, 10, 16)
			if err != nil || host == "" || parseErr != nil || number == 0 {
				return fmt.Errorf("TCP route %q has an invalid upstream %q", route.ID, upstream)
			}
			if !seen[upstream] {
				seen[upstream] = true
				runtime.backends = append(runtime.backends, &tcpUpstream{address: upstream})
			}
		}
		a.routes = append(a.routes, runtime)
	}
	return nil
}

func (a *TCPApp) Start() error {
	for _, route := range a.routes {
		route.bind()
		a.wg.Add(1)
		go func(r *tcpRouteRuntime) {
			defer a.wg.Done()
			r.maintain()
		}(route)
	}
	tcpApps.Lock()
	tcpApps.active = append(tcpApps.active, a)
	tcpApps.Unlock()
	return nil
}

func (a *TCPApp) Stop() error { return a.Cleanup() }

func (a *TCPApp) Cleanup() error {
	a.stop.Do(func() {
		tcpApps.Lock()
		for i, app := range tcpApps.active {
			if app == a {
				tcpApps.active = append(tcpApps.active[:i], tcpApps.active[i+1:]...)
				break
			}
		}
		tcpApps.Unlock()
		if a.cancel == nil {
			return
		}
		a.cancel()
		for _, route := range a.routes {
			route.mu.Lock()
			if route.listener != nil {
				_ = route.listener.Close()
				route.listener = nil
			}
			route.mu.Unlock()
		}
		a.connMu.Lock()
		for conn := range a.conns {
			_ = conn.Close()
		}
		a.connMu.Unlock()
		a.wg.Wait()
	})
	return nil
}

func (r *tcpRouteRuntime) bind() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listener != nil || r.app.ctx.Err() != nil {
		return
	}
	var ln net.Listener
	var err error
	if r.blocked != "" {
		err = errors.New(r.blocked)
	} else {
		var value any
		value, err = r.address.Listen(r.app.ctx, 0, net.ListenConfig{})
		if err == nil {
			ln = value.(net.Listener)
		}
	}
	if err != nil {
		if r.err != err.Error() {
			r.app.logger.Warn("TCP route cannot listen; HTTP is unaffected", zap.String("route", r.config.ID), zap.Error(err))
		}
		r.err = err.Error()
		return
	}
	if r.app.ctx.Err() != nil {
		_ = ln.Close()
		return
	}
	r.listener, r.err = ln, ""
	r.app.wg.Add(1)
	go func() { defer r.app.wg.Done(); r.accept(ln) }()
}

func (r *tcpRouteRuntime) maintain() {
	r.checkHealth()
	ticker := time.NewTicker(tcpHealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.app.ctx.Done():
			return
		case <-ticker.C:
			r.bind()
			r.checkHealth()
		}
	}
}

func (r *tcpRouteRuntime) checkHealth() {
	var wg sync.WaitGroup
	for _, backend := range r.backends {
		select {
		case tcpProbes <- struct{}{}:
		case <-r.app.ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(b *tcpUpstream) {
			defer wg.Done()
			defer func() { <-tcpProbes }()
			conn, err := (&net.Dialer{Timeout: tcpProbeTimeout}).DialContext(r.app.ctx, "tcp", b.address)
			if err == nil {
				_ = conn.Close()
			}
			b.result(err, false)
		}(backend)
	}
	wg.Wait()
}

func (b *tcpUpstream) result(err error, dialFailure bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.healthy, b.failures, b.err = true, 0, ""
	} else {
		b.failures++
		b.err = err.Error()
		if dialFailure || b.failures >= 2 {
			b.healthy = false
		}
	}
}

func (r *tcpRouteRuntime) accept(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			r.mu.Lock()
			if r.listener == ln {
				_ = ln.Close()
				r.listener = nil
				if r.app.ctx.Err() == nil {
					r.err = err.Error()
				}
			}
			r.mu.Unlock()
			return
		}
		select {
		case r.app.slots <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		if !r.app.track(conn) {
			<-r.app.slots
			continue
		}
		r.app.wg.Add(1)
		go func() {
			defer r.app.wg.Done()
			defer func() { <-r.app.slots }()
			defer r.app.untrack(conn)
			r.forward(conn)
		}()
	}
}

func (a *TCPApp) track(conn net.Conn) bool {
	a.connMu.Lock()
	defer a.connMu.Unlock()
	if a.ctx.Err() != nil {
		_ = conn.Close()
		return false
	}
	a.conns[conn] = struct{}{}
	return true
}

func (a *TCPApp) untrack(conn net.Conn) {
	_ = conn.Close()
	a.connMu.Lock()
	delete(a.conns, conn)
	a.connMu.Unlock()
}

func (r *tcpRouteRuntime) forward(down net.Conn) {
	if len(r.backends) == 0 {
		return
	}
	healthyBackends := make([]*tcpUpstream, 0, len(r.backends))
	for _, b := range r.backends {
		b.mu.RLock()
		healthy := b.healthy
		b.mu.RUnlock()
		if healthy {
			healthyBackends = append(healthyBackends, b)
		}
	}
	if len(healthyBackends) == 0 {
		return
	}
	selected := healthyBackends[(r.next.Add(1)-1)%uint64(len(healthyBackends))]
	up, err := (&net.Dialer{Timeout: tcpDialTimeout}).DialContext(r.app.ctx, "tcp", selected.address)
	if err != nil {
		selected.result(err, true)
		return
	}
	if !r.app.track(up) {
		return
	}
	defer r.app.untrack(up)
	results := make(chan error, 2)
	copyStream := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if closer, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		results <- err
	}
	go copyStream(up, down)
	go copyStream(down, up)
	if err := <-results; err != nil {
		_ = down.Close()
		_ = up.Close()
	}
	<-results
}

func (a *TCPApp) status() []TCPRouteStatus {
	routes := make([]TCPRouteStatus, 0, len(a.routes))
	for _, route := range a.routes {
		route.mu.RLock()
		status := TCPRouteStatus{ID: route.config.ID, Listen: route.config.Listen, Listening: route.listener != nil, Error: route.err, Backends: make([]TCPBackendStatus, 0, len(route.backends))}
		route.mu.RUnlock()
		for _, backend := range route.backends {
			backend.mu.RLock()
			status.Backends = append(status.Backends, TCPBackendStatus{Address: backend.address, Healthy: backend.healthy, Error: backend.err})
			backend.mu.RUnlock()
		}
		routes = append(routes, status)
	}
	return routes
}

type tcpAdmin struct{}

func (tcpAdmin) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "admin.api.swarmlite_tcp", New: func() caddy.Module { return new(tcpAdmin) }}
}

func (tcpAdmin) Routes() []caddy.AdminRoute {
	return []caddy.AdminRoute{{Pattern: "/swarmlite/tcp/status", Handler: caddy.AdminHandlerFunc(handleTCPStatus)}}
}

func handleTCPStatus(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: errors.New("method not allowed")}
	}
	tcpApps.RLock()
	routes := []TCPRouteStatus{}
	if len(tcpApps.active) > 0 {
		routes = tcpApps.active[len(tcpApps.active)-1].status()
	}
	tcpApps.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(struct {
		Routes []TCPRouteStatus `json:"routes"`
	}{routes})
}

var (
	_ caddy.App          = (*TCPApp)(nil)
	_ caddy.Provisioner  = (*TCPApp)(nil)
	_ caddy.CleanerUpper = (*TCPApp)(nil)
	_ caddy.AdminRouter  = (*tcpAdmin)(nil)
)
