package gatewayl4

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	caddycmd "github.com/caddyserver/caddy/v2/cmd"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	_ "github.com/mholt/caddy-l4/layer4"
	_ "github.com/mholt/caddy-l4/modules/l4close"
	_ "github.com/mholt/caddy-l4/modules/l4proxy"
	_ "github.com/swarmlite/swarmlite/caddy-storage"
)

// Each Caddy runs in a real child process with its own autosave directory.
// Tests exercise the same CLI, /load and --resume used by Gateway containers.
func TestCaddyChildProcess(t *testing.T) {
	if os.Getenv("SWARMLITE_L4_PROBE_CHILD") != "1" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("SWARMLITE_L4_PROBE_ARGS")), &args); err != nil {
		t.Fatal(err)
	}
	os.Args = append([]string{os.Args[0]}, args...)
	caddycmd.Main()
	os.Exit(0)
}

func TestSharedReloadPortConflictPreservesCurrentTraffic(t *testing.T) {
	f := newFixture(t)
	oldHTTP, newHTTP := httpBackend(t, "old"), httpBackend(t, "new")
	stableBackend := tcpBackend(t, "stable")
	stablePort, newPort := f.address(), f.address()
	blocker := listen(t, f.address())
	blockedPort := blocker.Addr().String()
	base := f.config(oldHTTP, map[string]tcpRoute{"stable": {[]string{stablePort}, stableBackend.address}})
	p := f.start(t.TempDir(), base, false)
	f.ready(p, "old")
	f.assertTCP(stablePort, "stable")
	beforeConfig := f.getConfig()
	beforeAutosave := readFile(t, p.autosave())

	candidate := f.config(newHTTP, map[string]tcpRoute{
		"stable": {[]string{stablePort}, stableBackend.address},
		// The first bind succeeds before the occupied second address fails.
		"new": {[]string{newPort, blockedPort}, stableBackend.address},
	})
	status, body := f.load(candidate)
	if status != http.StatusBadRequest || !strings.Contains(body, "address already in use") {
		t.Fatalf("expected occupied-port rejection, got %d: %s", status, body)
	}
	f.assertHTTP("old")
	f.assertTCP(stablePort, "stable")
	if !bytes.Equal(beforeConfig, f.getConfig()) || !bytes.Equal(beforeAutosave, readFile(t, p.autosave())) {
		t.Fatal("failed reload changed active or persisted configuration")
	}
	t.Logf("occupied-port /load: %d; old HTTP/TCP, active config and autosave preserved", status)

	// Record whether the failed third-party app leaves partially started
	// listeners behind; /config alone cannot detect this condition.
	probe, err := net.Listen("tcp4", newPort)
	if err != nil {
		t.Logf("LIMITATION: rejected layer4 startup left its first listener bound: %v", err)
		body, requestErr := tcpRequest(newPort)
		t.Logf("rejected listener response: payload=%q error=%v", body, requestErr)
	} else {
		_ = probe.Close()
		t.Log("partially started listener was released")
	}
	_ = blocker.Close()
	status, body = f.load(candidate)
	if status != http.StatusOK {
		t.Fatalf("retry after freeing port: %d %s", status, body)
	}
	f.ready(p, "new")
	f.assertTCP(stablePort, "stable")
	f.assertTCP(blockedPort, "stable")
	t.Log("freeing occupied port permits the same configuration to load")
	status, body = f.load(base)
	if status != http.StatusOK {
		t.Fatalf("removing candidate route: %d %s", status, body)
	}
	f.ready(p, "old")
	f.assertTCP(stablePort, "stable")
	probe, err = net.Listen("tcp4", newPort)
	if err != nil {
		t.Logf("LIMITATION: rejected listener remains bound even after later route removal: %v", err)
	} else {
		_ = probe.Close()
	}
}

func TestDeadBackendsZeroBackendsAndRecovery(t *testing.T) {
	f := newFixture(t)
	upstream := httpBackend(t, "http")
	stable, failing := tcpBackend(t, "stable"), tcpBackend(t, "failing")
	failingReplica := tcpBackend(t, "failing-replica")
	stablePort, failingPort := f.address(), f.address()
	routes := map[string]tcpRoute{
		"stable":  {[]string{stablePort}, stable.address},
		"failing": {[]string{failingPort}, failing.address},
	}
	config := f.config(upstream, routes)
	setProxyBackends(config, "failing", failing.address, failingReplica.address)
	p := f.start(t.TempDir(), config, false)
	f.ready(p, "http")
	for range 10 {
		body, err := tcpRequest(failingPort)
		if err != nil || (body != "failing" && body != "failing-replica") {
			t.Fatalf("multi-replica TCP forwarding: payload=%q error=%v", body, err)
		}
	}
	failing.close()
	failingReplica.close()
	for range 10 {
		f.assertTCPFailure(failingPort)
		f.assertHTTP("http")
		f.assertTCP(stablePort, "stable")
	}
	t.Log("all backends stopped: affected TCP connections fail; HTTP and another TCP route remain available")

	routes["failing"] = tcpRoute{[]string{failingPort}, ""}
	status, body := f.load(f.config(upstream, routes))
	if status != http.StatusOK {
		t.Fatalf("zero-backend close handler: %d %s", status, body)
	}
	f.ready(p, "http")
	f.assertTCPFailure(failingPort)
	f.assertHTTP("http")
	f.assertTCP(stablePort, "stable")
	t.Log("zero-backend close handler loads successfully and closes new connections")

	emptyProxy := f.config(upstream, routes)
	setProxyBackends(emptyProxy, "failing")
	status, body = f.load(emptyProxy)
	if status != http.StatusBadRequest || !strings.Contains(body, "no upstreams defined") {
		t.Fatalf("empty proxy must be rejected: %d %s", status, body)
	}
	f.assertHTTP("http")
	f.assertTCP(stablePort, "stable")
	t.Logf("empty proxy is invalid (%d); existing forwarding survives rejection", status)

	replacement := tcpBackend(t, "replacement")
	routes["failing"] = tcpRoute{[]string{failingPort}, replacement.address}
	status, body = f.load(f.config(upstream, routes))
	if status != http.StatusOK {
		t.Fatalf("restoring backend: %d %s", status, body)
	}
	f.ready(p, "http")
	f.assertTCP(failingPort, "replacement")
	f.assertHTTP("http")
	t.Log("restoring a backend resumes TCP forwarding without restarting Caddy")
}

func TestOccupiedPortOnColdStartFailsSharedProcess(t *testing.T) {
	f := newFixture(t)
	blocker := listen(t, f.address())
	backend := tcpBackend(t, "tcp")
	config := f.config(httpBackend(t, "http"), map[string]tcpRoute{
		"conflict": {[]string{blocker.Addr().String()}, backend.address},
	})
	p := f.start(t.TempDir(), config, false)
	f.expectStartupFailure(p)
	t.Log("LIMITATION: cold start with an occupied TCP listener exits the entire shared Caddy; HTTP is unavailable")
}

func TestOccupiedPortOnResumeFailsSharedProcessAndCanRecover(t *testing.T) {
	f := newFixture(t)
	upstream, backend := httpBackend(t, "http"), tcpBackend(t, "tcp")
	port, dir := f.address(), t.TempDir()
	config := f.config(upstream, map[string]tcpRoute{"tcp": {[]string{port}, backend.address}})
	p := f.start(dir, config, false)
	f.ready(p, "http")
	f.assertTCP(port, "tcp")
	saved := readFile(t, p.autosave())
	p.stop()
	blocker := listen(t, port)
	// Swarmlite also supplies an admin-only bootstrap, but --resume takes
	// precedence; the healthy bootstrap is not used if saved config fails.
	bootstrap := f.config("", nil)
	p = f.start(dir, bootstrap, true)
	f.expectStartupFailure(p)
	if !bytes.Equal(saved, readFile(t, p.autosave())) {
		t.Fatal("failed resume overwrote autosave")
	}
	t.Log("LIMITATION: --resume with a newly occupied TCP port exits the process; no bootstrap fallback and no HTTP")
	_ = blocker.Close()
	p = f.start(dir, bootstrap, true)
	f.ready(p, "http")
	f.assertTCP(port, "tcp")
	t.Log("after freeing the port, --resume restores the saved HTTP and TCP routes")
}

func TestResumeWithDeadAndZeroBackends(t *testing.T) {
	f := newFixture(t)
	upstream, backend := httpBackend(t, "http"), tcpBackend(t, "tcp")
	port, dir := f.address(), t.TempDir()
	routes := map[string]tcpRoute{"tcp": {[]string{port}, backend.address}}
	p := f.start(dir, f.config(upstream, routes), false)
	f.ready(p, "http")
	p.stop()
	backend.close()
	p = f.start(dir, f.config("", nil), true)
	f.ready(p, "http")
	f.assertTCPFailure(port)
	t.Log("--resume succeeds with all backend processes dead; HTTP still works")
	routes["tcp"] = tcpRoute{[]string{port}, ""}
	status, body := f.load(f.config(upstream, routes))
	if status != http.StatusOK {
		t.Fatalf("loading close handler: %d %s", status, body)
	}
	p.stop()
	p = f.start(dir, f.config("", nil), true)
	f.ready(p, "http")
	f.assertTCPFailure(port)
	t.Log("--resume also succeeds with zero-backend close handler")
}

func TestSeparateL4ProcessIsolatesHTTPReloadAndRestart(t *testing.T) {
	f := newFixture(t)
	oldHTTP, newHTTP := httpBackend(t, "old"), httpBackend(t, "new")
	backend := tcpBackend(t, "tcp")
	dir := t.TempDir()
	httpProcess := f.start(dir, f.config(oldHTTP, nil), false)
	f.ready(httpProcess, "old")
	blocker := listen(t, f.address())
	port := blocker.Addr().String()
	l4 := newFixture(t)
	l4Config := l4.config("", map[string]tcpRoute{"tcp": {[]string{port}, backend.address}})
	l4Process := l4.start(t.TempDir(), l4Config, false)
	l4.expectStartupFailure(l4Process)
	f.assertHTTP("old")
	status, body := f.load(f.config(newHTTP, nil))
	if status != http.StatusOK {
		t.Fatalf("unrelated HTTP config update: %d %s", status, body)
	}
	f.ready(httpProcess, "new")
	httpProcess.stop()
	httpProcess = f.start(dir, f.config("", nil), true)
	f.ready(httpProcess, "new")
	t.Log("separate L4 port conflict does not block HTTP traffic, configuration updates or --resume")
	_ = blocker.Close()
	l4Dir := t.TempDir()
	l4Process = l4.start(l4Dir, l4Config, false)
	l4.readyTCP(l4Process, port, "tcp")
	l4Process.stop()
	blocker = listen(t, port)
	l4Process = l4.start(l4Dir, l4.config("", nil), true)
	l4.expectStartupFailure(l4Process)
	f.assertHTTP("new")
	t.Log("separate L4 --resume failure also leaves HTTP running")
	_ = blocker.Close()
	l4Process = l4.start(l4Dir, l4.config("", nil), true)
	l4.readyTCP(l4Process, port, "tcp")
	backend.close()
	l4.assertTCPFailure(port)
	f.assertHTTP("new")
	t.Log("separate L4 process recovers after conflict; dead TCP backend does not affect HTTP")
}

func TestSeparateL4FailedLoadCanRestartLastGoodWithoutAffectingHTTP(t *testing.T) {
	httpGateway := newFixture(t)
	httpProcess := httpGateway.start(t.TempDir(), httpGateway.config(httpBackend(t, "http"), nil), false)
	httpGateway.ready(httpProcess, "http")
	l4 := newFixture(t)
	backend := tcpBackend(t, "tcp")
	stablePort, partialPort := l4.address(), l4.address()
	dir := t.TempDir()
	base := l4.config("", map[string]tcpRoute{"stable": {[]string{stablePort}, backend.address}})
	p := l4.start(dir, base, false)
	l4.readyTCP(p, stablePort, "tcp")
	saved := readFile(t, p.autosave())
	blocker := listen(t, l4.address())
	candidate := l4.config("", map[string]tcpRoute{
		"stable": {[]string{stablePort}, backend.address},
		"new":    {[]string{partialPort, blocker.Addr().String()}, backend.address},
	})
	status, body := l4.load(candidate)
	if status != http.StatusBadRequest || !strings.Contains(body, "address already in use") {
		t.Fatalf("occupied-port rejection: %d %s", status, body)
	}
	httpGateway.assertHTTP("http")
	if !bytes.Equal(saved, readFile(t, p.autosave())) {
		t.Fatal("failed L4 reload overwrote last-known-good autosave")
	}
	// Restart only the L4 process. The rejected candidate is not persisted;
	// --resume restores the previous L4 configuration and drops leaked sockets.
	p.stop()
	p = l4.start(dir, l4.config("", nil), true)
	l4.readyTCP(p, stablePort, "tcp")
	probe, err := net.Listen("tcp4", partialPort)
	if err != nil {
		t.Fatalf("L4 process restart failed to release rejected listener: %v", err)
	}
	_ = probe.Close()
	httpGateway.assertHTTP("http")
	t.Log("restarting only L4 restores last-known-good TCP config and removes rejected listeners; HTTP stays available")
}

type tcpRoute struct {
	listen  []string
	backend string
}

func setProxyBackends(config map[string]any, name string, backends ...string) {
	upstreams := make([]any, 0, len(backends))
	for _, backend := range backends {
		upstreams = append(upstreams, map[string]any{"dial": []string{backend}})
	}
	server := config["apps"].(map[string]any)["layer4"].(map[string]any)["servers"].(map[string]any)[name].(map[string]any)
	server["routes"] = []any{map[string]any{"handle": []any{map[string]any{"handler": "proxy", "upstreams": upstreams}}}}
}

type fixture struct {
	t         *testing.T
	admin     string
	http      string
	addresses map[string]bool
	client    *http.Client
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{t: t, addresses: make(map[string]bool), client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}}
	f.admin, f.http = f.address(), f.address()
	t.Cleanup(f.client.CloseIdleConnections)
	return f
}

func (f *fixture) address() string {
	f.t.Helper()
	for {
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			f.t.Fatal(err)
		}
		address := ln.Addr().String()
		_ = ln.Close()
		if !f.addresses[address] {
			f.addresses[address] = true
			return address
		}
	}
}

func (f *fixture) config(httpUpstream string, routes map[string]tcpRoute) map[string]any {
	apps := make(map[string]any)
	if httpUpstream != "" {
		proxy := func() map[string]any {
			return map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": httpUpstream}}}
		}
		apps["http"] = map[string]any{"servers": map[string]any{"swarmlite": map[string]any{
			"listen":          []string{f.http},
			"automatic_https": map[string]any{"disable": true},
			"routes": []any{
				map[string]any{"match": []any{map[string]any{"path": []string{"/proxy"}}}, "handle": []any{proxy()}},
				map[string]any{"match": []any{map[string]any{"path": []string{"/other"}}}, "handle": []any{proxy()}},
			},
		}}}
	}
	if len(routes) > 0 {
		servers := make(map[string]any)
		for name, route := range routes {
			handler := map[string]any{"handler": "close"}
			if route.backend != "" {
				handler = map[string]any{"handler": "proxy", "upstreams": []any{map[string]any{"dial": []string{route.backend}}}}
			}
			servers[name] = map[string]any{"listen": route.listen, "routes": []any{map[string]any{"handle": []any{handler}}}}
		}
		apps["layer4"] = map[string]any{"servers": servers}
	}
	return map[string]any{"admin": map[string]any{"listen": f.admin, "config": map[string]any{"persist": true}}, "apps": apps}
}

func (f *fixture) load(config map[string]any) (int, string) {
	f.t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		f.t.Fatal(err)
	}
	resp, err := f.client.Post("http://"+f.admin+"/load", "application/json", bytes.NewReader(encoded))
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func (f *fixture) getConfig() []byte {
	f.t.Helper()
	resp, err := f.client.Get("http://" + f.admin + "/config/")
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		f.t.Fatalf("reading config: %d %v", resp.StatusCode, err)
	}
	return body
}

func (f *fixture) assertHTTP(marker string) {
	f.t.Helper()
	for _, path := range []string{"/proxy", "/other"} {
		if err := f.checkHTTP(marker, path); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *fixture) checkHTTP(marker, path string) error {
	resp, err := f.client.Get("http://" + f.http + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || string(body) != marker+path {
		return fmt.Errorf("HTTP %s: %d %q, expected %q", path, resp.StatusCode, body, marker+path)
	}
	return nil
}

func (f *fixture) assertTCP(address, want string) {
	f.t.Helper()
	got, err := tcpRequest(address)
	if err != nil || got != want {
		f.t.Fatalf("TCP %s: %q %v, expected %q", address, got, err, want)
	}
}

func (f *fixture) assertTCPFailure(address string) {
	f.t.Helper()
	body, err := tcpRequest(address)
	if body != "" {
		f.t.Fatalf("TCP failure returned payload %q", body)
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		f.t.Fatalf("TCP connection hung instead of failing: %v", err)
	}
}

func tcpRequest(address string) (string, error) {
	conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte("ping")); err != nil {
		return "", err
	}
	body, err := io.ReadAll(conn)
	return string(body), err
}

type tcpServer struct {
	address  string
	listener net.Listener
	closed   sync.Once
}

func tcpBackend(t *testing.T, marker string) *tcpServer {
	t.Helper()
	ln := listen(t, "127.0.0.1:0")
	s := &tcpServer{address: ln.Addr().String(), listener: ln}
	t.Cleanup(s.close)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err := io.ReadFull(conn, make([]byte, 4)); err == nil {
					_, _ = io.WriteString(conn, marker)
				}
			}()
		}
	}()
	return s
}

func (s *tcpServer) close() { s.closed.Do(func() { _ = s.listener.Close() }) }

func httpBackend(t *testing.T, marker string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, marker+r.URL.Path) }))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func listen(t *testing.T, address string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

type process struct {
	t    *testing.T
	dir  string
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	logs lockedBuffer
}

type lockedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedBuffer) String() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

func (f *fixture) start(dir string, config map[string]any, resume bool) *process {
	f.t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(dir, "bootstrap.json")
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		f.t.Fatal(err)
	}
	args := []string{"run", "--config", path}
	if resume {
		args = append(args, "--resume")
	}
	argsJSON, _ := json.Marshal(args)
	executable, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	p := &process{t: f.t, dir: dir, done: make(chan struct{})}
	p.cmd = exec.Command(executable, "-test.run=^TestCaddyChildProcess$")
	p.cmd.Env = append(os.Environ(), "SWARMLITE_L4_PROBE_CHILD=1", "SWARMLITE_L4_PROBE_ARGS="+string(argsJSON), "XDG_CONFIG_HOME="+filepath.Join(dir, "config"), "XDG_DATA_HOME="+filepath.Join(dir, "data"))
	p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
	if err := p.cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	f.t.Cleanup(func() {
		p.stop()
		if f.t.Failed() {
			f.t.Logf("Caddy process %d logs:\n%s", p.cmd.Process.Pid, p.logs.String())
		}
	})
	return p
}

func (p *process) autosave() string { return filepath.Join(p.dir, "config", "caddy", "autosave.json") }

func (p *process) stop() {
	p.t.Helper()
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.t.Errorf("Caddy did not stop within 3 seconds: %s", p.logs.String())
	}
}

func (f *fixture) waitAdmin(p *process) {
	f.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			f.t.Fatalf("Caddy exited before readiness: %v\n%s", p.err, p.logs.String())
		default:
		}
		resp, err := f.client.Get("http://" + f.admin + "/config/")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("Caddy admin not ready: %s", p.logs.String())
}

func (f *fixture) ready(p *process, marker string) {
	f.t.Helper()
	f.waitAdmin(p)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		requestErr := f.checkHTTP(marker, "/proxy")
		if requestErr == nil {
			requestErr = f.checkHTTP(marker, "/other")
		}
		if _, err := os.Stat(p.autosave()); err == nil {
			if requestErr == nil {
				return
			}
			// Characterize recovery to a ready state, not zero-interruption
			// HTTP reloads. Keep transient errors visible in verbose output.
			f.t.Logf("readiness retry (not a zero-interruption assertion): %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("Caddy HTTP/autosave not ready: %s", p.logs.String())
}

func (f *fixture) readyTCP(p *process, address, marker string) {
	f.t.Helper()
	f.waitAdmin(p)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if body, err := tcpRequest(address); err == nil && body == marker {
			if _, err := os.Stat(p.autosave()); err == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("Caddy TCP/autosave not ready: %s", p.logs.String())
}

func (f *fixture) expectStartupFailure(p *process) {
	f.t.Helper()
	select {
	case <-p.done:
		if p.err == nil || !strings.Contains(p.logs.String(), "address already in use") {
			f.t.Fatalf("unexpected startup outcome: %v\n%s", p.err, p.logs.String())
		}
	case <-time.After(5 * time.Second):
		f.t.Fatalf("Caddy did not reject occupied port: %s", p.logs.String())
	}
	if resp, err := f.client.Get("http://" + f.http + "/proxy"); err == nil {
		_ = resp.Body.Close()
		f.t.Fatal("HTTP unexpectedly running after shared Caddy exited")
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return contents
}
