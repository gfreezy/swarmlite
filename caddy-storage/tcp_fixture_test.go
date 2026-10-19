package caddystorage

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

type tcpRoute struct {
	listen  []string
	backend string
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
		native := make([]any, 0, len(routes))
		for name, route := range routes {
			upstreams := []string{}
			if route.backend != "" {
				upstreams = append(upstreams, route.backend)
			}
			native = append(native, map[string]any{"id": name, "listen": route.listen[0], "upstreams": upstreams})
		}
		apps["swarmlite_tcp"] = map[string]any{"routes": native}
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

func tcpBackend(t *testing.T, marker string, address ...string) *tcpServer {
	t.Helper()
	endpoint := "127.0.0.1:0"
	if len(address) > 0 {
		endpoint = address[0]
	}
	ln := listen(t, endpoint)
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
