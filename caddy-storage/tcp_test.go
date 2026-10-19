package caddystorage

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTCPGatewayProcessReplacementDoesNotBlockHTTP(t *testing.T) {
	f := newFixture(t)
	backend, upstream := tcpBackend(t, "tcp"), httpBackend(t, "http")
	port := f.address()
	old := f.start(t.TempDir(), f.config(upstream, map[string]tcpRoute{"tcp": {[]string{port}, backend.address}}), false)
	f.ready(old, "http")
	f.readyTCP(old, port, "tcp")
	green := newFixture(t)
	green.http = f.http
	blocked := green.address()
	_ = listen(t, blocked)
	dir := t.TempDir()
	config := green.config(upstream, map[string]tcpRoute{
		"tcp":     {[]string{port}, backend.address},
		"blocked": {[]string{blocked}, backend.address},
	})
	candidate := green.start(dir, config, false)
	green.ready(candidate, "http")
	green.waitRoute("tcp", true, 1)
	green.waitRoute("blocked", false, 1)
	old.stop()
	green.assertHTTP("http")
	green.assertTCP(port, "tcp")
	config["admin"].(map[string]any)["listen"] = f.admin
	if status, body := green.load(config); status != http.StatusOK {
		t.Fatalf("admin promotion: %d %s", status, body)
	}
	green.admin = f.admin
	green.ready(candidate, "http")
	green.readyTCP(candidate, port, "tcp")
	candidate.stop()
	candidate = green.start(dir, green.config("", nil), true)
	green.ready(candidate, "http")
	green.readyTCP(candidate, port, "tcp")
	t.Log("overlapping Gateway processes can share HTTP/TCP listeners; an unrelated TCP conflict does not block admin promotion or --resume")
}

func TestTCPConflictPreservesHTTPSCertificateAndResume(t *testing.T) {
	f := newFixture(t)
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	defer certificateServer.Close()
	certificate := certificateServer.TLS.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	blocked := f.address()
	_ = listen(t, blocked)
	config := f.config(httpBackend(t, "https"), map[string]tcpRoute{"blocked": {[]string{blocked}, f.address()}})
	apps := config["apps"].(map[string]any)
	apps["tls"] = map[string]any{"certificates": map[string]any{"load_pem": []any{map[string]any{
		"certificate": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})),
		"key":         string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})),
	}}}}
	apps["http"].(map[string]any)["servers"].(map[string]any)["swarmlite"].(map[string]any)["tls_connection_policies"] = []any{map[string]any{}}
	dir := t.TempDir()
	p := f.start(dir, config, false)
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // test-only self-signed certificate
	defer client.CloseIdleConnections()
	check := func() {
		t.Helper()
		f.waitAdmin(p)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			response, err := client.Get("https://" + f.http + "/proxy")
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr == nil && string(body) == "https/proxy" {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("HTTPS unavailable:\n%s", p.logs.String())
	}
	check()
	f.waitRoute("blocked", false, 0)
	p.stop()
	p = f.start(dir, f.config("", nil), true)
	check()
	t.Log("HTTPS certificate loading and --resume succeed even with occupied TCP listener and dead backend")
}

func (f *fixture) tcpStatus() []TCPRouteStatus {
	f.t.Helper()
	response, err := f.client.Get("http://" + f.admin + "/swarmlite/tcp/status")
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	var status struct {
		Routes []TCPRouteStatus `json:"routes"`
	}
	if response.StatusCode != http.StatusOK {
		f.t.Fatalf("TCP status: %s", response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		f.t.Fatal(err)
	}
	return status.Routes
}

func (f *fixture) waitRoute(id string, listening bool, healthy int) TCPRouteStatus {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, route := range f.tcpStatus() {
			count := 0
			for _, backend := range route.Backends {
				if backend.Healthy {
					count++
				}
			}
			if route.ID == id && route.Listening == listening && count == healthy {
				return route
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.t.Fatalf("TCP route %s not ready: %+v", id, f.tcpStatus())
	return TCPRouteStatus{}
}

func setNativeBackends(config map[string]any, id string, addresses ...string) {
	routes := config["apps"].(map[string]any)["swarmlite_tcp"].(map[string]any)["routes"].([]any)
	for _, value := range routes {
		route := value.(map[string]any)
		if route["id"] == id {
			route["upstreams"] = addresses
		}
	}
}

func TestTCPPortConflictDoesNotBreakColdStartResumeOrOtherRoutes(t *testing.T) {
	f := newFixture(t)
	backend := tcpBackend(t, "tcp")
	stable, blocked := f.address(), f.address()
	blocker := listen(t, blocked)
	dir := t.TempDir()
	config := f.config(httpBackend(t, "http"), map[string]tcpRoute{
		"stable":  {[]string{stable}, backend.address},
		"blocked": {[]string{blocked}, backend.address},
	})
	p := f.start(dir, config, false)
	f.ready(p, "http")
	f.readyTCP(p, stable, "tcp")
	status := f.waitRoute("blocked", false, 1)
	if !strings.Contains(status.Error, "address already in use") {
		t.Fatalf("missing bind failure: %+v", status)
	}
	f.assertHTTP("http")
	p.stop()
	p = f.start(dir, f.config("", nil), true)
	f.ready(p, "http")
	f.readyTCP(p, stable, "tcp")
	f.waitRoute("blocked", false, 1)
	f.assertHTTP("http")
	_ = blocker.Close()
	f.waitRoute("blocked", true, 1)
	f.assertTCP(blocked, "tcp")
	f.assertHTTP("http")
	t.Log("occupied route is reported separately; cold start, --resume, HTTP and another TCP route work; freeing port recovers automatically")
}

func TestTCPReloadConflictIsPartialAndRemovedListenersAreReleased(t *testing.T) {
	f := newFixture(t)
	backend := tcpBackend(t, "tcp")
	stable, added, blocked := f.address(), f.address(), f.address()
	_ = listen(t, blocked)
	oldHTTP, newHTTP := httpBackend(t, "old"), httpBackend(t, "new")
	base := f.config(oldHTTP, map[string]tcpRoute{"stable": {[]string{stable}, backend.address}})
	p := f.start(t.TempDir(), base, false)
	f.ready(p, "old")
	f.readyTCP(p, stable, "tcp")
	candidate := f.config(newHTTP, map[string]tcpRoute{
		"stable":  {[]string{stable}, backend.address},
		"added":   {[]string{added}, backend.address},
		"blocked": {[]string{blocked}, backend.address},
	})
	if status, body := f.load(candidate); status != http.StatusOK {
		t.Fatalf("load: %d %s", status, body)
	}
	f.ready(p, "new")
	f.readyTCP(p, stable, "tcp")
	f.readyTCP(p, added, "tcp")
	f.waitRoute("blocked", false, 1)
	f.assertHTTP("new")
	if status, body := f.load(base); status != http.StatusOK {
		t.Fatalf("remove: %d %s", status, body)
	}
	f.ready(p, "old")
	f.readyTCP(p, stable, "tcp")
	probe, err := net.Listen("tcp4", added)
	if err != nil {
		t.Fatalf("removed TCP listener leaked: %v", err)
	}
	_ = probe.Close()
	if len(f.tcpStatus()) != 1 {
		t.Fatal("removed routes remain in TCP status")
	}
	t.Log("a TCP bind conflict does not reject HTTP changes or other TCP routes; removed listeners are released")
}

func TestTCPActiveHealthChecksRemoveAndRestoreBackends(t *testing.T) {
	f := newFixture(t)
	a, b, stableBackend := tcpBackend(t, "a"), tcpBackend(t, "b"), tcpBackend(t, "stable")
	port, stable := f.address(), f.address()
	config := f.config(httpBackend(t, "http"), map[string]tcpRoute{
		"pool":   {[]string{port}, a.address},
		"stable": {[]string{stable}, stableBackend.address},
	})
	setNativeBackends(config, "pool", a.address, b.address)
	p := f.start(t.TempDir(), config, false)
	f.ready(p, "http")
	f.waitRoute("pool", true, 2)
	seen := map[string]bool{}
	for range 6 {
		body, err := tcpRequest(port)
		if err != nil {
			t.Fatal(err)
		}
		seen[body] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("healthy replicas not both used: %v", seen)
	}
	a.close()
	// Do not send client traffic: removal must be caused by active probing.
	f.waitRoute("pool", true, 1)
	for range 5 {
		f.assertTCP(port, "b")
	}
	b.close()
	f.waitRoute("pool", true, 0)
	f.assertTCPFailure(port)
	f.assertHTTP("http")
	f.readyTCP(p, stable, "stable")
	_ = tcpBackend(t, "recovered", a.address)
	f.waitRoute("pool", true, 1)
	f.assertTCP(port, "recovered")
	f.assertHTTP("http")
	t.Log("active probes remove dead replicas without client traffic, isolate all-backend failure, and automatically restore recovered endpoints")
}

func TestTCPZeroBackendsResumeAndEndpointReplacement(t *testing.T) {
	f := newFixture(t)
	port, dir := f.address(), t.TempDir()
	upstream := httpBackend(t, "http")
	routes := map[string]tcpRoute{"empty": {[]string{port}, ""}}
	p := f.start(dir, f.config(upstream, routes), false)
	f.ready(p, "http")
	f.waitRoute("empty", true, 0)
	f.assertTCPFailure(port)
	p.stop()
	p = f.start(dir, f.config("", nil), true)
	f.ready(p, "http")
	f.waitRoute("empty", true, 0)
	f.assertTCPFailure(port)
	backend := tcpBackend(t, "new")
	routes["empty"] = tcpRoute{[]string{port}, backend.address}
	if status, body := f.load(f.config(upstream, routes)); status != http.StatusOK {
		t.Fatalf("load: %d %s", status, body)
	}
	f.readyTCP(p, port, "new")
	f.assertHTTP("http")
}

func TestTCPDoesNotShareHTTPListener(t *testing.T) {
	f := newFixture(t)
	backend := tcpBackend(t, "tcp")
	p := f.start(t.TempDir(), f.config(httpBackend(t, "http"), map[string]tcpRoute{
		"conflict": {[]string{f.http}, backend.address},
	}), false)
	f.ready(p, "http")
	status := f.waitRoute("conflict", false, 1)
	if !strings.Contains(status.Error, "HTTP") {
		t.Fatalf("HTTP port not protected: %+v", status)
	}
	for range 20 {
		f.assertHTTP("http")
	}
}

func TestTCPFailedConfigRollsBackAndCleansUp(t *testing.T) {
	f := newFixture(t)
	backend := tcpBackend(t, "tcp")
	stable, added, blocked := f.address(), f.address(), f.address()
	_ = listen(t, blocked)
	upstream := httpBackend(t, "http")
	p := f.start(t.TempDir(), f.config(upstream, map[string]tcpRoute{"stable": {[]string{stable}, backend.address}}), false)
	f.ready(p, "http")
	f.readyTCP(p, stable, "tcp")
	for range 10 {
		candidate := f.config(upstream, map[string]tcpRoute{"added": {[]string{added}, backend.address}})
		candidate["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["swarmlite"].(map[string]any)["listen"] = []string{blocked}
		if status, _ := f.load(candidate); status != http.StatusBadRequest {
			t.Fatalf("expected failed HTTP startup: %d", status)
		}
		f.assertHTTP("http")
		f.assertTCP(stable, "tcp")
		probe, err := net.Listen("tcp4", added)
		if err != nil {
			t.Fatalf("failed config leaked TCP listener: %v", err)
		}
		_ = probe.Close()
		statuses := f.tcpStatus()
		if len(statuses) != 1 || statuses[0].ID != "stable" {
			t.Fatalf("status did not roll back: %+v", statuses)
		}
	}
}

func TestTCPHalfCloseAndShutdownWithLiveConnection(t *testing.T) {
	f := newFixture(t)
	ln := listen(t, "127.0.0.1:0")
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				body, _ := io.ReadAll(conn)
				_, _ = conn.Write(append([]byte("reply:"), body...))
			}()
		}
	}()
	port := f.address()
	p := f.start(t.TempDir(), f.config(httpBackend(t, "http"), map[string]tcpRoute{"half": {[]string{port}, ln.Addr().String()}}), false)
	f.ready(p, "http")
	f.waitRoute("half", true, 1)
	conn, err := net.DialTimeout("tcp", port, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Write([]byte("request"))
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(conn)
	if err != nil || string(body) != "reply:request" {
		t.Fatalf("half-close: %q %v", body, err)
	}
	live, err := net.DialTimeout("tcp", port, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	p.stop()
	_ = live.SetReadDeadline(time.Now().Add(time.Second))
	_, err = live.Read(make([]byte, 1))
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("shutdown left connection hanging")
	}
}
