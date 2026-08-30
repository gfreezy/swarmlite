package caddystorage

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/caddyserver/caddy/v2/caddytest"
	"github.com/klauspost/compress/zstd"
)

func TestResponseCompressionNegotiationAndUpstreamPassthrough(t *testing.T) {
	largeBody := strings.Repeat("swarmlite response compression ", 64)
	shortBody := "short response"
	precompressedBody := gzipBytes(t, []byte(largeBody))

	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/plain")
		switch request.URL.Path {
		case "/short":
			_, _ = io.WriteString(response, shortBody)
		case "/precompressed":
			response.Header().Set("Content-Encoding", "gzip")
			_, _ = response.Write(precompressedBody)
		default:
			_, _ = io.WriteString(response, largeBody)
		}
	}))
	defer upstream.Close()

	adminPort := unusedTCPPort(t)
	httpPort := unusedTCPPort(t)
	for httpPort == adminPort {
		httpPort = unusedTCPPort(t)
	}
	upstreamAddress := strings.TrimPrefix(upstream.URL, "http://")
	config, err := json.Marshal(map[string]any{
		"admin": map[string]any{"listen": fmt.Sprintf("127.0.0.1:%d", adminPort)},
		"apps": map[string]any{
			"http": map[string]any{
				"servers": map[string]any{
					"compression": map[string]any{
						"listen": []string{fmt.Sprintf("127.0.0.1:%d", httpPort)},
						"routes": []any{map[string]any{
							"handle": []any{
								map[string]any{
									"handler":        "encode",
									"encodings":      map[string]any{"zstd": map[string]any{}, "gzip": map[string]any{}},
									"prefer":         []string{"zstd", "gzip"},
									"minimum_length": 512,
								},
								map[string]any{
									"handler":   "reverse_proxy",
									"upstreams": []any{map[string]any{"dial": upstreamAddress}},
								},
							},
						}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: adminPort})
	tester.InitServer(string(config), "json")
	baseURL := "http://127.0.0.1:" + strconv.Itoa(httpPort)

	zstdResponse, zstdEncoded := requestEncoded(t, tester.Client, baseURL+"/large", "gzip, zstd")
	if got := zstdResponse.Header.Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("expected zstd response, got %q", got)
	}
	assertVaryAcceptEncoding(t, zstdResponse.Header)
	zstdDecoder, err := zstd.NewReader(bytes.NewReader(zstdEncoded))
	if err != nil {
		t.Fatal(err)
	}
	zstdDecoded, err := io.ReadAll(zstdDecoder)
	zstdDecoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(zstdDecoded) != largeBody {
		t.Fatal("zstd response did not decode to the upstream body")
	}

	gzipResponse, gzipEncoded := requestEncoded(t, tester.Client, baseURL+"/large", "gzip")
	if got := gzipResponse.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected gzip response, got %q", got)
	}
	assertVaryAcceptEncoding(t, gzipResponse.Header)
	gzipReader, err := gzip.NewReader(bytes.NewReader(gzipEncoded))
	if err != nil {
		t.Fatal(err)
	}
	gzipDecoded, err := io.ReadAll(gzipReader)
	if closeErr := gzipReader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(gzipDecoded) != largeBody {
		t.Fatal("gzip response did not decode to the upstream body")
	}

	shortResponse, shortReceived := requestEncoded(t, tester.Client, baseURL+"/short", "gzip")
	if got := shortResponse.Header.Get("Content-Encoding"); got != "" {
		t.Fatalf("short response was unexpectedly encoded with %q", got)
	}
	if string(shortReceived) != shortBody {
		t.Fatalf("unexpected short response %q", shortReceived)
	}

	precompressedResponse, precompressedReceived := requestEncoded(
		t,
		tester.Client,
		baseURL+"/precompressed",
		"zstd, gzip",
	)
	if got := precompressedResponse.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("upstream Content-Encoding changed to %q", got)
	}
	if !bytes.Equal(precompressedReceived, precompressedBody) {
		t.Fatal("upstream precompressed bytes were encoded again or otherwise changed")
	}
}

func TestCachedResponseCompressionRemainsClientSpecific(t *testing.T) {
	largeBody := strings.Repeat("cache uncompressed upstream representation ", 64)
	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		upstreamRequests.Add(1)
		response.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(response, largeBody)
	}))
	defer upstream.Close()

	adminPort := unusedTCPPort(t)
	httpPort := unusedTCPPort(t)
	for httpPort == adminPort {
		httpPort = unusedTCPPort(t)
	}
	config, err := json.Marshal(map[string]any{
		"admin": map[string]any{"listen": fmt.Sprintf("127.0.0.1:%d", adminPort)},
		"apps": map[string]any{
			"http": map[string]any{
				"servers": map[string]any{
					"cached-compression": map[string]any{
						"listen": []string{fmt.Sprintf("127.0.0.1:%d", httpPort)},
						"routes": []any{map[string]any{
							"handle": []any{
								map[string]any{
									"handler":        "encode",
									"encodings":      map[string]any{"zstd": map[string]any{}, "gzip": map[string]any{}},
									"prefer":         []string{"zstd", "gzip"},
									"minimum_length": 512,
								},
								map[string]any{
									"handler": "cache",
									"path":    filepath.Join(t.TempDir(), "cache.db"),
									"ttl":     "5m",
								},
								map[string]any{
									"handler":   "reverse_proxy",
									"upstreams": []any{map[string]any{"dial": strings.TrimPrefix(upstream.URL, "http://")}},
								},
							},
						}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tester := caddytest.NewTester(t).WithDefaultOverrides(caddytest.Config{AdminPort: adminPort})
	tester.InitServer(string(config), "json")
	baseURL := "http://127.0.0.1:" + strconv.Itoa(httpPort)

	gzipResponse, gzipEncoded := requestEncoded(t, tester.Client, baseURL+"/cached", "gzip")
	if !strings.Contains(gzipResponse.Header.Get("Cache-Status"), "fwd=uri-miss") {
		t.Fatalf("unexpected initial Cache-Status %q", gzipResponse.Header.Get("Cache-Status"))
	}
	gzipReader, err := gzip.NewReader(bytes.NewReader(gzipEncoded))
	if err != nil {
		t.Fatal(err)
	}
	gzipDecoded, err := io.ReadAll(gzipReader)
	if closeErr := gzipReader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(gzipDecoded) != largeBody {
		t.Fatalf("invalid gzip cache miss: %v", err)
	}

	zstdResponse, zstdEncoded := requestEncoded(t, tester.Client, baseURL+"/cached", "zstd")
	if !strings.Contains(zstdResponse.Header.Get("Cache-Status"), "hit") {
		t.Fatalf("unexpected cached Cache-Status %q", zstdResponse.Header.Get("Cache-Status"))
	}
	zstdDecoder, err := zstd.NewReader(bytes.NewReader(zstdEncoded))
	if err != nil {
		t.Fatal(err)
	}
	zstdDecoded, err := io.ReadAll(zstdDecoder)
	zstdDecoder.Close()
	if err != nil || string(zstdDecoded) != largeBody {
		t.Fatalf("invalid zstd cache hit: %v", err)
	}
	if actual := upstreamRequests.Load(); actual != 1 {
		t.Fatalf("cache encoding negotiation reached upstream %d times", actual)
	}
}

func requestEncoded(t *testing.T, client *http.Client, url, acceptEncoding string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", acceptEncoding)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %s: %s", response.Status, body)
	}
	return response, body
}

func assertVaryAcceptEncoding(t *testing.T, header http.Header) {
	t.Helper()
	for _, value := range header.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), "Accept-Encoding") {
				return
			}
		}
	}
	t.Fatalf("Vary does not contain Accept-Encoding: %v", header.Values("Vary"))
}

func gzipBytes(t *testing.T, input []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func unusedTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}
