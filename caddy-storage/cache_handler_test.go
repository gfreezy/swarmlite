package caddystorage

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

func TestNativeCacheHandlerHTTPBehavior(t *testing.T) {
	var countsMu sync.Mutex
	counts := make(map[string]int)
	count := func(path string) int {
		countsMu.Lock()
		defer countsMu.Unlock()
		counts[path]++
		return counts[path]
	}
	currentCount := func(path string) int {
		countsMu.Lock()
		defer countsMu.Unlock()
		return counts[path]
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		invocation := count(request.URL.Path)
		body, _ := io.ReadAll(request.Body)
		response.Header().Set("Content-Type", "text/plain")
		switch request.URL.Path {
		case "/vary":
			response.Header().Set("Vary", "Accept-Language")
			fmt.Fprintf(response, "%s:%d", request.Header.Get("Accept-Language"), invocation)
		case "/no-store":
			response.Header().Set("Cache-Control", "no-store")
			fmt.Fprintf(response, "no-store:%d", invocation)
		case "/cookie":
			response.Header().Set("Set-Cookie", "session=private")
			fmt.Fprintf(response, "cookie:%d", invocation)
		case "/large":
			_, _ = io.WriteString(response, strings.Repeat("x", 128))
		case "/slow":
			time.Sleep(75 * time.Millisecond)
			fmt.Fprintf(response, "slow:%d", invocation)
		default:
			fmt.Fprintf(response, "%s:%s:%d", request.Method, body, invocation)
		}
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
					"cache": map[string]any{
						"listen": []string{fmt.Sprintf("127.0.0.1:%d", httpPort)},
						"routes": []any{map[string]any{
							"handle": []any{
								map[string]any{
									"handler":                  "cache",
									"path":                     filepath.Join(t.TempDir(), "cache.db"),
									"ttl":                      "5m",
									"allowed_http_verbs":       []string{"GET", "POST"},
									"max_cacheable_body_bytes": 64,
									"max_request_body_bytes":   32,
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

	firstBody, firstHeader := cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/basic?q=1", "", nil)
	secondBody, secondHeader := cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/basic?q=1", "", nil)
	if firstBody != secondBody || currentCount("/basic") != 1 {
		t.Fatalf("GET response was not cached: first=%q second=%q count=%d", firstBody, secondBody, currentCount("/basic"))
	}
	assertCacheStatus(t, firstHeader, "fwd=uri-miss")
	assertCacheStatus(t, secondHeader, "hit")
	cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/basic?q=2", "", nil)
	if currentCount("/basic") != 2 {
		t.Fatal("query string was not included in the cache key")
	}

	postHeaders := http.Header{"Content-Type": []string{"application/json"}}
	postOne, _ := cacheRequest(t, tester.Client, http.MethodPost, baseURL+"/post", `{"query":1}`, postHeaders)
	postOneAgain, postHit := cacheRequest(t, tester.Client, http.MethodPost, baseURL+"/post", `{"query":1}`, postHeaders)
	postTwo, _ := cacheRequest(t, tester.Client, http.MethodPost, baseURL+"/post", `{"query":2}`, postHeaders)
	if postOne != postOneAgain || postOne == postTwo || currentCount("/post") != 2 {
		t.Fatalf("POST body cache key failed: %q %q %q count=%d", postOne, postOneAgain, postTwo, currentCount("/post"))
	}
	assertCacheStatus(t, postHit, "hit")

	for _, language := range []string{"en", "fr", "en"} {
		cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/vary", "", http.Header{
			"Accept-Language": []string{language},
		})
	}
	if currentCount("/vary") != 2 {
		t.Fatalf("Vary variants were not selected independently: %d", currentCount("/vary"))
	}

	for range 2 {
		cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/no-store", "", nil)
		cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/cookie", "", nil)
		cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/auth", "", http.Header{
			"Authorization": []string{"Bearer private"},
		})
		cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/large", "", nil)
		cacheRequest(t, tester.Client, http.MethodPost, baseURL+"/large-request", strings.Repeat("q", 40), nil)
		cacheRequest(t, tester.Client, http.MethodPut, baseURL+"/put", "value", nil)
	}
	for _, path := range []string{"/no-store", "/cookie", "/auth", "/large", "/large-request", "/put"} {
		if currentCount(path) != 2 {
			t.Fatalf("%s was unexpectedly cached: %d", path, currentCount(path))
		}
	}

	cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/head", "", nil)
	headBody, headHeader := cacheRequest(t, tester.Client, http.MethodHead, baseURL+"/head", "", nil)
	if headBody != "" || currentCount("/head") != 1 {
		t.Fatalf("HEAD did not reuse GET cache: body=%q count=%d", headBody, currentCount("/head"))
	}
	assertCacheStatus(t, headHeader, "hit")

	const concurrentRequests = 8
	start := make(chan struct{})
	results := make(chan string, concurrentRequests)
	var wait sync.WaitGroup
	for range concurrentRequests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			body, _ := cacheRequest(t, tester.Client, http.MethodGet, baseURL+"/slow", "", nil)
			results <- body
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for body := range results {
		if body != "slow:1" {
			t.Errorf("unexpected coalesced response %q", body)
		}
	}
	if currentCount("/slow") != 1 {
		t.Fatalf("concurrent cache miss was not coalesced: %d", currentCount("/slow"))
	}
}

func TestCacheMethodPolicyDefaultsToAllAndHonorsAllowlist(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "https://example.test/query", strings.NewReader("query"))
	if decision := new(CacheHandler).requestCachePolicy(request); !decision.lookup || !decision.store {
		t.Fatalf("explicit cache route did not accept POST by default: %#v", decision)
	}

	restricted := &CacheHandler{methods: map[string]struct{}{http.MethodGet: {}}}
	if decision := restricted.requestCachePolicy(request); decision.lookup || decision.store {
		t.Fatalf("method allowlist did not bypass POST: %#v", decision)
	}
	head := httptest.NewRequest(http.MethodHead, "https://example.test/query", nil)
	if decision := restricted.requestCachePolicy(head); !decision.lookup || decision.store {
		t.Fatalf("GET allowlist did not permit HEAD lookup: %#v", decision)
	}
}

func TestCacheBaseKeyHonorsSouinDisableQuery(t *testing.T) {
	first := httptest.NewRequest(http.MethodGet, "https://example.test/page?utm_source=one", nil)
	second := httptest.NewRequest(http.MethodGet, "https://example.test/page?utm_source=two", nil)

	includingQuery := &CacheHandler{namespace: "test"}
	if includingQuery.baseKey(first, "") == includingQuery.baseKey(second, "") {
		t.Fatal("query-aware cache keys unexpectedly matched")
	}

	excludingQuery := &CacheHandler{namespace: "test", disableQuery: true}
	if excludingQuery.baseKey(first, "") != excludingQuery.baseKey(second, "") {
		t.Fatal("query-independent cache keys unexpectedly differed")
	}
}

func cacheRequest(
	t *testing.T,
	client *http.Client,
	method string,
	url string,
	body string,
	header http.Header,
) (string, http.Header) {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, values := range header {
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response %s: %s", response.Status, content)
	}
	return string(content), response.Header.Clone()
}

func assertCacheStatus(t *testing.T, header http.Header, expected string) {
	t.Helper()
	if actual := header.Get("Cache-Status"); !strings.Contains(actual, expected) {
		t.Fatalf("Cache-Status %q does not contain %q", actual, expected)
	}
}
