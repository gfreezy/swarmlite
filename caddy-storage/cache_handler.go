package caddystorage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

const (
	defaultCacheTTL              = 5 * time.Minute
	defaultMaxCacheableBodyBytes = int64(10 << 20)
	defaultMaxRequestBodyBytes   = int64(1 << 20)
	cacheStatusName              = "Swarmlite"
)

// CacheHandler is Swarmlite's node-local HTTP response cache. It owns the HTTP
// cache semantics directly and persists complete responses in SQLite without
// going through cache-handler or Souin storage-provider abstractions.
type CacheHandler struct {
	Path                  string         `json:"path,omitempty"`
	TTL                   caddy.Duration `json:"ttl,omitempty"`
	MaxCacheableBodyBytes int64          `json:"max_cacheable_body_bytes,omitempty"`
	MaxRequestBodyBytes   int64          `json:"max_request_body_bytes,omitempty"`
	AllowedHTTPVerbs      *[]string      `json:"allowed_http_verbs,omitempty"`
	Key                   *CacheKey      `json:"key,omitempty"`
	StatusCodes           []int          `json:"status_codes,omitempty"`

	MaxSizeBytes         int64          `json:"max_size_bytes,omitempty"`
	LowWaterPercent      int            `json:"low_water_percent,omitempty"`
	HitSampleRatio       uint64         `json:"hit_sample_ratio,omitempty"`
	AccessUpdateInterval caddy.Duration `json:"access_update_interval,omitempty"`
	CacheSizeKiB         int64          `json:"cache_size_kib,omitempty"`
	MmapSizeBytes        *int64         `json:"mmap_size_bytes,omitempty"`
	ReadConnections      int            `json:"read_connections,omitempty"`
	BusyTimeout          caddy.Duration `json:"busy_timeout,omitempty"`
	CleanupInterval      caddy.Duration `json:"cleanup_interval,omitempty"`
	JournalSizeLimit     int64          `json:"journal_size_limit,omitempty"`

	store           *sqliteResponseStore
	logger          *zap.Logger
	ttl             time.Duration
	methods         map[string]struct{}
	statusCodes     map[int]struct{}
	keyHeaders      []string
	disableQuery    bool
	queryParameters []string
	namespace       string
	flights         cacheFlightGroup
}

// CacheKey keeps the compatible subset of Souin's cache key configuration and
// adds a structured query-parameter allowlist. Native cache keys are always
// hashed, so Hash is accepted without changing key identity.
type CacheKey struct {
	DisableQuery    bool     `json:"disable_query,omitempty"`
	Hash            bool     `json:"hash,omitempty"`
	Headers         []string `json:"headers,omitempty"`
	QueryParameters []string `json:"query_parameters,omitempty"`
}

func init() {
	caddy.RegisterModule(CacheHandler{})
}

func (CacheHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.cache",
		New: func() caddy.Module { return new(CacheHandler) },
	}
}

func (handler *CacheHandler) Provision(ctx caddy.Context) error {
	handler.logger = ctx.Logger(handler)
	handler.ttl = time.Duration(handler.TTL)
	if handler.ttl == 0 {
		handler.ttl = defaultCacheTTL
	}
	if handler.ttl < 0 {
		return errors.New("cache ttl must be positive")
	}
	if handler.MaxCacheableBodyBytes == 0 {
		handler.MaxCacheableBodyBytes = defaultMaxCacheableBodyBytes
	}
	if handler.MaxCacheableBodyBytes < 0 {
		return errors.New("cache max_cacheable_body_bytes must be positive")
	}
	if handler.MaxRequestBodyBytes == 0 {
		handler.MaxRequestBodyBytes = defaultMaxRequestBodyBytes
	}
	if handler.MaxRequestBodyBytes < 0 {
		return errors.New("cache max_request_body_bytes must be positive")
	}
	configuredMethods := []string{http.MethodGet, http.MethodHead}
	if handler.AllowedHTTPVerbs != nil {
		if len(*handler.AllowedHTTPVerbs) == 0 {
			return errors.New("cache allowed_http_verbs must not be empty")
		}
		configuredMethods = *handler.AllowedHTTPVerbs
	}
	handler.methods = make(map[string]struct{}, len(configuredMethods))
	methods := make([]string, 0, len(configuredMethods))
	for _, method := range configuredMethods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if !validHTTPHeaderName(method) || method == http.MethodConnect {
			return fmt.Errorf("cache allowed_http_verbs contains unsupported method %q", method)
		}
		if _, found := handler.methods[method]; found {
			continue
		}
		handler.methods[method] = struct{}{}
		methods = append(methods, method)
	}
	sort.Strings(methods)
	handler.AllowedHTTPVerbs = &methods

	configuredKeyHeaders := []string(nil)
	if handler.Key != nil {
		configuredKeyHeaders = append(configuredKeyHeaders, handler.Key.Headers...)
		handler.disableQuery = handler.Key.DisableQuery
	}
	seenHeaders := make(map[string]struct{}, len(configuredKeyHeaders))
	keyHeaders := make([]string, 0, len(configuredKeyHeaders))
	for _, name := range configuredKeyHeaders {
		name = strings.TrimSpace(name)
		if !validHTTPHeaderName(name) {
			return fmt.Errorf("cache key.headers contains invalid field %q", name)
		}
		name = http.CanonicalHeaderKey(name)
		if _, found := seenHeaders[name]; found {
			continue
		}
		seenHeaders[name] = struct{}{}
		keyHeaders = append(keyHeaders, name)
	}
	sort.Strings(keyHeaders)
	handler.keyHeaders = keyHeaders
	if handler.Key != nil {
		handler.Key.Headers = keyHeaders
	}

	if handler.Key != nil && handler.Key.QueryParameters != nil {
		if handler.disableQuery {
			return errors.New("cache key.disable_query and key.query_parameters cannot be combined")
		}
		if len(handler.Key.QueryParameters) == 0 {
			return errors.New("cache key.query_parameters must not be empty")
		}
		seenParameters := make(map[string]struct{}, len(handler.Key.QueryParameters))
		queryParameters := make([]string, 0, len(handler.Key.QueryParameters))
		for _, name := range handler.Key.QueryParameters {
			if name == "" {
				return errors.New("cache key.query_parameters contains an empty parameter name")
			}
			if _, found := seenParameters[name]; found {
				return fmt.Errorf("cache key.query_parameters contains duplicate parameter %q", name)
			}
			seenParameters[name] = struct{}{}
			queryParameters = append(queryParameters, name)
		}
		sort.Strings(queryParameters)
		handler.queryParameters = queryParameters
		handler.Key.QueryParameters = queryParameters
	}

	if len(handler.StatusCodes) == 0 {
		handler.StatusCodes = []int{http.StatusOK}
	}
	handler.statusCodes = make(map[int]struct{}, len(handler.StatusCodes))
	statusCodes := make([]int, 0, len(handler.StatusCodes))
	for _, status := range handler.StatusCodes {
		if status < 200 || status > 599 || status == http.StatusNotModified {
			return fmt.Errorf("cache status_codes contains unsupported status %d", status)
		}
		if _, found := handler.statusCodes[status]; found {
			continue
		}
		handler.statusCodes[status] = struct{}{}
		statusCodes = append(statusCodes, status)
	}
	sort.Ints(statusCodes)
	handler.StatusCodes = statusCodes

	handler.namespace = handler.cacheNamespace()
	store, err := newSQLiteResponseStore(sqliteOptions{
		path:             handler.Path,
		maxSizeBytes:     handler.MaxSizeBytes,
		lowWaterPercent:  handler.LowWaterPercent,
		hitSampleRatio:   handler.HitSampleRatio,
		accessInterval:   time.Duration(handler.AccessUpdateInterval),
		cacheSizeKiB:     handler.CacheSizeKiB,
		mmapSizeBytes:    handler.MmapSizeBytes,
		readConnections:  handler.ReadConnections,
		busyTimeout:      time.Duration(handler.BusyTimeout),
		cleanupInterval:  time.Duration(handler.CleanupInterval),
		journalSizeLimit: handler.JournalSizeLimit,
	}, handler.logger)
	if err != nil {
		return err
	}
	handler.store = store
	return nil
}

func (handler *CacheHandler) Cleanup() error {
	if handler.store == nil {
		return nil
	}
	return handler.store.Close()
}

func (handler *CacheHandler) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
	next caddyhttp.Handler,
) error {
	decision := handler.requestCachePolicy(request)
	if !decision.lookup && !decision.store {
		response.Header().Set("Cache-Status", cacheStatusName+"; fwd=bypass")
		return next.ServeHTTP(response, request)
	}

	bodyKey, withinLimit, err := handler.requestBodyKey(request)
	if err != nil {
		return err
	}
	if !withinLimit {
		response.Header().Set("Cache-Status", cacheStatusName+"; fwd=bypass")
		return next.ServeHTTP(response, request)
	}
	baseKey := handler.baseKey(request, bodyKey)
	if decision.lookup {
		if entry := handler.load(request.Context(), baseKey, request); entry != nil {
			return serveCacheEntry(response, request, entry)
		}
	}

	// A HEAD miss is passed through but never stored because its body cannot
	// populate a later GET. A cached GET can still satisfy HEAD requests.
	if request.Method == http.MethodHead {
		response.Header().Set("Cache-Status", cacheStatusName+"; fwd=uri-miss")
		return next.ServeHTTP(response, request)
	}

	var flight *cacheFlight
	if decision.lookup {
		flightKey := cacheFlightKey{
			base: baseKey,
			vary: requestVaryKey(request, handler.keyHeaders),
		}
		var leader bool
		flight, leader = handler.flights.acquire(flightKey)
		if !leader {
			select {
			case <-flight.done:
				if entry := handler.load(request.Context(), baseKey, request); entry != nil {
					return serveCacheEntry(response, request, entry)
				}
			case <-request.Context().Done():
				return request.Context().Err()
			}
			// The leading response was not cacheable. Avoid serializing every
			// waiter behind repeated attempts and let this request reach origin.
			flight = nil
		} else {
			defer handler.flights.release(flightKey, flight)
		}
	}

	capture := newCacheCaptureWriter(
		response,
		handler.statusCodes,
		handler.MaxCacheableBodyBytes,
	)
	if err := next.ServeHTTP(capture, request); err != nil {
		return err
	}
	if !decision.store || !capture.cacheable || capture.status == 0 {
		return nil
	}

	varyHeaders, cacheable := responseVaryHeaders(capture.header, handler.keyHeaders)
	if !cacheable {
		return nil
	}
	now := time.Now()
	entry := &cacheEntry{
		BaseKey:     baseKey,
		VaryHeaders: varyHeaders,
		Status:      capture.status,
		Header:      sanitizedCacheHeader(capture.header),
		Body:        capture.body.Bytes(),
		StoredAt:    now,
		ExpiresAt:   now.Add(handler.ttl),
	}
	if err := handler.store.Put(request.Context(), request, entry); err != nil {
		handler.logger.Error("store HTTP response in SQLite cache", zap.Error(err))
	}
	return nil
}

func (handler *CacheHandler) load(
	ctx context.Context,
	baseKey cacheKey,
	request *http.Request,
) *cacheEntry {
	entry, err := handler.store.Get(ctx, baseKey, request)
	if err != nil {
		handler.logger.Error("read HTTP response from SQLite cache", zap.Error(err))
		return nil
	}
	return entry
}

func (handler *CacheHandler) baseKey(request *http.Request, bodyKey string) cacheKey {
	scheme := request.URL.Scheme
	if scheme == "" {
		if request.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	path := request.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	method := request.Method
	if method == http.MethodHead {
		method = http.MethodGet
	}
	contentType := ""
	contentEncoding := ""
	if bodyKey != "" {
		contentType = request.Header.Get("Content-Type")
		contentEncoding = request.Header.Get("Content-Encoding")
	}
	query := request.URL.RawQuery
	if handler.disableQuery {
		query = ""
	} else if len(handler.queryParameters) != 0 {
		query = selectedQuery(request.URL.Query(), handler.queryParameters)
	}
	value := strings.Join([]string{
		handler.namespace,
		strings.ToLower(scheme),
		strings.ToLower(request.Host),
		method,
		path,
		query,
		contentType,
		contentEncoding,
		bodyKey,
	}, "\n")
	sum := sha256.Sum256([]byte(value))
	var key cacheKey
	copy(key[:], sum[:])
	return key
}

func (handler *CacheHandler) cacheNamespace() string {
	methods := "*"
	if handler.AllowedHTTPVerbs != nil {
		methods = strings.Join(*handler.AllowedHTTPVerbs, ",")
	}
	parts := []string{
		"swarmlite-http-cache-v1",
		handler.ttl.String(),
		strconv.FormatInt(handler.MaxCacheableBodyBytes, 10),
		strconv.FormatInt(handler.MaxRequestBodyBytes, 10),
		methods,
		strings.Join(handler.keyHeaders, ","),
		strconv.FormatBool(handler.disableQuery),
		url.Values{"parameter": handler.queryParameters}.Encode(),
	}
	for _, status := range handler.StatusCodes {
		parts = append(parts, strconv.Itoa(status))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func selectedQuery(query url.Values, parameters []string) string {
	selected := make(url.Values, len(parameters))
	for _, name := range parameters {
		if values, found := query[name]; found {
			selected[name] = values
		}
	}
	return selected.Encode()
}

type requestCacheDecision struct {
	lookup bool
	store  bool
}

func (handler *CacheHandler) requestCachePolicy(request *http.Request) requestCacheDecision {
	if request.Method == http.MethodConnect || request.Header.Get("Upgrade") != "" {
		return requestCacheDecision{}
	}
	allowed := request.Method == http.MethodGet || request.Method == http.MethodHead
	if handler.methods != nil {
		_, allowed = handler.methods[request.Method]
		if request.Method == http.MethodHead && !allowed {
			_, allowed = handler.methods[http.MethodGet]
		}
	}
	if !allowed {
		return requestCacheDecision{}
	}
	if request.Header.Get("Range") != "" {
		return requestCacheDecision{}
	}
	for _, field := range []string{
		"If-Match",
		"If-None-Match",
		"If-Modified-Since",
		"If-Unmodified-Since",
		"If-Range",
	} {
		if request.Header.Get(field) != "" {
			return requestCacheDecision{}
		}
	}
	directives := cacheControlDirectives(request.Header.Values("Cache-Control"))
	if _, found := directives["no-store"]; found {
		return requestCacheDecision{}
	}
	decision := requestCacheDecision{lookup: true, store: request.Method != http.MethodHead}
	if _, found := directives["no-cache"]; found {
		decision.lookup = false
	}
	if value, found := directives["max-age"]; found && strings.Trim(value, "\"") == "0" {
		decision.lookup = false
	}
	if strings.EqualFold(strings.TrimSpace(request.Header.Get("Pragma")), "no-cache") {
		decision.lookup = false
	}
	return decision
}

func (handler *CacheHandler) requestBodyKey(request *http.Request) (string, bool, error) {
	if request.Method == http.MethodHead || request.Body == nil || request.Body == http.NoBody || request.ContentLength == 0 {
		return "", true, nil
	}
	if request.ContentLength > handler.MaxRequestBodyBytes {
		return "", false, nil
	}
	original := request.Body
	content, err := io.ReadAll(io.LimitReader(original, handler.MaxRequestBodyBytes+1))
	if err != nil {
		request.Body = &replayRequestBody{
			Reader: io.MultiReader(bytes.NewReader(content), original),
			Closer: original,
		}
		return "", false, fmt.Errorf("read cacheable request body: %w", err)
	}
	if int64(len(content)) > handler.MaxRequestBodyBytes {
		request.Body = &replayRequestBody{
			Reader: io.MultiReader(bytes.NewReader(content), original),
			Closer: original,
		}
		return "", false, nil
	}
	if err := original.Close(); err != nil {
		return "", false, fmt.Errorf("close cacheable request body: %w", err)
	}
	request.Body = io.NopCloser(bytes.NewReader(content))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), true, nil
}

type replayRequestBody struct {
	io.Reader
	io.Closer
}

func serveCacheEntry(
	response http.ResponseWriter,
	request *http.Request,
	entry *cacheEntry,
) error {
	for name, values := range entry.Header {
		response.Header()[name] = append([]string(nil), values...)
	}
	age := max(int64(0), int64(time.Since(entry.StoredAt)/time.Second))
	ttl := max(int64(0), int64(time.Until(entry.ExpiresAt)/time.Second))
	response.Header().Set("Age", strconv.FormatInt(age, 10))
	response.Header().Set(
		"Cache-Status",
		fmt.Sprintf("%s; hit; ttl=%d", cacheStatusName, ttl),
	)
	response.WriteHeader(entry.Status)
	if request.Method == http.MethodHead {
		return nil
	}
	_, err := response.Write(entry.Body)
	return err
}

func requestVaryKey(request *http.Request, fields []string) cacheKey {
	hash := sha256.New()
	writeRequestHeaderFingerprint(hash, request, fields)
	return compactCacheKey(hash.Sum(nil))
}

func responseCacheKey(
	baseKey cacheKey,
	generation int64,
	request *http.Request,
	fields []string,
) cacheKey {
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarmlite-http-cache-entry-v1"))
	_, _ = hash.Write(baseKey[:])
	writeFingerprintPart(hash, strconv.FormatInt(generation, 10))
	writeRequestHeaderFingerprint(hash, request, fields)
	return compactCacheKey(hash.Sum(nil))
}

func writeRequestHeaderFingerprint(hash io.Writer, request *http.Request, fields []string) {
	for _, field := range fields {
		writeFingerprintPart(hash, http.CanonicalHeaderKey(field))
		values := request.Header.Values(field)
		writeFingerprintPart(hash, strconv.Itoa(len(values)))
		for _, value := range values {
			writeFingerprintPart(hash, value)
		}
	}
}

func compactCacheKey(sum []byte) cacheKey {
	var key cacheKey
	copy(key[:], sum)
	return key
}

func writeFingerprintPart(writer io.Writer, value string) {
	_, _ = io.WriteString(writer, strconv.Itoa(len(value)))
	_, _ = io.WriteString(writer, ":")
	_, _ = io.WriteString(writer, value)
}

func responseVaryHeaders(header http.Header, configured []string) ([]string, bool) {
	fields := make(map[string]struct{}, len(configured))
	for _, field := range configured {
		fields[http.CanonicalHeaderKey(field)] = struct{}{}
	}
	for _, value := range header.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if field == "*" || !validHTTPHeaderName(field) {
				return nil, false
			}
			fields[http.CanonicalHeaderKey(field)] = struct{}{}
		}
	}
	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	sort.Strings(result)
	return result, true
}

func cacheControlDirectives(values []string) map[string]string {
	result := make(map[string]string)
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			name, parameter, found := strings.Cut(strings.TrimSpace(directive), "=")
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			if !found {
				parameter = ""
			}
			result[name] = strings.TrimSpace(parameter)
		}
	}
	return result
}

func responseMayBeStored(statusCodes map[int]struct{}, status int, header http.Header) bool {
	if _, allowed := statusCodes[status]; !allowed {
		return false
	}
	return header.Get("Set-Cookie") == "" && header.Get("Content-Range") == ""
}

func sanitizedCacheHeader(header http.Header) http.Header {
	result := header.Clone()
	for _, value := range result.Values("Connection") {
		for _, field := range strings.Split(value, ",") {
			result.Del(strings.TrimSpace(field))
		}
	}
	for _, field := range []string{
		"Age",
		"Cache-Status",
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Proxy-Connection",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	} {
		result.Del(field)
	}
	return result
}

func validHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if ('a' <= character && character <= 'z') ||
			('A' <= character && character <= 'Z') ||
			('0' <= character && character <= '9') ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

type cacheCaptureWriter struct {
	http.ResponseWriter
	statusCodes map[int]struct{}
	maxBody     int64
	status      int
	header      http.Header
	body        bytes.Buffer
	wroteHeader bool
	cacheable   bool
}

func newCacheCaptureWriter(
	response http.ResponseWriter,
	statusCodes map[int]struct{},
	maxBody int64,
) *cacheCaptureWriter {
	return &cacheCaptureWriter{
		ResponseWriter: response,
		statusCodes:    statusCodes,
		maxBody:        maxBody,
	}
}

func (writer *cacheCaptureWriter) WriteHeader(status int) {
	if writer.wroteHeader {
		return
	}
	writer.wroteHeader = true
	writer.status = status
	writer.Header().Set("Cache-Status", cacheStatusName+"; fwd=uri-miss")
	writer.header = writer.Header().Clone()
	writer.cacheable = responseMayBeStored(writer.statusCodes, status, writer.header)
	if length, err := strconv.ParseInt(writer.header.Get("Content-Length"), 10, 64); err == nil && length > writer.maxBody {
		writer.cacheable = false
	}
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *cacheCaptureWriter) Write(value []byte) (int, error) {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	written, err := writer.ResponseWriter.Write(value)
	if writer.cacheable && written > 0 {
		if int64(writer.body.Len())+int64(written) <= writer.maxBody {
			_, _ = writer.body.Write(value[:written])
		} else {
			writer.cacheable = false
			writer.body.Reset()
		}
	}
	return written, err
}

func (writer *cacheCaptureWriter) ReadFrom(reader io.Reader) (int64, error) {
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		read, readErr := reader.Read(buffer)
		if read > 0 {
			written, writeErr := writer.Write(buffer[:read])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
			if written != read {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
	}
}

func (writer *cacheCaptureWriter) Flush() {
	_ = writer.FlushError()
}

func (writer *cacheCaptureWriter) FlushError() error {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(writer.ResponseWriter).Flush()
}

func (writer *cacheCaptureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	writer.cacheable = false
	writer.body.Reset()
	return http.NewResponseController(writer.ResponseWriter).Hijack()
}

func (writer *cacheCaptureWriter) Push(target string, options *http.PushOptions) error {
	if pusher, ok := writer.ResponseWriter.(http.Pusher); ok {
		return pusher.Push(target, options)
	}
	return http.ErrNotSupported
}

func (writer *cacheCaptureWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

type cacheFlight struct {
	done chan struct{}
}

type cacheFlightKey struct {
	base cacheKey
	vary cacheKey
}

type cacheFlightGroup struct {
	sync.Mutex
	calls map[cacheFlightKey]*cacheFlight
}

func (group *cacheFlightGroup) acquire(key cacheFlightKey) (*cacheFlight, bool) {
	group.Lock()
	defer group.Unlock()
	if group.calls == nil {
		group.calls = make(map[cacheFlightKey]*cacheFlight)
	}
	if call := group.calls[key]; call != nil {
		return call, false
	}
	call := &cacheFlight{done: make(chan struct{})}
	group.calls[key] = call
	return call, true
}

func (group *cacheFlightGroup) release(key cacheFlightKey, call *cacheFlight) {
	group.Lock()
	if group.calls[key] == call {
		delete(group.calls, key)
		close(call.done)
	}
	group.Unlock()
}

var (
	_ caddy.Module                = (*CacheHandler)(nil)
	_ caddy.Provisioner           = (*CacheHandler)(nil)
	_ caddy.CleanerUpper          = (*CacheHandler)(nil)
	_ caddyhttp.MiddlewareHandler = (*CacheHandler)(nil)
	_ http.ResponseWriter         = (*cacheCaptureWriter)(nil)
	_ io.ReaderFrom               = (*cacheCaptureWriter)(nil)
	_ http.Flusher                = (*cacheCaptureWriter)(nil)
	_ http.Hijacker               = (*cacheCaptureWriter)(nil)
	_ http.Pusher                 = (*cacheCaptureWriter)(nil)
)
