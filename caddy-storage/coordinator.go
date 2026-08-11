package caddystorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	errRemoteUnavailable = errors.New("Swarmlite coordinator unavailable")
	errRemoteNotFound    = errors.New("Swarmlite cache key not found")
)

const kvNamespace = "caddy"

type cacheVersion struct {
	PhysicalUnixMS int64  `json:"physical_unix_ms"`
	Logical        uint64 `json:"logical"`
	ReplicaID      string `json:"replica_id"`
}

type putRequest struct {
	Key              string       `json:"key"`
	ValueBase64      string       `json:"value_base64"`
	Version          cacheVersion `json:"version"`
	ModifiedAtUnixMS int64        `json:"modified_at_unix_ms"`
}

type deleteRequest struct {
	Key              string       `json:"key"`
	Version          cacheVersion `json:"version"`
	ModifiedAtUnixMS int64        `json:"modified_at_unix_ms"`
	Recursive        bool         `json:"recursive"`
}

type putResponse struct {
	Applied bool         `json:"applied"`
	Version cacheVersion `json:"version"`
}

type objectResponse struct {
	Key              string       `json:"key"`
	ValueBase64      string       `json:"value_base64"`
	Version          cacheVersion `json:"version"`
	ModifiedAtUnixMS int64        `json:"modified_at_unix_ms"`
	Size             int64        `json:"size"`
}

type listResponse struct {
	Keys []string `json:"keys"`
}

type statResponse struct {
	Key              string `json:"key"`
	ModifiedAtUnixMS int64  `json:"modified_at_unix_ms"`
	Size             int64  `json:"size"`
	IsValue          bool   `json:"is_value"`
}

type lockAcquireRequest struct {
	Name        string `json:"name"`
	OwnerID     string `json:"owner_id"`
	LeaseMillis uint64 `json:"lease_millis"`
}

type lockAcquireResponse struct {
	Status           string  `json:"status"`
	FencingToken     *uint64 `json:"fencing_token"`
	LeaseUntilUnixMS *int64  `json:"lease_until_unix_ms"`
	RetryAfterMillis *uint64 `json:"retry_after_millis"`
}

type lockMutationRequest struct {
	Name         string  `json:"name"`
	OwnerID      string  `json:"owner_id"`
	FencingToken uint64  `json:"fencing_token"`
	LeaseMillis  *uint64 `json:"lease_millis"`
}

type coordinator struct {
	controller string
	token      string
	timeout    time.Duration
	client     *http.Client
}

func newCoordinator(controller string, token string, timeout time.Duration) *coordinator {
	controller = strings.TrimRight(strings.TrimSpace(controller), "/")
	return &coordinator{
		controller: controller,
		token:      token,
		timeout:    timeout,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (c *coordinator) configured() bool {
	return c != nil && c.controller != "" && c.token != ""
}

func (c *coordinator) put(ctx context.Context, request putRequest) (putResponse, error) {
	request.Key = namespacedKey(request.Key)
	var response putResponse
	err := c.doJSON(ctx, http.MethodPut, "/v1/kv", nil, request, &response)
	return response, err
}

func (c *coordinator) delete(ctx context.Context, request deleteRequest) (putResponse, error) {
	request.Key = namespacedKey(request.Key)
	var response putResponse
	err := c.doJSON(ctx, http.MethodDelete, "/v1/kv", nil, request, &response)
	return response, err
}

func (c *coordinator) object(ctx context.Context, key string) (objectResponse, error) {
	var response objectResponse
	query := url.Values{"key": []string{namespacedKey(key)}}
	err := c.doJSON(ctx, http.MethodGet, "/v1/kv", query, nil, &response)
	response.Key = localKey(response.Key)
	return response, err
}

func (c *coordinator) list(ctx context.Context, path string, recursive bool) ([]string, error) {
	var response listResponse
	query := url.Values{
		"prefix":    []string{namespacedKey(path)},
		"recursive": []string{fmt.Sprint(recursive)},
	}
	err := c.doJSON(ctx, http.MethodGet, "/v1/kv/keys", query, nil, &response)
	for index, key := range response.Keys {
		response.Keys[index] = localKey(key)
	}
	return response.Keys, err
}

func (c *coordinator) stat(ctx context.Context, key string) (statResponse, error) {
	var response statResponse
	query := url.Values{"key": []string{namespacedKey(key)}}
	err := c.doJSON(ctx, http.MethodGet, "/v1/kv/stat", query, nil, &response)
	response.Key = localKey(response.Key)
	return response, err
}

func (c *coordinator) acquire(ctx context.Context, request lockAcquireRequest) (lockAcquireResponse, error) {
	request.Name = namespacedLock(request.Name)
	var response lockAcquireResponse
	err := c.doJSON(ctx, http.MethodPost, "/v1/kv/locks/acquire", nil, request, &response)
	return response, err
}

func (c *coordinator) renew(ctx context.Context, request lockMutationRequest) error {
	request.Name = namespacedLock(request.Name)
	return c.doJSON(ctx, http.MethodPost, "/v1/kv/locks/renew", nil, request, nil)
}

func (c *coordinator) release(ctx context.Context, request lockMutationRequest) error {
	request.Name = namespacedLock(request.Name)
	return c.doJSON(ctx, http.MethodPost, "/v1/kv/locks/release", nil, request, nil)
}

func namespacedKey(key string) string {
	if key == "" {
		return kvNamespace
	}
	return kvNamespace + "/" + key
}

func localKey(key string) string {
	if key == kvNamespace {
		return ""
	}
	return strings.TrimPrefix(key, kvNamespace+"/")
}

func namespacedLock(name string) string {
	return kvNamespace + "/locks/" + name
}

func (c *coordinator) doJSON(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body any,
	output any,
) error {
	if !c.configured() {
		return errRemoteUnavailable
	}
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	requestURL := c.controller + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	response, err := c.send(ctx, method, requestURL, encoded)
	if err != nil {
		return fmt.Errorf("%w: %v", errRemoteUnavailable, err)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusNotFound:
		return errRemoteNotFound
	case response.StatusCode >= 200 && response.StatusCode < 300:
		if output == nil || response.StatusCode == http.StatusNoContent {
			return nil
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(output); err != nil {
			return fmt.Errorf("decode Swarmlite response: %w", err)
		}
		return nil
	default:
		io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("%w: Swarmlite returned HTTP %d", errRemoteUnavailable, response.StatusCode)
	}
}

func (c *coordinator) send(ctx context.Context, method, requestURL string, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	return c.client.Do(request)
}
