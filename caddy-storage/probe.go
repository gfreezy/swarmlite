package caddystorage

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

const (
	gatewayIDEnv     = "SWARMLITE_GATEWAY_ID"
	gatewayProbePath = "/.well-known/swarmlite/gateway-owner"
)

var errNotGatewayOwner = errors.New("certificate hostname is routed to another Gateway")

type gatewayProbeResponse struct {
	GatewayID string `json:"gateway_id"`
	Hostname  string `json:"hostname"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
}

// GatewayProbeHandler identifies the Gateway reached through a public
// hostname without disclosing the cluster token used to authenticate it.
type GatewayProbeHandler struct {
	gatewayID string
	token     []byte
}

func init() {
	caddy.RegisterModule(GatewayProbeHandler{})
}

func (GatewayProbeHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.swarmlite_gateway_probe",
		New: func() caddy.Module { return new(GatewayProbeHandler) },
	}
}

func (h *GatewayProbeHandler) Provision(caddy.Context) error {
	h.gatewayID = strings.TrimSpace(os.Getenv(gatewayIDEnv))
	if h.gatewayID == "" {
		return fmt.Errorf("%s must identify this managed Gateway", gatewayIDEnv)
	}
	h.token = []byte(os.Getenv("SWARMLITE_TOKEN"))
	if len(h.token) < 16 {
		return errors.New("SWARMLITE_TOKEN must contain at least 16 bytes")
	}
	return nil
}

func (h GatewayProbeHandler) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
	_ caddyhttp.Handler,
) error {
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		response.WriteHeader(http.StatusMethodNotAllowed)
		return nil
	}
	nonce := request.URL.Query().Get("nonce")
	if err := validateProbeNonce(nonce); err != nil {
		http.Error(response, "invalid probe nonce", http.StatusBadRequest)
		return nil
	}
	hostname, err := normalizeProbeHostname(request.Host)
	if err != nil {
		http.Error(response, "invalid probe hostname", http.StatusBadRequest)
		return nil
	}
	payload := gatewayProbeResponse{
		GatewayID: h.gatewayID,
		Hostname:  hostname,
		Nonce:     nonce,
	}
	payload.Signature = signGatewayProbe(h.token, payload)
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	return json.NewEncoder(response).Encode(payload)
}

type gatewayOwnerProbe func(context.Context, string) (string, error)

func newGatewayOwnerProbe(token string, timeout time.Duration) gatewayOwnerProbe {
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return gatewayOwnerProbeWithClient(token, client)
}

func gatewayOwnerProbeWithClient(token string, client *http.Client) gatewayOwnerProbe {
	return func(ctx context.Context, hostname string) (string, error) {
		hostname, err := normalizeProbeHostname(hostname)
		if err != nil {
			return "", err
		}
		if strings.HasPrefix(hostname, "*.") {
			return "", errors.New("wildcard hostnames cannot use an HTTP Gateway probe")
		}
		nonce, err := newProbeNonce()
		if err != nil {
			return "", err
		}
		endpoint := url.URL{
			Scheme: "http",
			Host:   hostname,
			Path:   gatewayProbePath,
		}
		query := endpoint.Query()
		query.Set("nonce", nonce)
		endpoint.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return "", err
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "swarmlite-gateway-probe/1")
		response, err := client.Do(request)
		if err != nil {
			return "", err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
			return "", fmt.Errorf("Gateway probe returned HTTP %d", response.StatusCode)
		}
		var payload gatewayProbeResponse
		if err := json.NewDecoder(io.LimitReader(response.Body, 8<<10)).Decode(&payload); err != nil {
			return "", fmt.Errorf("decode Gateway probe: %w", err)
		}
		if payload.Hostname != hostname || payload.Nonce != nonce {
			return "", errors.New("Gateway probe response does not match the request")
		}
		if payload.GatewayID == "" || len(payload.GatewayID) > 512 {
			return "", errors.New("Gateway probe returned an invalid gateway_id")
		}
		expected := signGatewayProbe([]byte(token), payload)
		if !hmac.Equal([]byte(payload.Signature), []byte(expected)) {
			return "", errors.New("Gateway probe signature is invalid")
		}
		return payload.GatewayID, nil
	}
}

func newProbeNonce() (string, error) {
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

func validateProbeNonce(value string) error {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) < 16 || len(decoded) > 64 {
		return errors.New("probe nonce must encode 16 to 64 bytes")
	}
	return nil
}

func normalizeProbeHostname(value string) (string, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if hostname, _, err := net.SplitHostPort(value); err == nil {
		value = hostname
	}
	value = strings.TrimSuffix(strings.Trim(value, "[]"), ".")
	if value == "" || len(value) > 253 || strings.ContainsAny(value, " /\\\t\r\n") {
		return "", errors.New("invalid probe hostname")
	}
	return value, nil
}

func signGatewayProbe(token []byte, payload gatewayProbeResponse) string {
	mac := hmac.New(sha256.New, token)
	fmt.Fprintf(mac, "swarmlite-gateway-owner-v1\n%s\n%s\n%s", payload.Hostname, payload.Nonce, payload.GatewayID)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

var (
	_ caddy.Provisioner           = (*GatewayProbeHandler)(nil)
	_ caddyhttp.MiddlewareHandler = (*GatewayProbeHandler)(nil)
)
