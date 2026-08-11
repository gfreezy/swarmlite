package caddystorage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

func init() {
	caddy.RegisterModule(Module{})
}

// Module configures local-first Caddy storage with optional Swarmlite
// coordination. The local filesystem is always authoritative. Controller
// errors only reduce cross-instance certificate reuse; they never make local
// storage unavailable.
type Module struct {
	Root       string         `json:"root,omitempty"`
	Controller string         `json:"controller,omitempty"`
	Token      string         `json:"token,omitempty"`
	TokenEnv   string         `json:"token_env,omitempty"`
	Timeout    caddy.Duration `json:"timeout,omitempty"`
	LockLease  caddy.Duration `json:"lock_lease,omitempty"`
}

func (Module) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.swarmlite",
		New: func() caddy.Module { return new(Module) },
	}
}

func (m Module) CertMagicStorage() (certmagic.Storage, error) {
	root := m.Root
	if root == "" {
		// Use Caddy's normal data directory so enabling or disabling the
		// optional coordinator does not move the authoritative certificates.
		root = caddy.AppDataDir()
	}
	token := m.Token
	if token == "" && m.TokenEnv != "" {
		token = os.Getenv(m.TokenEnv)
	}
	timeout := time.Duration(m.Timeout)
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	lease := time.Duration(m.LockLease)
	if lease <= 0 {
		lease = 30 * time.Second
	}
	if lease < time.Second || lease > 5*time.Minute {
		return nil, fmt.Errorf("lock_lease must be between 1s and 5m")
	}
	return newStorage(root, m.Controller, token, timeout, lease), nil
}

func (m *Module) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	if !d.Next() {
		return d.Err("expected tokens")
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "root":
			if !d.NextArg() || m.Root != "" {
				return d.ArgErr()
			}
			m.Root = filepath.Clean(d.Val())
		case "controller":
			if !d.NextArg() || m.Controller != "" {
				return d.ArgErr()
			}
			m.Controller = d.Val()
		case "token":
			if !d.NextArg() || m.Token != "" {
				return d.ArgErr()
			}
			m.Token = d.Val()
		case "token_env":
			if !d.NextArg() || m.TokenEnv != "" {
				return d.ArgErr()
			}
			m.TokenEnv = d.Val()
		case "timeout":
			if !d.NextArg() {
				return d.ArgErr()
			}
			value, err := caddy.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("invalid timeout: %v", err)
			}
			m.Timeout = caddy.Duration(value)
		case "lock_lease":
			if !d.NextArg() {
				return d.ArgErr()
			}
			value, err := caddy.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("invalid lock_lease: %v", err)
			}
			m.LockLease = caddy.Duration(value)
		default:
			return d.Errf("unrecognized parameter %q", d.Val())
		}
		if d.NextArg() {
			return d.ArgErr()
		}
	}
	return nil
}

var (
	_ caddy.StorageConverter = (*Module)(nil)
	_ caddyfile.Unmarshaler  = (*Module)(nil)
)
