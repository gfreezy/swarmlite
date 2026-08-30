package caddystorage

import (
	"fmt"
	"os"

	"github.com/caddyserver/caddy/v2"
	"github.com/darkweak/storages/core"
)

func init() {
	caddy.RegisterModule(SQLiteCacheModule{})
	caddy.RegisterModule(sqliteSimpleFSBridge{})
}

// SQLiteCacheModule exposes the SQLite response-cache provider under the
// native Caddy module ID. cache-handler v0.16.0 cannot dispatch third-party
// provider names yet, so managed Swarmlite configurations currently reach the
// same implementation through sqliteSimpleFSBridge below.
type SQLiteCacheModule struct {
	core.Configuration
	storer *sqliteStorer
}

func (SQLiteCacheModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "storages.cache.sqlite",
		New: func() caddy.Module { return new(SQLiteCacheModule) },
	}
}

func (m *SQLiteCacheModule) Provision(ctx caddy.Context) error {
	logger := ctx.Logger(m).Sugar()
	storer, err := newSQLiteStorer(
		m.Configuration.Provider,
		logger,
		m.Configuration.Stale,
		"SQLITE",
		func(options sqliteOptions) string {
			return fmt.Sprintf("%s-%s", options.path, m.Configuration.Stale)
		},
	)
	if err != nil {
		return err
	}
	m.storer = storer
	core.RegisterStorage(storer)
	return nil
}

func (m *SQLiteCacheModule) Cleanup() error {
	if m.storer == nil {
		return nil
	}
	return m.storer.Reset()
}

// sqliteSimpleFSBridge is a compatibility adapter for cache-handler v0.16.0.
// That release hard-codes its provider dispatch list. Swarmlite uses the
// otherwise-unused SimpleFS slot to load SQLite without maintaining a fork of
// cache-handler. The registered name and UUID deliberately match the values
// cache-handler computes for SimpleFS; the stored data is SQLite.
type sqliteSimpleFSBridge struct {
	core.Configuration
	storer *sqliteStorer
}

func (sqliteSimpleFSBridge) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "storages.cache.simplefs",
		New: func() caddy.Module { return new(sqliteSimpleFSBridge) },
	}
}

func (m *sqliteSimpleFSBridge) Provision(ctx caddy.Context) error {
	logger := ctx.Logger(m).Sugar()
	storer, err := newSQLiteStorer(
		m.Configuration.Provider,
		logger,
		m.Configuration.Stale,
		"SIMPLEFS",
		func(options sqliteOptions) string {
			path := configuredSQLitePath(m.Configuration.Provider)
			if path == "" {
				path, _ = os.Getwd()
			}
			return fmt.Sprintf("%s-%d", path, configuredSimpleFSSize(m.Configuration.Provider))
		},
	)
	if err != nil {
		return err
	}
	m.storer = storer
	core.RegisterStorage(storer)
	return nil
}

func (m *sqliteSimpleFSBridge) Cleanup() error {
	if m.storer == nil {
		return nil
	}
	return m.storer.Reset()
}

var (
	_ caddy.Module       = (*SQLiteCacheModule)(nil)
	_ caddy.Provisioner  = (*SQLiteCacheModule)(nil)
	_ caddy.CleanerUpper = (*SQLiteCacheModule)(nil)
	_ caddy.Module       = (*sqliteSimpleFSBridge)(nil)
	_ caddy.Provisioner  = (*sqliteSimpleFSBridge)(nil)
	_ caddy.CleanerUpper = (*sqliteSimpleFSBridge)(nil)
)
