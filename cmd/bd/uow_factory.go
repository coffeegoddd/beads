package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/storage/uow"
)

// errUOWUnsupported is returned when the UoW provider factory is asked for a
// mode that has not yet been migrated to the UoW pipeline. Embedded and
// dolt sql-server modes still go through newDoltStore; only proxied-server
// is wired through UoW today.
var errUOWUnsupported = errors.New("uow: only proxied-server mode is supported by the UoW provider; other modes still use the storage.DoltStorage path")

// newUOWProvider constructs a UoW provider from an explicit *dolt.Config.
// Used by bd init in proxied-server mode, where the global storage.DoltStorage
// is replaced by a UoW-driven flow.
//
// For non-proxied modes the function returns errUOWUnsupported so the caller
// knows to fall back to the legacy store factory.
func newUOWProvider(ctx context.Context, cfg *dolt.Config) (uow.UnitOfWorkProvider, error) {
	if cfg == nil {
		return nil, fmt.Errorf("newUOWProvider: cfg is nil")
	}
	if !cfg.ProxiedServer {
		return nil, errUOWUnsupported
	}
	if cfg.BeadsDir == "" {
		return nil, fmt.Errorf("newUOWProvider: cfg.BeadsDir must be set")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("newUOWProvider: cfg.Database must be set")
	}

	doltBin, err := exec.LookPath("dolt")
	if err != nil {
		return nil, fmt.Errorf("newUOWProvider: dolt is not installed (not found in PATH); install from https://docs.dolthub.com/introduction/installation: %w", err)
	}

	persisted, _ := configfile.Load(cfg.BeadsDir)

	rootPath := resolveProxiedServerRootPath(cfg.BeadsDir, persisted)
	if persisted != nil && persisted.GetDoltProxiedServerRootPath(cfg.BeadsDir) != "" {
		if err := validateProxiedServerRootPath(rootPath); err != nil {
			return nil, err
		}
	}

	configPath, err := ensureProxiedServerConfig(cfg.BeadsDir, persisted)
	if err != nil {
		return nil, err
	}

	logPath, isCustomLog := resolveProxiedServerLogPath(cfg.BeadsDir, persisted)
	if isCustomLog {
		if err := validateProxiedServerLogPath(logPath); err != nil {
			return nil, err
		}
	}

	return uow.NewDoltServerUOWProvider(
		ctx,
		rootPath,
		cfg.Database,
		logPath,
		configPath,
		proxy.BackendLocalServer,
		"root",
		"", // proxy is loopback-only, no auth
		doltBin,
	)
}

// newUOWProviderFromConfig is the metadata.json-driven counterpart to
// newUOWProvider. It loads .beads/metadata.json, dispatches on dolt_mode,
// and delegates to newUOWProvider for proxied-server mode. Other modes
// return errUOWUnsupported until they are migrated.
//
// This is a stub for the eventual non-init code path: callers that aren't yet
// UoW-aware should keep using newDoltStoreFromConfig and only switch when
// their command flow has been ported.
func newUOWProviderFromConfig(ctx context.Context, beadsDir string) (uow.UnitOfWorkProvider, error) {
	cfg, err := configfile.Load(beadsDir)
	if err != nil {
		return nil, fmt.Errorf("newUOWProviderFromConfig: load metadata.json: %w", err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("newUOWProviderFromConfig: no metadata.json under %s", beadsDir)
	}
	if !cfg.IsDoltProxiedServerMode() {
		return nil, errUOWUnsupported
	}
	return newUOWProvider(ctx, &dolt.Config{
		BeadsDir:      beadsDir,
		Database:      cfg.GetDoltDatabase(),
		ProxiedServer: true,
	})
}
