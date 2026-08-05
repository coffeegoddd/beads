package uow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/latency"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
)

func NewDoltServerUOWProvider(
	ctx context.Context,
	serverRootDir string,
	database string,
	serverLogFilePath string,
	serverConfigFilePath string,
	backend proxy.Backend,
	rootUser string,
	rootPassword string,
	doltBinExec string,
	proxyPort int,
	idleTimeout time.Duration,
	teamServer bool,
	expectedProjectID string,
	opts ...ProviderOption,
) (UnitOfWorkProvider, error) {
	if idleTimeout == 0 {
		idleTimeout = defaultProxyIdleTimeout
	}
	if database == "" {
		return nil, fmt.Errorf("uow: database name must not be empty (caller should default to %q)", "beads")
	}
	if err := backend.Validate(); err != nil {
		return nil, fmt.Errorf("uow: backend: %w", err)
	}
	if rootUser == "" {
		return nil, fmt.Errorf("uow: rootUser must not be empty")
	}
	if doltBinExec == "" {
		return nil, fmt.Errorf("uow: doltBinExec must not be empty")
	}

	absDone := latency.Span("uow:filepath.Abs")
	absServerRootDir, err := filepath.Abs(serverRootDir)
	if err != nil {
		absDone()
		return nil, fmt.Errorf("uow: resolving server root dir: %w", err)
	}
	absDoltBinExec, err := filepath.Abs(doltBinExec)
	absDone()
	if err != nil {
		return nil, fmt.Errorf("uow: resolving dolt bin exec: %w", err)
	}

	mkdirDone := latency.Span("uow:mkdirServerRoot")
	mkdirErr := os.MkdirAll(absServerRootDir, config.BeadsDirPerm)
	mkdirDone()
	if mkdirErr != nil {
		return nil, fmt.Errorf("uow: creating server root directory: %w", mkdirErr)
	}

	endpointDone := latency.Span("uow:GetCreateDatabaseProxyServerEndpoint")
	ep, err := proxy.GetCreateDatabaseProxyServerEndpoint(absServerRootDir, proxy.OpenOpts{
		Backend:        backend,
		ConfigFilePath: serverConfigFilePath,
		LogFilePath:    serverLogFilePath,
		DoltBinPath:    absDoltBinExec,
		Database:       database,
		IdleTimeout:    idleTimeout,
		Port:           proxyPort,
	})
	endpointDone()
	if err != nil {
		return nil, fmt.Errorf("uow: get proxy endpoint: %w", err)
	}

	return openAndInitSchema(ctx, ep, database, rootUser, rootPassword, "", teamServer, expectedProjectID, applyProviderOptions(opts))
}
