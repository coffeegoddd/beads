package uow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/latency"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
	"github.com/steveyegge/beads/internal/storage/dbproxy/server"
)

func NewExternalDoltServerUOWProvider(
	ctx context.Context,
	serverRootDir string,
	database string,
	serverLogFilePath string,
	external configfile.ExternalDoltConfig,
	rootUser string,
	rootPassword string,
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
	if rootUser == "" {
		return nil, fmt.Errorf("uow: rootUser must not be empty")
	}
	if err := external.Validate(); err != nil {
		return nil, fmt.Errorf("uow: external: %w", err)
	}

	absDone := latency.Span("uow:filepath.Abs")
	absServerRootDir, err := filepath.Abs(serverRootDir)
	absDone()
	if err != nil {
		return nil, fmt.Errorf("uow: resolving server root dir: %w", err)
	}

	mkdirDone := latency.Span("uow:mkdirServerRoot")
	mkdirErr := os.MkdirAll(absServerRootDir, config.BeadsDirPerm)
	mkdirDone()
	if mkdirErr != nil {
		return nil, fmt.Errorf("uow: creating server root directory: %w", mkdirErr)
	}

	tlsDone := latency.Span("uow:registerExternalTLSConfig")
	tlsConfigName, err := registerExternalTLSConfig(external)
	tlsDone()
	if err != nil {
		return nil, fmt.Errorf("uow: external TLS: %w", err)
	}

	endpointDone := latency.Span("uow:GetCreateDatabaseProxyServerEndpoint")
	ep, err := proxy.GetCreateDatabaseProxyServerEndpoint(absServerRootDir, proxy.OpenOpts{
		Backend:     proxy.BackendExternal,
		LogFilePath: serverLogFilePath,
		External:    external,
		IdleTimeout: idleTimeout,
		Port:        proxyPort,
	})
	endpointDone()
	if err != nil {
		return nil, fmt.Errorf("uow: get proxy endpoint: %w", err)
	}

	return openAndInitSchema(ctx, ep, database, rootUser, rootPassword, tlsConfigName, teamServer, expectedProjectID, applyProviderOptions(opts))
}

func registerExternalTLSConfig(external configfile.ExternalDoltConfig) (string, error) {
	if !external.TLSRequired {
		return "", nil
	}
	tc, err := external.TLSClientConfig()
	if err != nil {
		return "", err
	}
	name := "beads-external-" + server.ExternalDoltServerID(external)
	if err := mysql.RegisterTLSConfig(name, tc); err != nil {
		return "", fmt.Errorf("register TLS config: %w", err)
	}
	return name, nil
}
