package workspace

import (
	"github.com/steveyegge/beads/internal/doltserver"
	"github.com/steveyegge/beads/internal/storage/dolt"
)

// ToDoltConfig produces a dolt.Config ready for dolt.New(ctx, cfg).
// This replaces the manual assembly of dolt.Config in cmd/bd/main.go.
//
// Fields that are caller-specific (ReadOnly, CommitterName, CommitterEmail,
// Remote, SyncRemote, CreateIfMissing, MaxOpenConns) are left at zero values.
// Callers should set those after calling ToDoltConfig().
func (ws *WorkspaceContext) ToDoltConfig() *dolt.Config {
	cfg := &dolt.Config{
		Path:           ws.DatabasePath,
		BeadsDir:       ws.BeadsDir,
		Database:       ws.Database,
		ServerMode:     ws.ServerMode != ServerModeEmbedded,
		ServerHost:     ws.ServerConfig.Host,
		ServerPort:     ws.ServerConfig.Port,
		ServerSocket:   ws.ServerConfig.Socket,
		ServerUser:     ws.ServerConfig.User,
		ServerPassword: ws.ServerConfig.Password,
		ServerTLS:      ws.ServerConfig.TLS,
		AutoStart:      ws.ServerConfig.AutoStart,
	}

	// Resolve the actual dolt data directory (shared server uses a central dir)
	cfg.Path = doltserver.ResolveDoltDir(ws.BeadsDir)

	return cfg
}

// ApplyAutoStart resolves the auto-start policy for CLI use and sets it
// on the WorkspaceContext's ServerConfig. This is the equivalent of
// dolt.ApplyCLIAutoStart but operates on WorkspaceContext directly.
func (ws *WorkspaceContext) ApplyAutoStart() {
	doltCfg := ws.ToDoltConfig()
	dolt.ApplyCLIAutoStart(ws.BeadsDir, doltCfg)
	ws.ServerConfig.AutoStart = doltCfg.AutoStart
}
