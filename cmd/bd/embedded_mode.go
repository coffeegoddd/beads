package main

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/configfile"
)

// detectWorkspaceBackend returns the configured backend for the current workspace.
// Best-effort: returns ("", false) if it can't be determined.
func detectWorkspaceBackend() (string, bool) {
	beadsDir := beads.FindBeadsDir()
	if beadsDir == "" {
		// Fall back to configured dbPath if set.
		if dbPath != "" {
			beadsDir = filepath.Dir(dbPath)
		} else if found := beads.FindDatabasePath(); found != "" {
			beadsDir = filepath.Dir(found)
		}
	}
	if beadsDir == "" {
		return "", false
	}
	cfg, err := configfile.Load(beadsDir)
	if err != nil || cfg == nil {
		return "", false
	}
	return cfg.GetBackend(), true
}

func isEmbeddedDoltWorkspace() bool {
	backend, ok := detectWorkspaceBackend()
	return ok && backend == configfile.BackendEmbeddedDolt
}

func guardNoDaemonForEmbeddedDolt(_ *cobra.Command, _ []string) error {
	if isEmbeddedDoltWorkspace() {
		return fmt.Errorf("daemon mode is disabled for embedded-dolt backend")
	}
	return nil
}
