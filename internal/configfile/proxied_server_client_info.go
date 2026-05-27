package configfile

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ProxiedServerClientInfoFileName is the gitignored sidecar that records
// machine-local paths the proxied-server client needs to reach its dolt
// sql-server: the root directory (lockfiles, pidfiles, child .dolt repo),
// a user-supplied server YAML config, and the server log file.
//
// These were previously stored in metadata.json with a strip-on-save guard
// that silently dropped absolute paths. The sidecar lets us keep absolute
// paths (resolved from --proxied-server-* flags at init time) without
// leaking machine-specific paths through git.
const ProxiedServerClientInfoFileName = "proxied_server_client_info.json"

// ProxiedServerClientInfo holds the machine-local paths used by a proxied-server
// client. All fields hold absolute paths; relative values stored on disk are
// resolved against beadsDir at load time.
type ProxiedServerClientInfo struct {
	RootPath   string `json:"root_path,omitempty"`
	ConfigPath string `json:"config_path,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
}

// ProxiedServerClientInfoPath returns the absolute path to the sidecar file
// inside beadsDir.
func ProxiedServerClientInfoPath(beadsDir string) string {
	return filepath.Join(beadsDir, ProxiedServerClientInfoFileName)
}

// LoadProxiedServerClientInfo reads the sidecar from beadsDir. Returns
// (nil, nil) when the file does not exist — callers should treat that as
// "no overrides; use defaults."
func LoadProxiedServerClientInfo(beadsDir string) (*ProxiedServerClientInfo, error) {
	path := ProxiedServerClientInfoPath(beadsDir)
	data, err := os.ReadFile(path) // #nosec G304 - controlled path
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", ProxiedServerClientInfoFileName, err)
	}
	var info ProxiedServerClientInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", ProxiedServerClientInfoFileName, err)
	}
	return &info, nil
}

// SaveProxiedServerClientInfo writes the sidecar to beadsDir. Empty fields
// are omitted from the JSON. The file is gitignored via .beads/.gitignore.
func SaveProxiedServerClientInfo(beadsDir string, info *ProxiedServerClientInfo) error {
	if info == nil {
		info = &ProxiedServerClientInfo{}
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling %s: %w", ProxiedServerClientInfoFileName, err)
	}
	path := ProxiedServerClientInfoPath(beadsDir)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", ProxiedServerClientInfoFileName, err)
	}
	return nil
}

// resolveSidecarPath returns the path as absolute. Empty input returns empty;
// absolute input is returned as-is; relative input is joined against beadsDir.
func resolveSidecarPath(beadsDir, p string) string {
	if p == "" {
		return ""
	}
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(beadsDir, p)
}

// ResolvedRootPath returns the absolute root path from the sidecar, or "" if
// unset. Relative entries are joined against beadsDir.
func (i *ProxiedServerClientInfo) ResolvedRootPath(beadsDir string) string {
	if i == nil {
		return ""
	}
	return resolveSidecarPath(beadsDir, i.RootPath)
}

// ResolvedConfigPath returns the absolute server-config YAML path from the
// sidecar, or "" if unset.
func (i *ProxiedServerClientInfo) ResolvedConfigPath(beadsDir string) string {
	if i == nil {
		return ""
	}
	return resolveSidecarPath(beadsDir, i.ConfigPath)
}

// ResolvedLogPath returns the absolute log path from the sidecar, or "" if
// unset.
func (i *ProxiedServerClientInfo) ResolvedLogPath(beadsDir string) string {
	if i == nil {
		return ""
	}
	return resolveSidecarPath(beadsDir, i.LogPath)
}
