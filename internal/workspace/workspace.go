// Package workspace provides unified resolution of all beads workspace
// information. It replaces the scattered resolution across beads.FindBeadsDir,
// context.RepoContext, configfile.Config, doltserver.ResolveServerMode, and
// cmd/bd/main.go's PersistentPreRun glue code.
//
// The central type is WorkspaceContext, a single struct that holds every piece
// of information needed to locate, connect to, and operate on a beads
// workspace: filesystem paths, git/worktree state, redirect state, server
// configuration, and project identity.
//
// Usage:
//
//	ws, err := workspace.Resolve()
//	if err != nil {
//	    return err
//	}
//	fmt.Println(ws.BeadsDir)           // resolved .beads directory
//	fmt.Println(ws.ServerConfig.Port)  // resolved dolt server port
//	cfg := ws.ToDoltConfig()           // ready for dolt.New(ctx, cfg)
package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/doltserver"
	"github.com/steveyegge/beads/internal/git"
	"github.com/steveyegge/beads/internal/utils"
)

// WorkspaceContext is the single source of truth for the current beads
// workspace. It is resolved once (or per-workspace for multi-workspace
// scenarios) and passed to all components that need workspace information.
type WorkspaceContext struct {
	// --- Filesystem paths ---

	// BeadsDir is the resolved .beads directory (after following redirects).
	BeadsDir string

	// DatabasePath is the resolved path to the dolt data directory.
	// For server mode with shared server, this may point to
	// ~/.beads/shared-server/dolt/. For embedded mode, this is typically
	// .beads/dolt/ or .beads/embeddeddolt/.
	DatabasePath string

	// RepoRoot is the git repository root containing BeadsDir.
	// Git commands for beads operations should run here.
	RepoRoot string

	// CWDRepoRoot is the git repo root for the user's working directory.
	// Differs from RepoRoot when BEADS_DIR points elsewhere.
	CWDRepoRoot string

	// --- Redirect state ---

	// IsRedirected is true when BeadsDir was resolved via redirect or
	// external BEADS_DIR pointing to a different repository.
	IsRedirected bool

	// RedirectSource captures the pre-redirect .beads dir and its database
	// identity, when a redirect was followed. Zero value when no redirect.
	RedirectSource RedirectSource

	// --- Git/worktree state ---

	// IsWorktree is true when CWD is in a git worktree.
	IsWorktree bool

	// MainRepoRoot is the main repository root (differs from RepoRoot only
	// in worktrees where the main repo is the parent). Empty when not in a
	// worktree.
	MainRepoRoot string

	// WorktreeResolution describes how BeadsDir was found in a worktree
	// context. Useful for diagnostics and server mode decisions.
	WorktreeResolution WorktreeResolution

	// --- Server configuration ---

	// ServerMode is the resolved server lifecycle mode.
	ServerMode ServerMode

	// ServerConfig holds resolved connection parameters.
	ServerConfig ServerConfig

	// --- Project identity ---

	// ProjectID is the unique project identifier from metadata.json.
	ProjectID string

	// Database is the SQL database name (e.g., "beads", "beads_hq").
	Database string
}

// RedirectSource captures the pre-redirect .beads dir and its database identity.
type RedirectSource struct {
	// Dir is the original .beads directory before redirect was followed.
	Dir string
	// Database is the dolt_database field from the source metadata.json.
	// Empty if no source metadata exists or has no dolt_database configured.
	Database string
}

// WorktreeResolution describes how the BeadsDir was discovered relative to
// the current worktree. This is critical for server mode decisions: when
// using the shared fallback, port files and server state live in the main
// repo's .beads/, not the worktree's.
type WorktreeResolution int

const (
	// WorktreeNotApplicable means the workspace is not in a git worktree.
	WorktreeNotApplicable WorktreeResolution = iota

	// WorktreeOwnDatabase means the worktree has its own .beads/ with a
	// real database (dolt/, embeddeddolt/, or *.db). This worktree manages
	// its own server independently.
	WorktreeOwnDatabase

	// WorktreeRedirect means the worktree's .beads/redirect was followed
	// to find the effective BeadsDir.
	WorktreeRedirect

	// WorktreeFallbackShared means the BeadsDir was resolved via the main
	// repo's .beads/ (through git-common-dir). Port files and server state
	// live in that shared location.
	WorktreeFallbackShared
)

// String returns a human-readable label for the worktree resolution.
func (wr WorktreeResolution) String() string {
	switch wr {
	case WorktreeNotApplicable:
		return "not-applicable"
	case WorktreeOwnDatabase:
		return "own-database"
	case WorktreeRedirect:
		return "redirect"
	case WorktreeFallbackShared:
		return "fallback-shared"
	default:
		return fmt.Sprintf("WorktreeResolution(%d)", int(wr))
	}
}

// ServerMode describes who owns and manages the dolt sql-server lifecycle.
type ServerMode = doltserver.ServerMode

// Re-export ServerMode constants for callers that import workspace.
const (
	ServerModeOwned    = doltserver.ServerModeOwned
	ServerModeExternal = doltserver.ServerModeExternal
	ServerModeEmbedded = doltserver.ServerModeEmbedded
)

// ServerConfig holds the resolved dolt server connection parameters.
type ServerConfig struct {
	Host     string
	Port     int
	Socket   string
	User     string
	Password string // #nosec G117 - not a hardcoded credential, populated from env var at runtime
	TLS      bool

	// PortSource describes where the port came from. Useful for diagnosing
	// worktree server issues where the wrong port file is read.
	PortSource PortSource

	// AutoStart is the resolved auto-start policy.
	AutoStart bool
}

// PortSource describes the origin of the resolved server port.
type PortSource int

const (
	// PortSourceNone means no port was configured (ephemeral will be used).
	PortSourceNone PortSource = iota
	// PortSourceEnvVar means BEADS_DOLT_SERVER_PORT env var was set.
	PortSourceEnvVar
	// PortSourcePortFile means .beads/dolt-server.port was read.
	PortSourcePortFile
	// PortSourceConfigYAML means dolt.port from config.yaml was used.
	PortSourceConfigYAML
	// PortSourceMetadataJSON means dolt_server_port from metadata.json was used (deprecated).
	PortSourceMetadataJSON
	// PortSourceSharedDefault means the shared-server default port (3308) was used.
	PortSourceSharedDefault
)

// String returns a human-readable label for the port source.
func (ps PortSource) String() string {
	switch ps {
	case PortSourceNone:
		return "none (ephemeral)"
	case PortSourceEnvVar:
		return "BEADS_DOLT_SERVER_PORT env var"
	case PortSourcePortFile:
		return "dolt-server.port file"
	case PortSourceConfigYAML:
		return "config.yaml dolt.port"
	case PortSourceMetadataJSON:
		return "metadata.json dolt_server_port (deprecated)"
	case PortSourceSharedDefault:
		return "shared-server default (3308)"
	default:
		return fmt.Sprintf("PortSource(%d)", int(ps))
	}
}

// UserRole represents the user's relationship to a repository.
type UserRole string

const (
	// Contributor indicates the user is contributing to a fork.
	Contributor UserRole = "contributor"
	// Maintainer indicates the user owns/maintains the repository.
	Maintainer UserRole = "maintainer"
)

// ErrNoWorkspace is returned when no .beads directory can be found.
var ErrNoWorkspace = errors.New("no .beads directory found")

// --- Convenience methods ---

// PortFilePath returns the path to the dolt-server.port file for this
// workspace. When using a shared .beads via worktree fallback, this
// correctly points to the shared location rather than a worktree-local
// path that doesn't have a port file.
func (ws *WorkspaceContext) PortFilePath() string {
	return filepath.Join(ws.BeadsDir, doltserver.PortFileName)
}

// PIDFilePath returns the path to the dolt-server.pid file for this workspace.
func (ws *WorkspaceContext) PIDFilePath() string {
	return filepath.Join(ws.BeadsDir, doltserver.PIDFileName)
}

// GitCmd creates an exec.Cmd configured to run git in the beads repository.
// Sets cmd.Dir to RepoRoot and explicitly sets GIT_DIR and GIT_WORK_TREE
// to ensure git operates on the correct repository regardless of CWD.
// Git hooks and templates are disabled for security (SEC-001, SEC-002).
func (ws *WorkspaceContext) GitCmd(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = ws.RepoRoot
	gitDir := filepath.Join(ws.RepoRoot, ".git")
	cmd.Env = append(os.Environ(),
		"GIT_HOOKS_PATH=",
		"GIT_TEMPLATE_DIR=",
		"GIT_DIR="+gitDir,
		"GIT_WORK_TREE="+ws.RepoRoot,
	)
	return cmd
}

// GitCmdCWD creates an exec.Cmd configured to run git in the user's working
// repository. Use this for git commands that should reflect the user's
// current context rather than the beads repo.
func (ws *WorkspaceContext) GitCmdCWD(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", args...)
	if ws.CWDRepoRoot != "" {
		cmd.Dir = ws.CWDRepoRoot
	}
	return cmd
}

// GitOutput runs a git command in the beads repository and returns stdout.
func (ws *WorkspaceContext) GitOutput(ctx context.Context, args ...string) (string, error) {
	cmd := ws.GitCmd(ctx, args...)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

// RelPath returns the given absolute path relative to the beads repository root.
func (ws *WorkspaceContext) RelPath(absPath string) (string, error) {
	return filepath.Rel(ws.RepoRoot, absPath)
}

// Validate checks if the workspace context is still valid (paths exist).
// Useful for long-running processes that need to detect stale context.
func (ws *WorkspaceContext) Validate() error {
	if _, err := os.Stat(ws.BeadsDir); os.IsNotExist(err) {
		return fmt.Errorf("BeadsDir no longer exists: %s", ws.BeadsDir)
	}
	if _, err := os.Stat(ws.RepoRoot); os.IsNotExist(err) {
		return fmt.Errorf("RepoRoot no longer exists: %s", ws.RepoRoot)
	}
	return nil
}

// Role reads beads.role from git config.
// If IsRedirected, returns Contributor implicitly.
func (ws *WorkspaceContext) Role() (UserRole, bool) {
	if ws.IsRedirected {
		return Contributor, true
	}
	output, err := ws.GitOutput(context.Background(), "config", "--get", "beads.role")
	if err != nil {
		return "", false
	}
	return UserRole(strings.TrimSpace(output)), true
}

// IsContributor returns true if user is configured as contributor.
func (ws *WorkspaceContext) IsContributor() bool {
	role, ok := ws.Role()
	return ok && role == Contributor
}

// IsMaintainer returns true if user is configured as maintainer.
func (ws *WorkspaceContext) IsMaintainer() bool {
	role, ok := ws.Role()
	return ok && role == Maintainer
}

// --- Internal helpers ---

// isExternalBeadsDir returns true if beadsDir is in a different git repo than CWD.
func isExternalBeadsDir(beadsDir string) (bool, error) {
	cwdCommonDir, err := git.GetGitCommonDir()
	if err != nil {
		return false, err
	}
	beadsCommonDir, err := getGitCommonDirForPath(beadsDir)
	if err != nil {
		return false, err
	}
	return cwdCommonDir != beadsCommonDir, nil
}

// getGitCommonDirForPath returns the shared git directory for a path.
func getGitCommonDirForPath(path string) (string, error) {
	cmd := exec.Command("git", "-C", path, "rev-parse", "--git-common-dir")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get git common dir for %s: %w", path, err)
	}
	result := strings.TrimSpace(string(output))
	if !filepath.IsAbs(result) {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("failed to get absolute path for %s: %w", path, err)
		}
		result = filepath.Join(absPath, result)
	}
	result = filepath.Clean(result)
	if resolved, err := filepath.EvalSymlinks(result); err == nil {
		result = resolved
	}
	return result, nil
}

// repoRootForBeadsDir returns the repository root for a beads directory.
func repoRootForBeadsDir(beadsDir string) string {
	cmd := exec.Command("git", "-C", beadsDir, "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err == nil {
		root := strings.TrimSpace(string(output))
		if root != "" {
			return root
		}
	}
	return filepath.Dir(beadsDir)
}

// unsafePrefixes lists system directories that BEADS_DIR should never point to.
var unsafePrefixes = []string{
	"/etc", "/usr", "/var", "/root", "/System", "/Library",
	"/bin", "/sbin", "/opt", "/private",
}

// isPathInSafeBoundary validates that a path is not in sensitive system directories.
func isPathInSafeBoundary(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	// Allow OS temp dirs
	tempDir := os.TempDir()
	resolvedTemp, _ := filepath.EvalSymlinks(tempDir)
	resolvedPath, _ := filepath.EvalSymlinks(absPath)
	if resolvedTemp != "" && strings.HasPrefix(resolvedPath, resolvedTemp) {
		return true
	}
	if strings.HasPrefix(absPath, tempDir) {
		return true
	}

	// Allow /var/home (Fedora Silverblue, Bluefin, etc.)
	if strings.HasPrefix(absPath, "/var/home/") {
		return true
	}

	for _, prefix := range unsafePrefixes {
		if strings.HasPrefix(absPath, prefix+"/") || absPath == prefix {
			return false
		}
	}

	homeDir, _ := os.UserHomeDir()
	if strings.HasPrefix(absPath, "/Users/") || strings.HasPrefix(absPath, "/home/") || strings.HasPrefix(absPath, "/var/home/") {
		if homeDir != "" && !strings.HasPrefix(absPath, homeDir) {
			return false
		}
	}
	return true
}

// gitOutputFromDir runs a git command in a specific directory and returns stdout.
func gitOutputFromDir(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...) //nolint:gosec
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

// worktreeFallbackBeadsDirForRepo returns the shared .beads/ for a repo path
// by examining git-common-dir. Returns "" if not in a worktree or no shared
// .beads exists.
func worktreeFallbackBeadsDirForRepo(repoPath string) string {
	cmd := exec.Command("git", "-C", repoPath, "rev-parse", "--git-dir", "--git-common-dir")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) < 2 {
		return ""
	}
	gitDir := gitPathForRepo(repoPath, strings.TrimSpace(lines[0]))
	commonDir := gitPathForRepo(repoPath, strings.TrimSpace(lines[1]))
	if gitDir == "" || commonDir == "" || utils.PathsEqual(gitDir, commonDir) {
		return ""
	}
	if filepath.Base(commonDir) == ".git" {
		return filepath.Join(filepath.Dir(commonDir), ".beads")
	}
	return filepath.Join(commonDir, ".beads")
}

func gitPathForRepo(repoPath, path string) string {
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(repoPath, path)
	}
	return utils.CanonicalizePath(path)
}

// resolveServerConfig builds a ServerConfig from the resolved beadsDir and
// configfile.Config. This consolidates the logic currently split between
// doltserver.DefaultConfig() and cmd/bd/main.go.
func resolveServerConfig(beadsDir string, fileCfg *configfile.Config, _ ServerMode) ServerConfig {
	sc := ServerConfig{
		Host:   fileCfg.GetDoltServerHost(),
		Socket: fileCfg.GetDoltServerSocket(),
		User:   fileCfg.GetDoltServerUser(),
		TLS:    fileCfg.GetDoltServerTLS(),
	}

	// Port resolution with source tracking
	sc.Port, sc.PortSource = resolvePort(beadsDir, fileCfg)

	// Password lookup uses the resolved port for credential file matching
	// (metadata.json port and runtime port can diverge, e.g. tunnel on 3308
	// vs local on 3307).
	sc.Password = fileCfg.GetDoltServerPasswordForPort(sc.Port)

	return sc
}

// resolvePort determines the server port and tracks its source.
// Priority: env var > port file > config.yaml > metadata.json > shared default.
func resolvePort(beadsDir string, fileCfg *configfile.Config) (int, PortSource) {
	// For shared server mode, read port file from the shared server dir
	effectiveDir := beadsDir
	if doltserver.IsSharedServerMode() {
		if sharedDir, err := doltserver.SharedServerDir(); err == nil {
			effectiveDir = sharedDir
		}
	}

	// 1. Env var (highest priority)
	if p := os.Getenv("BEADS_DOLT_SERVER_PORT"); p != "" {
		var port int
		if _, err := fmt.Sscanf(p, "%d", &port); err == nil && port > 0 {
			return port, PortSourceEnvVar
		}
	}

	// 2. Port file (.beads/dolt-server.port or shared-server dir)
	if p := doltserver.ReadPortFile(effectiveDir); p > 0 {
		return p, PortSourcePortFile
	}

	// 3. config.yaml dolt.port
	if p := configYAMLPort(); p > 0 {
		return p, PortSourceConfigYAML
	}

	// 4. metadata.json dolt_server_port (deprecated)
	if fileCfg != nil && fileCfg.DoltServerPort > 0 {
		return fileCfg.DoltServerPort, PortSourceMetadataJSON
	}

	// 5. Shared server default
	if doltserver.IsSharedServerMode() {
		return doltserver.DefaultSharedServerPort, PortSourceSharedDefault
	}

	return 0, PortSourceNone
}

// configYAMLPort reads dolt.port from config.yaml. Returns 0 if not set.
func configYAMLPort() int {
	p := config.GetYamlConfig("dolt.port")
	if p == "" {
		return 0
	}
	var port int
	if _, err := fmt.Sscanf(p, "%d", &port); err == nil && port > 0 {
		return port
	}
	return 0
}
