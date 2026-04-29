package workspace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/doltserver"
	"github.com/steveyegge/beads/internal/git"
	"github.com/steveyegge/beads/internal/utils"
)

// Resolve computes a WorkspaceContext from the current environment (CWD, env
// vars, config files). This is the primary entry point and replaces the
// scattered calls to FindBeadsDir, FindDatabasePath, GetRepoContext,
// configfile.Load, ResolveServerMode, etc.
//
// The resolution is deterministic and side-effect free (no CWD changes,
// no server starts).
func Resolve() (*WorkspaceContext, error) {
	// 1. Find .beads directory using the same resolution order as FindBeadsDir
	beadsDir, wtResolution, err := resolveBeadsDir()
	if err != nil {
		return nil, err
	}
	if beadsDir == "" {
		return nil, ErrNoWorkspace
	}

	// 2. Security check
	if !isPathInSafeBoundary(beadsDir) {
		return nil, fmt.Errorf("beads directory in unsafe location: %s", beadsDir)
	}

	// 3. Build the full context
	return buildContext(beadsDir, wtResolution)
}

// ResolveFrom computes a WorkspaceContext for a specific directory, without
// using CWD or BEADS_DIR. Uses git -C flags instead of changing CWD.
func ResolveFrom(dir string) (*WorkspaceContext, error) {
	if dir == "" {
		return nil, fmt.Errorf("empty directory path")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve path %s: %w", dir, err)
	}

	info, err := os.Stat(absDir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("cannot access directory %s: %w", absDir, err)
	}

	absDir = utils.CanonicalizePath(absDir)

	// Find repo root from the target directory
	repoRoot, _ := gitOutputFromDir(absDir, "rev-parse", "--show-toplevel")
	if repoRoot == "" {
		return nil, fmt.Errorf("directory %s is not in a git repository", absDir)
	}
	repoRoot = utils.CanonicalizePath(repoRoot)

	// Check for worktree
	gitDir, _ := gitOutputFromDir(absDir, "rev-parse", "--git-dir")
	commonDir, _ := gitOutputFromDir(absDir, "rev-parse", "--git-common-dir")

	isWt := false
	var mainRepoRoot string
	if gitDir != "" && commonDir != "" {
		absGitDir := gitPathForRepo(absDir, gitDir)
		absCommon := gitPathForRepo(absDir, commonDir)
		isWt = !utils.PathsEqual(absGitDir, absCommon)
		if isWt {
			if filepath.Base(absCommon) == ".git" {
				mainRepoRoot = filepath.Dir(absCommon)
			}
		}
	}

	// Find .beads in repo root
	beadsDir := filepath.Join(repoRoot, ".beads")
	wtResolution := WorktreeNotApplicable

	if isWt {
		// Check worktree-specific .beads
		beadsDir, wtResolution = resolveWorktreeBeadsDir(repoRoot)
	}

	if info, err := os.Stat(beadsDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("no .beads directory found at %s", beadsDir)
	}

	// Follow redirect
	resolved := followRedirect(beadsDir)
	if resolved != beadsDir && wtResolution == WorktreeNotApplicable {
		// Non-worktree redirect
	}
	beadsDir = resolved

	if !isPathInSafeBoundary(beadsDir) {
		return nil, fmt.Errorf("beads directory in unsafe location: %s", beadsDir)
	}

	if !hasBeadsProjectFiles(beadsDir) {
		return nil, fmt.Errorf("beads directory missing required files: %s", beadsDir)
	}

	ws, err := buildContext(beadsDir, wtResolution)
	if err != nil {
		return nil, err
	}

	// Override CWDRepoRoot with the target dir's repo
	ws.CWDRepoRoot = repoRoot
	if isWt {
		ws.IsWorktree = true
		ws.MainRepoRoot = mainRepoRoot
	}

	return ws, nil
}

// ResolveForBeadsDir computes a WorkspaceContext given a known .beads directory.
// Used when the caller already knows the beads dir (e.g., from --db flag).
func ResolveForBeadsDir(beadsDir string) (*WorkspaceContext, error) {
	if beadsDir == "" {
		return nil, fmt.Errorf("empty beads directory path")
	}

	beadsDir = utils.CanonicalizePath(beadsDir)

	if info, err := os.Stat(beadsDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("beads directory does not exist: %s", beadsDir)
	}

	if !isPathInSafeBoundary(beadsDir) {
		return nil, fmt.Errorf("beads directory in unsafe location: %s", beadsDir)
	}

	// Follow redirect if present
	beadsDir = followRedirect(beadsDir)

	return buildContext(beadsDir, WorktreeNotApplicable)
}

// buildContext assembles a full WorkspaceContext from a resolved beadsDir.
func buildContext(beadsDir string, wtResolution WorktreeResolution) (*WorkspaceContext, error) {
	ws := &WorkspaceContext{
		BeadsDir:           beadsDir,
		WorktreeResolution: wtResolution,
	}

	// Resolve redirect info
	ws.resolveRedirectInfo(beadsDir)

	// Determine repo root
	ws.RepoRoot = repoRootForBeadsDir(beadsDir)
	ws.CWDRepoRoot = git.GetRepoRoot()
	ws.IsWorktree = git.IsWorktree()

	if ws.IsWorktree {
		mainRoot, err := git.GetMainRepoRoot()
		if err == nil {
			ws.MainRepoRoot = mainRoot
		}
	}

	// Determine if external (different repo than CWD)
	if !ws.IsRedirected {
		if external, err := isExternalBeadsDir(beadsDir); err == nil {
			ws.IsRedirected = external
		}
	}

	// Load metadata.json
	fileCfg, _ := loadConfigSafe(beadsDir)
	if fileCfg == nil {
		fileCfg = configfile.DefaultConfig()
	}

	// Apply central config defaults for server fields
	applyCentralConfigDefaults(fileCfg)

	// Resolve database path
	ws.DatabasePath = resolveDatabasePath(beadsDir, fileCfg)

	// Project identity
	ws.ProjectID = fileCfg.ProjectID
	ws.Database = fileCfg.GetDoltDatabase()

	// If redirect preserved a source database, use that
	if ws.RedirectSource.Database != "" && os.Getenv("BEADS_DOLT_SERVER_DATABASE") == "" {
		ws.Database = ws.RedirectSource.Database
	}

	// Server mode
	ws.ServerMode = doltserver.ResolveServerMode(beadsDir)

	// Server config
	ws.ServerConfig = resolveServerConfig(beadsDir, fileCfg, ws.ServerMode)

	return ws, nil
}

// resolveRedirectInfo checks if this beadsDir was arrived at via redirect.
func (ws *WorkspaceContext) resolveRedirectInfo(beadsDir string) {
	// Check if the local repo has a redirect
	repoRoot := git.GetRepoRoot()
	if repoRoot == "" {
		return
	}

	localBeadsDir := filepath.Join(repoRoot, ".beads")
	if localBeadsDir == beadsDir {
		return // same dir, no redirect
	}

	// Check if local .beads has a redirect file
	redirectFile := filepath.Join(localBeadsDir, "redirect")
	if _, err := os.Stat(redirectFile); err != nil {
		return
	}

	// Read source database from the local metadata.json before redirect
	ws.IsRedirected = true
	ws.RedirectSource.Dir = localBeadsDir

	metadataPath := filepath.Join(localBeadsDir, "metadata.json")
	if data, err := os.ReadFile(metadataPath); err == nil { // #nosec G304 - controlled path from validated beads dir
		var raw struct {
			DoltDatabase string `json:"dolt_database"`
		}
		if json.Unmarshal(data, &raw) == nil {
			ws.RedirectSource.Database = raw.DoltDatabase
		}
	}
}

// resolveBeadsDir finds the .beads directory using the standard resolution order.
// Returns the resolved beadsDir and how it was found (worktree resolution).
func resolveBeadsDir() (string, WorktreeResolution, error) {
	// 1. BEADS_DIR env var (highest priority)
	if beadsDir := os.Getenv("BEADS_DIR"); beadsDir != "" {
		absBeadsDir := utils.CanonicalizePath(beadsDir)
		absBeadsDir = followRedirect(absBeadsDir)
		if info, err := os.Stat(absBeadsDir); err == nil && info.IsDir() {
			if hasBeadsProjectFiles(absBeadsDir) {
				return absBeadsDir, WorktreeNotApplicable, nil
			}
		}
	}

	// 2. Walk up from CWD toward the repo root boundary
	cwd, err := os.Getwd()
	if err != nil {
		return "", WorktreeNotApplicable, fmt.Errorf("cannot get working directory: %w", err)
	}

	gitRoot := git.GetRepoRoot()
	isWt := git.IsWorktree()
	walkBoundary := gitRoot
	if isWt {
		walkBoundary = git.GetRepoRoot()
	}

	cwdCanonical := utils.CanonicalizePath(cwd)
	walkBoundaryCanonical := ""
	if walkBoundary != "" {
		walkBoundaryCanonical = utils.CanonicalizePath(walkBoundary)
	}

	for dir := cwdCanonical; dir != "/" && dir != "."; {
		if walkBoundaryCanonical != "" && dir == walkBoundaryCanonical {
			break
		}

		beadsDir := filepath.Join(dir, ".beads")
		if info, err := os.Stat(beadsDir); err == nil && info.IsDir() {
			beadsDir = followRedirect(beadsDir)
			if hasBeadsProjectFiles(beadsDir) {
				return beadsDir, WorktreeNotApplicable, nil
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	// 3. Worktree-specific fallback
	var mainRepoRoot string
	if isWt {
		worktreeRoot := git.GetRepoRoot()
		if worktreeRoot != "" {
			mainRoot, err := git.GetMainRepoRoot()
			if err == nil {
				mainRepoRoot = mainRoot
			}

			beadsDir, wtRes := resolveWorktreeBeadsDir(worktreeRoot)
			if beadsDir != "" {
				if info, err := os.Stat(beadsDir); err == nil && info.IsDir() {
					return beadsDir, wtRes, nil
				}
			}
		}
	}

	// 4. Extended walk from walk boundary to git/main-repo root
	if walkBoundary != "" {
		extendedRoot := gitRoot
		if isWt && mainRepoRoot != "" {
			extendedRoot = mainRepoRoot
		}
		extendedRootCanonical := ""
		if extendedRoot != "" {
			extendedRootCanonical = utils.CanonicalizePath(extendedRoot)
		}

		for dir := walkBoundaryCanonical; dir != "/" && dir != "."; {
			beadsDir := filepath.Join(dir, ".beads")
			if info, err := os.Stat(beadsDir); err == nil && info.IsDir() {
				beadsDir = followRedirect(beadsDir)
				if hasBeadsProjectFiles(beadsDir) {
					return beadsDir, WorktreeNotApplicable, nil
				}
			}

			if extendedRootCanonical != "" && dir == extendedRootCanonical {
				break
			}

			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	return "", WorktreeNotApplicable, nil
}

// resolveWorktreeBeadsDir handles the 3-step worktree fallback:
// 3a: per-worktree redirect override
// 3b: worktree's own .beads (separate-DB mode)
// 3c: shared .beads via git-common-dir
func resolveWorktreeBeadsDir(worktreeRoot string) (string, WorktreeResolution) {
	// 3a. Per-worktree redirect override
	worktreeBeadsDir := filepath.Join(worktreeRoot, ".beads")
	redirectFile := filepath.Join(worktreeBeadsDir, "redirect")
	if _, err := os.Stat(redirectFile); err == nil {
		target := followRedirect(worktreeBeadsDir)
		if target != worktreeBeadsDir {
			if info, err := os.Stat(target); err == nil && info.IsDir() {
				if hasBeadsProjectFiles(target) {
					return target, WorktreeRedirect
				}
			}
		}
	}

	// 3b. Worktree's own .beads (separate-DB mode)
	if info, err := os.Stat(worktreeBeadsDir); err == nil && info.IsDir() {
		if hasBeadsDatabase(worktreeBeadsDir) {
			return worktreeBeadsDir, WorktreeOwnDatabase
		}

		// Lenient acceptance only when no shared fallback has a real database
		fallback := getWorktreeFallbackBeadsDir()
		fallbackHasDB := false
		if fallback != "" {
			if fbInfo, err := os.Stat(fallback); err == nil && fbInfo.IsDir() {
				resolved := followRedirect(fallback)
				fallbackHasDB = hasBeadsDatabase(resolved)
			}
		}
		if !fallbackHasDB && hasBeadsProjectFiles(worktreeBeadsDir) {
			return worktreeBeadsDir, WorktreeOwnDatabase
		}
	}

	// 3c. Shared .beads via git-common-dir
	if fallback := getWorktreeFallbackBeadsDir(); fallback != "" {
		if info, err := os.Stat(fallback); err == nil && info.IsDir() {
			fallback = followRedirect(fallback)
			if hasBeadsProjectFiles(fallback) {
				return fallback, WorktreeFallbackShared
			}
		}
	}

	return "", WorktreeNotApplicable
}

// getWorktreeFallbackBeadsDir returns the shared .beads location via git-common-dir.
func getWorktreeFallbackBeadsDir() string {
	if !git.IsWorktree() {
		return ""
	}
	commonDir, err := git.GetGitCommonDir()
	if err != nil || commonDir == "" {
		return ""
	}
	commonDir = utils.CanonicalizePath(commonDir)
	if filepath.Base(commonDir) == ".git" {
		return filepath.Join(filepath.Dir(commonDir), ".beads")
	}
	return filepath.Join(commonDir, ".beads")
}

// followRedirect checks for a redirect file and follows it (one level only).
func followRedirect(beadsDir string) string {
	redirectFile := filepath.Join(beadsDir, "redirect")
	data, err := os.ReadFile(redirectFile) // #nosec G304 - controlled path from validated beads dir
	if err != nil {
		return beadsDir
	}

	// Parse: skip comments and empty lines
	var target string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			target = line
			break
		}
	}
	if target == "" {
		return beadsDir
	}

	// Resolve relative paths from the parent of .beads (project root)
	if !filepath.IsAbs(target) {
		projectRoot := filepath.Dir(beadsDir)
		target = filepath.Join(projectRoot, target)
	}

	// Canonicalize and prefer stable branch worktree
	target = canonicalizeBeadsDirPath(target)

	// Verify target exists
	info, err := os.Stat(target)
	if err != nil || !info.IsDir() {
		return beadsDir
	}

	// Prevent redirect chains
	targetRedirect := filepath.Join(target, "redirect")
	if _, err := os.Stat(targetRedirect); err == nil {
		fmt.Fprintf(os.Stderr, "Warning: redirect chains not allowed, ignoring redirect in %s\n", target)
	}

	return target
}

// canonicalizeBeadsDirPath resolves symlinks and prefers stable worktree paths.
func canonicalizeBeadsDirPath(beadsDir string) string {
	canonical := utils.CanonicalizePath(beadsDir)
	if stable := preferStableBranchWorktreeBeadsDir(canonical); stable != "" {
		return stable
	}
	return canonical
}

type worktreeEntry struct {
	Path     string
	Head     string
	Branch   string
	Detached bool
	Bare     bool
}

func preferStableBranchWorktreeBeadsDir(beadsDir string) string {
	if filepath.Base(beadsDir) != ".beads" {
		return ""
	}

	repoRoot := filepath.Dir(beadsDir)
	if !strings.Contains(filepath.ToSlash(repoRoot), "/refs/commits/") {
		return ""
	}

	branch, err := gitOutputFromDir(repoRoot, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil || branch != "HEAD" {
		return ""
	}

	head, err := gitOutputFromDir(repoRoot, "rev-parse", "HEAD")
	if err != nil || head == "" {
		return ""
	}

	output, err := gitOutputFromDir(repoRoot, "worktree", "list", "--porcelain")
	if err != nil {
		return ""
	}

	var worktrees []worktreeEntry
	var current *worktreeEntry
	for _, line := range strings.Split(output, "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			if current != nil {
				worktrees = append(worktrees, *current)
			}
			current = &worktreeEntry{Path: strings.TrimPrefix(line, "worktree ")}
		case current == nil:
			continue
		case strings.HasPrefix(line, "HEAD "):
			current.Head = strings.TrimPrefix(line, "HEAD ")
		case strings.HasPrefix(line, "branch refs/heads/"):
			current.Branch = strings.TrimPrefix(line, "branch refs/heads/")
		case line == "detached":
			current.Detached = true
		case line == "bare":
			current.Bare = true
		}
	}
	if current != nil {
		worktrees = append(worktrees, *current)
	}

	var candidates []worktreeEntry
	for _, wt := range worktrees {
		if wt.Bare || wt.Detached || wt.Branch == "" {
			continue
		}
		if wt.Head != head || utils.PathsEqual(wt.Path, repoRoot) {
			continue
		}
		candidates = append(candidates, wt)
	}

	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		iStable := !strings.Contains(filepath.ToSlash(candidates[i].Path), "/refs/commits/")
		jStable := !strings.Contains(filepath.ToSlash(candidates[j].Path), "/refs/commits/")
		if iStable != jStable {
			return iStable
		}
		return candidates[i].Path < candidates[j].Path
	})

	stableBeadsDir := filepath.Join(candidates[0].Path, ".beads")
	if info, err := os.Stat(stableBeadsDir); err == nil && info.IsDir() {
		return utils.CanonicalizePath(stableBeadsDir)
	}

	return ""
}

// hasBeadsProjectFiles checks if a .beads directory contains project files.
func hasBeadsProjectFiles(beadsDir string) bool {
	if _, err := os.Stat(filepath.Join(beadsDir, "metadata.json")); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(beadsDir, "config.yaml")); err == nil {
		return true
	}
	if info, err := os.Stat(filepath.Join(beadsDir, "dolt")); err == nil && info.IsDir() {
		return true
	}
	if info, err := os.Stat(filepath.Join(beadsDir, "embeddeddolt")); err == nil && info.IsDir() {
		return true
	}
	dbMatches, _ := filepath.Glob(filepath.Join(beadsDir, "*.db"))
	for _, match := range dbMatches {
		baseName := filepath.Base(match)
		if !strings.Contains(baseName, ".backup") && baseName != "vc.db" {
			return true
		}
	}
	return false
}

// hasBeadsDatabase is the strict counterpart: returns true only when the
// directory contains an actual database (dolt/, embeddeddolt/, or *.db).
func hasBeadsDatabase(beadsDir string) bool {
	if info, err := os.Stat(filepath.Join(beadsDir, "dolt")); err == nil && info.IsDir() {
		return true
	}
	if info, err := os.Stat(filepath.Join(beadsDir, "embeddeddolt")); err == nil && info.IsDir() {
		return true
	}
	dbMatches, _ := filepath.Glob(filepath.Join(beadsDir, "*.db"))
	for _, match := range dbMatches {
		baseName := filepath.Base(match)
		if !strings.Contains(baseName, ".backup") && baseName != "vc.db" {
			return true
		}
	}
	return false
}

// resolveDatabasePath determines the database path from .beads dir and config.
func resolveDatabasePath(beadsDir string, fileCfg *configfile.Config) string {
	// BEADS_DB env var (deprecated but supported)
	if envDB := os.Getenv("BEADS_DB"); envDB != "" {
		absDB := utils.CanonicalizePath(envDB)
		if info, err := os.Stat(absDB); err == nil && info.IsDir() {
			return absDB
		}
		return absDB
	}

	// Shared server mode uses centralized dolt data directory
	if doltserver.IsSharedServerMode() {
		if dir, err := doltserver.SharedDoltDir(); err == nil {
			return dir
		}
	}

	// Custom BEADS_DOLT_DATA_DIR env var
	if d := os.Getenv("BEADS_DOLT_DATA_DIR"); d != "" {
		if filepath.IsAbs(d) {
			return d
		}
		return filepath.Join(beadsDir, d)
	}

	// Server mode: path from config (may not exist locally)
	if fileCfg.IsDoltServerMode() {
		return fileCfg.DatabasePath(beadsDir)
	}

	// Check for embeddeddolt first, then dolt
	embeddedPath := filepath.Join(beadsDir, "embeddeddolt")
	if info, err := os.Stat(embeddedPath); err == nil && info.IsDir() {
		return embeddedPath
	}

	doltPath := fileCfg.DatabasePath(beadsDir)
	if info, err := os.Stat(doltPath); err == nil && info.IsDir() {
		return doltPath
	}

	// Fall back to default
	return fileCfg.DatabasePath(beadsDir)
}

// loadConfigSafe loads metadata.json without triggering migration side effects.
func loadConfigSafe(beadsDir string) (*configfile.Config, error) {
	metadataPath := configfile.ConfigPath(beadsDir)
	if _, err := os.Stat(metadataPath); err != nil {
		return nil, err
	}
	return configfile.Load(beadsDir)
}

// applyCentralConfigDefaults fills in server fields from config.yaml when
// the per-project metadata.json doesn't specify them.
func applyCentralConfigDefaults(fileCfg *configfile.Config) {
	if fileCfg.DoltServerHost == "" {
		if h := config.GetYamlConfig("dolt.host"); h != "" {
			fileCfg.DoltServerHost = h
		}
	}
	if fileCfg.DoltServerUser == "" {
		if u := config.GetYamlConfig("dolt.user"); u != "" {
			fileCfg.DoltServerUser = u
		}
	}
}
