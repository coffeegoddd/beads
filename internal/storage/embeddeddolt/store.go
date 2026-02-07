// Package embeddeddolt is a placeholder storage backend for a future embedded Dolt implementation.
//
// For now, it satisfies the storage.Storage interface but returns "unimplemented" errors
// for all operations. This allows plumbing and CLI selection to land before the
// real embedded implementation is built.
package embeddeddolt

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/idgen"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// ErrUnimplemented is returned by all EmbeddedDoltStore methods for now.
var ErrUnimplemented = errors.New("embedded-dolt backend is not implemented")

func unimplemented(method string) error {
	return fmt.Errorf("%w: %s", ErrUnimplemented, method)
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// DoltCommit creates a Dolt commit in the embedded database.
// This is an embedded-dolt-only helper (not part of storage.Storage).
//
// Best-effort callers may choose to ignore "nothing to commit" errors.
func (s *EmbeddedDoltStore) DoltCommit(ctx context.Context, message string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		msg = "bd: create"
	}
	_, err := s.exec.ExecContext(ctx, "beads", "CALL DOLT_COMMIT('-Am', ?)", msg)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
			return nil
		}
		return fmt.Errorf("failed to dolt_commit: %w", err)
	}
	return nil
}

// =============================================================================
// ID generation (embedded-dolt)
// =============================================================================

type adaptiveIDConfig struct {
	maxCollisionProb float64
	minLen           int
	maxLen           int
}

func defaultAdaptiveIDConfig() adaptiveIDConfig {
	return adaptiveIDConfig{
		maxCollisionProb: 0.25,
		minLen:           3,
		maxLen:           8,
	}
}

func collisionProb(numIssues int, idLength int) float64 {
	// P(collision) ≈ 1 - e^(-n²/2N), N = 36^len (base36)
	const base = 36.0
	N := math.Pow(base, float64(idLength))
	exponent := -float64(numIssues*numIssues) / (2.0 * N)
	return 1.0 - math.Exp(exponent)
}

func computeAdaptiveLen(numIssues int, cfg adaptiveIDConfig) int {
	for l := cfg.minLen; l <= cfg.maxLen; l++ {
		if collisionProb(numIssues, l) <= cfg.maxCollisionProb {
			return l
		}
	}
	return cfg.maxLen
}

func readAdaptiveConfig(ctx context.Context, db *sql.DB) adaptiveIDConfig {
	cfg := defaultAdaptiveIDConfig()

	// These keys mirror sqlite's adaptive ID config keys.
	var s string
	if err := db.QueryRowContext(ctx, "SELECT value FROM config WHERE `key` = ?", "max_collision_prob").Scan(&s); err == nil {
		if v, err := strconvParseFloat(s); err == nil {
			cfg.maxCollisionProb = v
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT value FROM config WHERE `key` = ?", "min_hash_length").Scan(&s); err == nil {
		if v, err := strconvParseInt(s); err == nil && v > 0 {
			cfg.minLen = v
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT value FROM config WHERE `key` = ?", "max_hash_length").Scan(&s); err == nil {
		if v, err := strconvParseInt(s); err == nil && v > 0 {
			cfg.maxLen = v
		}
	}

	if cfg.minLen < 3 {
		cfg.minLen = 3
	}
	if cfg.maxLen > 8 {
		cfg.maxLen = 8
	}
	if cfg.maxLen < cfg.minLen {
		cfg.maxLen = cfg.minLen
	}
	if cfg.maxCollisionProb <= 0 || cfg.maxCollisionProb > 1 {
		cfg.maxCollisionProb = defaultAdaptiveIDConfig().maxCollisionProb
	}
	return cfg
}

func countTopLevelIssues(ctx context.Context, db *sql.DB, prefix string) (int, error) {
	// Count top-level issues only: ids matching "{prefix}-..." with no '.' in the remainder.
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM issues
		WHERE id LIKE CONCAT(?, '-%')
		  AND LOCATE('.', SUBSTRING(id, CHAR_LENGTH(?) + 2)) = 0
	`, prefix, prefix).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func getAdaptiveIDLength(ctx context.Context, db *sql.DB, prefix string) int {
	n, err := countTopLevelIssues(ctx, db, prefix)
	if err != nil {
		return 6
	}
	cfg := readAdaptiveConfig(ctx, db)
	return computeAdaptiveLen(n, cfg)
}

func genHashID(prefix, title, description, creator string, ts time.Time, length, nonce int) string {
	return idgen.GenerateHashID(prefix, title, description, creator, ts, length, nonce)
}

// tiny parsing helpers to avoid pulling extra deps here
func strconvParseFloat(s string) (float64, error) {
	// Accept both integer and float strings.
	var f float64
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%f", &f)
	return f, err
}

func strconvParseInt(s string) (int, error) {
	var i int
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &i)
	return i, err
}

func (s *EmbeddedDoltStore) generateIssueID(ctx context.Context, db *sql.DB, prefix string, issue *types.Issue, actor string) (string, error) {
	baseLen := getAdaptiveIDLength(ctx, db, prefix)
	if baseLen < 3 {
		baseLen = 3
	}
	if baseLen > 8 {
		baseLen = 8
	}
	for length := baseLen; length <= 8; length++ {
		for nonce := 0; nonce < 10; nonce++ {
			candidate := genHashID(prefix, issue.Title, issue.Description, actor, issue.CreatedAt, length, nonce)
			var count int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM issues WHERE id = ?", candidate).Scan(&count); err != nil {
				return "", fmt.Errorf("failed to check for ID collision: %w", err)
			}
			if count == 0 {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf("failed to generate unique ID after trying lengths %d-%d with 10 nonces each", baseLen, 8)
}

// Config configures the embedded-dolt backend (placeholder).
type Config struct {
	// Path is the repo-local directory containing one or more embedded Dolt databases.
	//
	// For beads, this should be `.beads/dolt`.
	Path string

	// ReadOnly indicates the store should open read-only (placeholder).
	ReadOnly bool
}

// EmbeddedDoltStore is a placeholder implementation of storage.Storage.
type EmbeddedDoltStore struct {
	path     string
	readOnly bool
	exec     *Executor
}

var _ storage.Storage = (*EmbeddedDoltStore)(nil)

func validateAndAbsPath(cfg *Config) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("embedded-dolt config is required")
	}
	if cfg.Path == "" {
		return "", fmt.Errorf("embedded-dolt path is required")
	}
	absPath, err := filepath.Abs(cfg.Path)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}
	return absPath, nil
}

// New initializes (if needed) and opens an embedded Dolt database named "beads" in cfg.Path.
//
// cfg.Path MUST be a directory whose subdirectories are Dolt databases (multi-db dir).
func New(ctx context.Context, cfg *Config) (*EmbeddedDoltStore, error) {
	absPath, err := validateAndAbsPath(cfg)
	if err != nil {
		return nil, err
	}

	// Ensure multi-db directory exists
	if err := os.MkdirAll(absPath, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create embedded dolt directory: %w", err)
	}

	exec := NewExecutor(absPath)

	// Bootstrap: ensure DB "beads" exists inside this multi-db directory.
	// We intentionally do not hold a long-lived connection.
	if _, err := exec.ExecContext(ctx, "", "CREATE DATABASE IF NOT EXISTS beads"); err != nil {
		return nil, fmt.Errorf("failed to create embedded dolt database 'beads': %w", err)
	}

	// Initialize schema in the "beads" database.
	if err := exec.withDB(ctx, "beads", func(db *sql.DB) error {
		return InitSchemaOnDB(ctx, db)
	}); err != nil {
		return nil, fmt.Errorf("failed to initialize embedded dolt schema: %w", err)
	}

	// Commit schema changes (best-effort: ignore "nothing to commit").
	if _, err := exec.ExecContext(ctx, "beads", "CALL DOLT_COMMIT('-Am', 'schema: embedded (no jsonl/flush)')"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
			return nil, fmt.Errorf("failed to dolt_commit schema: %w", err)
		}
	}

	return &EmbeddedDoltStore{
		path:     absPath,
		readOnly: cfg.ReadOnly,
		exec:     exec,
	}, nil
}

// =============================================================================
// Issues
// =============================================================================

func (s *EmbeddedDoltStore) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	if issue == nil {
		return fmt.Errorf("issue is required")
	}
	if strings.TrimSpace(issue.Title) == "" {
		return fmt.Errorf("title is required")
	}

	now := time.Now()
	if issue.CreatedAt.IsZero() {
		issue.CreatedAt = now
	}
	if issue.UpdatedAt.IsZero() {
		issue.UpdatedAt = now
	}

	// Embedded schema requires these columns non-NULL.
	if issue.Description == "" {
		issue.Description = ""
	}
	if issue.Design == "" {
		issue.Design = ""
	}
	if issue.AcceptanceCriteria == "" {
		issue.AcceptanceCriteria = ""
	}
	if issue.Notes == "" {
		issue.Notes = ""
	}
	if issue.Status == "" {
		issue.Status = types.StatusOpen
	}
	if issue.IssueType == "" {
		issue.IssueType = types.TypeTask
	}

	// Validate metadata JSON if present.
	if len(issue.Metadata) > 0 && !json.Valid(issue.Metadata) {
		return fmt.Errorf("metadata must be valid JSON")
	}

	if issue.ContentHash == "" {
		issue.ContentHash = issue.ComputeContentHash()
	}

	// Do the insert (and ID generation) in a single short-lived connection.
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		// Require issue_prefix to be configured.
		var cfgPrefix string
		if err := db.QueryRowContext(ctx, "SELECT value FROM config WHERE `key` = ?", "issue_prefix").Scan(&cfgPrefix); err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.TrimSpace(cfgPrefix) == "" {
				return fmt.Errorf("database not initialized: issue_prefix config is missing (run 'bd init --prefix <prefix>' first)")
			}
			return fmt.Errorf("failed to read issue_prefix: %w", err)
		}
		cfgPrefix = strings.TrimSpace(cfgPrefix)

		// Determine prefix to use for ID generation.
		prefix := cfgPrefix
		if strings.TrimSpace(issue.PrefixOverride) != "" {
			prefix = strings.TrimSpace(issue.PrefixOverride)
		} else if strings.TrimSpace(issue.IDPrefix) != "" {
			prefix = cfgPrefix + "-" + strings.TrimSpace(issue.IDPrefix)
		}

		if issue.ID == "" {
			id, err := s.generateIssueID(ctx, db, prefix, issue, actor)
			if err != nil {
				return err
			}
			issue.ID = id
		}

		// MySQL JSON columns accept text; empty means default JSON_OBJECT().
		meta := issue.Metadata
		if len(meta) == 0 {
			meta = []byte(`{}`)
		}

		ephemeral := 0
		if issue.Ephemeral {
			ephemeral = 1
		}
		pinned := 0
		if issue.Pinned {
			pinned = 1
		}
		isTemplate := 0
		if issue.IsTemplate {
			isTemplate = 1
		}
		crystallizes := 0
		if issue.Crystallizes {
			crystallizes = 1
		}

		waitersJSON := "[]"
		if len(issue.Waiters) > 0 {
			if b, err := json.Marshal(issue.Waiters); err == nil {
				waitersJSON = string(b)
			}
		}

		_, err := db.ExecContext(ctx, `
			INSERT INTO issues (
				id, content_hash, title, description, design, acceptance_criteria, notes,
				status, priority, issue_type, assignee, estimated_minutes,
				created_at, created_by, owner, updated_at, closed_at, closed_by_session, external_ref, spec_id,
				compaction_level, compacted_at, compacted_at_commit, original_size,
				deleted_at, deleted_by, delete_reason, original_type,
				sender, ephemeral, wisp_type, pinned, is_template, crystallizes,
				mol_type, work_type, quality_score,
				source_system, metadata, source_repo, close_reason,
				event_kind, actor, target, payload,
				await_type, await_id, timeout_ns, waiters,
				hook_bead, role_bead, agent_state, last_activity, role_type, rig,
				due_at, defer_until
			) VALUES (
				?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?, ?, ?,
				?, ?, ?,
				?, CAST(? AS JSON), ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?, ?, ?,
				?, ?
			)
		`,
			issue.ID, issue.ContentHash, issue.Title, issue.Description, issue.Design, issue.AcceptanceCriteria, issue.Notes,
			string(issue.Status), issue.Priority, string(issue.IssueType), nullIfEmpty(issue.Assignee), issue.EstimatedMinutes,
			issue.CreatedAt, issue.CreatedBy, issue.Owner, issue.UpdatedAt, issue.ClosedAt, issue.ClosedBySession, issue.ExternalRef, issue.SpecID,
			issue.CompactionLevel, issue.CompactedAt, issue.CompactedAtCommit, issue.OriginalSize,
			issue.DeletedAt, issue.DeletedBy, issue.DeleteReason, issue.OriginalType,
			issue.Sender, ephemeral, string(issue.WispType), pinned, isTemplate, crystallizes,
			string(issue.MolType), issue.WorkType, issue.QualityScore,
			issue.SourceSystem, string(meta), issue.SourceRepo, issue.CloseReason,
			issue.EventKind, issue.Actor, issue.Target, issue.Payload,
			issue.AwaitType, issue.AwaitID, int64(issue.Timeout), waitersJSON,
			issue.HookBead, issue.RoleBead, string(issue.AgentState), issue.LastActivity, issue.RoleType, issue.Rig,
			issue.DueAt, issue.DeferUntil,
		)
		if err != nil {
			return fmt.Errorf("failed to insert issue: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *EmbeddedDoltStore) CreateIssues(ctx context.Context, issues []*types.Issue, actor string) error {
	_, _, _ = ctx, issues, actor
	return unimplemented("CreateIssues")
}

func (s *EmbeddedDoltStore) CreateIssuesWithFullOptions(ctx context.Context, issues []*types.Issue, actor string, opts storage.BatchCreateOptions) error {
	_, _, _, _ = ctx, issues, actor, opts
	return unimplemented("CreateIssuesWithFullOptions")
}

func (s *EmbeddedDoltStore) GetIssue(ctx context.Context, id string) (*types.Issue, error) {
	_, _ = ctx, id
	return nil, unimplemented("GetIssue")
}

func (s *EmbeddedDoltStore) GetIssueByExternalRef(ctx context.Context, externalRef string) (*types.Issue, error) {
	_, _ = ctx, externalRef
	return nil, unimplemented("GetIssueByExternalRef")
}

func (s *EmbeddedDoltStore) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	_, _, _, _ = ctx, id, updates, actor
	return unimplemented("UpdateIssue")
}

func (s *EmbeddedDoltStore) ClaimIssue(ctx context.Context, id string, actor string) error {
	_, _, _ = ctx, id, actor
	return unimplemented("ClaimIssue")
}

func (s *EmbeddedDoltStore) CloseIssue(ctx context.Context, id string, reason string, actor string, session string) error {
	_, _, _, _, _ = ctx, id, reason, actor, session
	return unimplemented("CloseIssue")
}

func (s *EmbeddedDoltStore) DeleteIssue(ctx context.Context, id string) error {
	_, _ = ctx, id
	return unimplemented("DeleteIssue")
}

func (s *EmbeddedDoltStore) SearchIssues(ctx context.Context, query string, filter types.IssueFilter) ([]*types.Issue, error) {
	_, _, _ = ctx, query, filter
	return nil, unimplemented("SearchIssues")
}

// =============================================================================
// Dependencies
// =============================================================================

func (s *EmbeddedDoltStore) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	if dep == nil {
		return fmt.Errorf("dependency is required")
	}
	if strings.TrimSpace(dep.IssueID) == "" || strings.TrimSpace(dep.DependsOnID) == "" {
		return fmt.Errorf("dependency requires issue_id and depends_on_id")
	}
	depType := dep.Type
	if depType == "" {
		depType = types.DepBlocks
	}
	meta := strings.TrimSpace(dep.Metadata)
	if meta == "" {
		meta = "{}"
	}
	_, err := s.exec.ExecContext(ctx, "beads", `
		INSERT INTO dependencies (issue_id, depends_on_id, type, created_by, metadata, thread_id)
		VALUES (?, ?, ?, ?, CAST(? AS JSON), ?)
		ON DUPLICATE KEY UPDATE type = VALUES(type), metadata = VALUES(metadata), thread_id = VALUES(thread_id)
	`, dep.IssueID, dep.DependsOnID, string(depType), actor, meta, dep.ThreadID)
	if err != nil {
		return fmt.Errorf("failed to add dependency: %w", err)
	}
	return nil
}

func (s *EmbeddedDoltStore) RemoveDependency(ctx context.Context, issueID, dependsOnID string, actor string) error {
	_, _, _, _ = ctx, issueID, dependsOnID, actor
	return unimplemented("RemoveDependency")
}

func (s *EmbeddedDoltStore) GetDependencies(ctx context.Context, issueID string) ([]*types.Issue, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetDependencies")
}

func (s *EmbeddedDoltStore) GetDependents(ctx context.Context, issueID string) ([]*types.Issue, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetDependents")
}

func (s *EmbeddedDoltStore) GetDependenciesWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetDependenciesWithMetadata")
}

func (s *EmbeddedDoltStore) GetDependentsWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetDependentsWithMetadata")
}

func (s *EmbeddedDoltStore) GetDependencyRecords(ctx context.Context, issueID string) ([]*types.Dependency, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetDependencyRecords")
}

func (s *EmbeddedDoltStore) GetAllDependencyRecords(ctx context.Context) (map[string][]*types.Dependency, error) {
	_ = ctx
	return nil, unimplemented("GetAllDependencyRecords")
}

func (s *EmbeddedDoltStore) GetDependencyRecordsForIssues(ctx context.Context, issueIDs []string) (map[string][]*types.Dependency, error) {
	_, _ = ctx, issueIDs
	return nil, unimplemented("GetDependencyRecordsForIssues")
}

func (s *EmbeddedDoltStore) GetDependencyCounts(ctx context.Context, issueIDs []string) (map[string]*types.DependencyCounts, error) {
	_, _ = ctx, issueIDs
	return nil, unimplemented("GetDependencyCounts")
}

func (s *EmbeddedDoltStore) GetDependencyTree(ctx context.Context, issueID string, maxDepth int, showAllPaths bool, reverse bool) ([]*types.TreeNode, error) {
	_, _, _, _, _ = ctx, issueID, maxDepth, showAllPaths, reverse
	return nil, unimplemented("GetDependencyTree")
}

func (s *EmbeddedDoltStore) DetectCycles(ctx context.Context) ([][]*types.Issue, error) {
	_ = ctx
	return nil, unimplemented("DetectCycles")
}

// =============================================================================
// Labels
// =============================================================================

func (s *EmbeddedDoltStore) AddLabel(ctx context.Context, issueID, label, actor string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	_ = actor // labels table doesn't track actor
	issueID = strings.TrimSpace(issueID)
	label = strings.TrimSpace(label)
	if issueID == "" || label == "" {
		return fmt.Errorf("issueID and label are required")
	}
	_, err := s.exec.ExecContext(ctx, "beads", `
		INSERT INTO labels (issue_id, label) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE label = VALUES(label)
	`, issueID, label)
	if err != nil {
		return fmt.Errorf("failed to add label: %w", err)
	}
	return nil
}

func (s *EmbeddedDoltStore) RemoveLabel(ctx context.Context, issueID, label, actor string) error {
	_, _, _, _ = ctx, issueID, label, actor
	return unimplemented("RemoveLabel")
}

func (s *EmbeddedDoltStore) GetLabels(ctx context.Context, issueID string) ([]string, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetLabels")
}

func (s *EmbeddedDoltStore) GetLabelsForIssues(ctx context.Context, issueIDs []string) (map[string][]string, error) {
	_, _ = ctx, issueIDs
	return nil, unimplemented("GetLabelsForIssues")
}

func (s *EmbeddedDoltStore) GetIssuesByLabel(ctx context.Context, label string) ([]*types.Issue, error) {
	_, _ = ctx, label
	return nil, unimplemented("GetIssuesByLabel")
}

// =============================================================================
// Ready Work & Blocking
// =============================================================================

func (s *EmbeddedDoltStore) GetReadyWork(ctx context.Context, filter types.WorkFilter) ([]*types.Issue, error) {
	_, _ = ctx, filter
	return nil, unimplemented("GetReadyWork")
}

func (s *EmbeddedDoltStore) GetBlockedIssues(ctx context.Context, filter types.WorkFilter) ([]*types.BlockedIssue, error) {
	_, _ = ctx, filter
	return nil, unimplemented("GetBlockedIssues")
}

func (s *EmbeddedDoltStore) IsBlocked(ctx context.Context, issueID string) (bool, []string, error) {
	_, _ = ctx, issueID
	return false, nil, unimplemented("IsBlocked")
}

func (s *EmbeddedDoltStore) GetEpicsEligibleForClosure(ctx context.Context) ([]*types.EpicStatus, error) {
	_ = ctx
	return nil, unimplemented("GetEpicsEligibleForClosure")
}

func (s *EmbeddedDoltStore) GetStaleIssues(ctx context.Context, filter types.StaleFilter) ([]*types.Issue, error) {
	_, _ = ctx, filter
	return nil, unimplemented("GetStaleIssues")
}

func (s *EmbeddedDoltStore) GetNewlyUnblockedByClose(ctx context.Context, closedIssueID string) ([]*types.Issue, error) {
	_, _ = ctx, closedIssueID
	return nil, unimplemented("GetNewlyUnblockedByClose")
}

// =============================================================================
// Events
// =============================================================================

func (s *EmbeddedDoltStore) AddComment(ctx context.Context, issueID, actor, comment string) error {
	_, _, _, _ = ctx, issueID, actor, comment
	return unimplemented("AddComment")
}

func (s *EmbeddedDoltStore) GetEvents(ctx context.Context, issueID string, limit int) ([]*types.Event, error) {
	_, _, _ = ctx, issueID, limit
	return nil, unimplemented("GetEvents")
}

func (s *EmbeddedDoltStore) GetAllEventsSince(ctx context.Context, sinceID int64) ([]*types.Event, error) {
	_, _ = ctx, sinceID
	return nil, unimplemented("GetAllEventsSince")
}

// =============================================================================
// Comments
// =============================================================================

func (s *EmbeddedDoltStore) AddIssueComment(ctx context.Context, issueID, author, text string) (*types.Comment, error) {
	_, _, _, _ = ctx, issueID, author, text
	return nil, unimplemented("AddIssueComment")
}

func (s *EmbeddedDoltStore) ImportIssueComment(ctx context.Context, issueID, author, text string, createdAt time.Time) (*types.Comment, error) {
	_, _, _, _, _ = ctx, issueID, author, text, createdAt
	return nil, unimplemented("ImportIssueComment")
}

func (s *EmbeddedDoltStore) GetIssueComments(ctx context.Context, issueID string) ([]*types.Comment, error) {
	_, _ = ctx, issueID
	return nil, unimplemented("GetIssueComments")
}

func (s *EmbeddedDoltStore) GetCommentsForIssues(ctx context.Context, issueIDs []string) (map[string][]*types.Comment, error) {
	_, _ = ctx, issueIDs
	return nil, unimplemented("GetCommentsForIssues")
}

func (s *EmbeddedDoltStore) GetCommentCounts(ctx context.Context, issueIDs []string) (map[string]int, error) {
	_, _ = ctx, issueIDs
	return nil, unimplemented("GetCommentCounts")
}

// =============================================================================
// Statistics
// =============================================================================

func (s *EmbeddedDoltStore) GetStatistics(ctx context.Context) (*types.Statistics, error) {
	_ = ctx
	return nil, unimplemented("GetStatistics")
}

func (s *EmbeddedDoltStore) GetMoleculeProgress(ctx context.Context, moleculeID string) (*types.MoleculeProgressStats, error) {
	_, _ = ctx, moleculeID
	return nil, unimplemented("GetMoleculeProgress")
}

// =============================================================================
// Dirty tracking
// =============================================================================

func (s *EmbeddedDoltStore) GetDirtyIssues(ctx context.Context) ([]string, error) {
	_ = ctx
	// Embedded-dolt is DB-only: dirty tracking is disabled.
	return []string{}, nil
}

func (s *EmbeddedDoltStore) GetDirtyIssueHash(ctx context.Context, issueID string) (string, error) {
	_, _ = ctx, issueID
	// Embedded-dolt is DB-only: dirty tracking is disabled.
	return "", nil
}

func (s *EmbeddedDoltStore) ClearDirtyIssuesByID(ctx context.Context, issueIDs []string) error {
	_, _ = ctx, issueIDs
	// Embedded-dolt is DB-only: dirty tracking is disabled.
	return nil
}

// =============================================================================
// Export hash tracking
// =============================================================================

func (s *EmbeddedDoltStore) GetExportHash(ctx context.Context, issueID string) (string, error) {
	_, _ = ctx, issueID
	// Embedded-dolt is DB-only: export hashes are disabled.
	return "", nil
}

func (s *EmbeddedDoltStore) SetExportHash(ctx context.Context, issueID, contentHash string) error {
	_, _, _ = ctx, issueID, contentHash
	// Embedded-dolt is DB-only: export hashes are disabled.
	return nil
}

func (s *EmbeddedDoltStore) ClearAllExportHashes(ctx context.Context) error {
	_ = ctx
	// Embedded-dolt is DB-only: export hashes are disabled.
	return nil
}

// =============================================================================
// JSONL file integrity
// =============================================================================

func (s *EmbeddedDoltStore) GetJSONLFileHash(ctx context.Context) (string, error) {
	_ = ctx
	// Embedded-dolt is DB-only: JSONL file integrity tracking is disabled.
	return "", nil
}

func (s *EmbeddedDoltStore) SetJSONLFileHash(ctx context.Context, fileHash string) error {
	_, _ = ctx, fileHash
	// Embedded-dolt is DB-only: JSONL file integrity tracking is disabled.
	return nil
}

// =============================================================================
// ID generation
// =============================================================================

func (s *EmbeddedDoltStore) GetNextChildID(ctx context.Context, parentID string) (string, error) {
	_, _ = ctx, parentID
	return "", unimplemented("GetNextChildID")
}

// =============================================================================
// Config
// =============================================================================

func (s *EmbeddedDoltStore) SetConfig(ctx context.Context, key, value string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	_, _, _ = ctx, key, value
	_, err := s.exec.ExecContext(ctx, "beads", `
		INSERT INTO config (`+"`key`"+`, value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)
	`, key, value)
	if err != nil {
		return fmt.Errorf("failed to set config: %w", err)
	}
	return nil
}

func (s *EmbeddedDoltStore) GetConfig(ctx context.Context, key string) (string, error) {
	if s == nil || s.exec == nil {
		return "", fmt.Errorf("embedded-dolt store is not initialized")
	}
	var out string
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		err := db.QueryRowContext(ctx, "SELECT value FROM config WHERE `key` = ?", key).Scan(&out)
		if errors.Is(err, sql.ErrNoRows) {
			out = ""
			return nil
		}
		return err
	})
	if err != nil {
		return "", fmt.Errorf("failed to get config: %w", err)
	}
	return out, nil
}

func (s *EmbeddedDoltStore) GetAllConfig(ctx context.Context) (map[string]string, error) {
	_ = ctx
	return nil, unimplemented("GetAllConfig")
}

func (s *EmbeddedDoltStore) DeleteConfig(ctx context.Context, key string) error {
	_, _ = ctx, key
	return unimplemented("DeleteConfig")
}

func (s *EmbeddedDoltStore) GetCustomStatuses(ctx context.Context) ([]string, error) {
	_ = ctx
	return nil, unimplemented("GetCustomStatuses")
}

func (s *EmbeddedDoltStore) GetCustomTypes(ctx context.Context) ([]string, error) {
	_ = ctx
	return nil, unimplemented("GetCustomTypes")
}

// =============================================================================
// Metadata
// =============================================================================

func (s *EmbeddedDoltStore) SetMetadata(ctx context.Context, key, value string) error {
	if s == nil || s.exec == nil {
		return fmt.Errorf("embedded-dolt store is not initialized")
	}
	_, err := s.exec.ExecContext(ctx, "beads", `
		INSERT INTO metadata (`+"`key`"+`, value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)
	`, key, value)
	if err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}
	return nil
}

func (s *EmbeddedDoltStore) GetMetadata(ctx context.Context, key string) (string, error) {
	if s == nil || s.exec == nil {
		return "", fmt.Errorf("embedded-dolt store is not initialized")
	}
	var out string
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key` = ?", key).Scan(&out)
		if errors.Is(err, sql.ErrNoRows) {
			out = ""
			return nil
		}
		return err
	})
	if err != nil {
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}
	return out, nil
}

// =============================================================================
// Prefix rename operations
// =============================================================================

func (s *EmbeddedDoltStore) UpdateIssueID(ctx context.Context, oldID, newID string, issue *types.Issue, actor string) error {
	_, _, _, _, _ = ctx, oldID, newID, issue, actor
	return unimplemented("UpdateIssueID")
}

func (s *EmbeddedDoltStore) RenameDependencyPrefix(ctx context.Context, oldPrefix, newPrefix string) error {
	_, _, _ = ctx, oldPrefix, newPrefix
	return unimplemented("RenameDependencyPrefix")
}

func (s *EmbeddedDoltStore) RenameCounterPrefix(ctx context.Context, oldPrefix, newPrefix string) error {
	_, _, _ = ctx, oldPrefix, newPrefix
	return unimplemented("RenameCounterPrefix")
}

// =============================================================================
// Transactions
// =============================================================================

func (s *EmbeddedDoltStore) RunInTransaction(ctx context.Context, fn func(tx storage.Transaction) error) error {
	_, _ = ctx, fn
	return unimplemented("RunInTransaction")
}

// =============================================================================
// Lifecycle / DB access
// =============================================================================

func (s *EmbeddedDoltStore) Close() error {
	// No-op: this store does not keep long-lived connections.
	return nil
}

func (s *EmbeddedDoltStore) Path() string {
	return s.path
}

func (s *EmbeddedDoltStore) UnderlyingDB() *sql.DB {
	// Intentionally nil: executor opens on demand.
	return nil
}

func (s *EmbeddedDoltStore) UnderlyingConn(ctx context.Context) (*sql.Conn, error) {
	_ = ctx
	// Intentionally unsupported: executor is short-lived and does not expose conns.
	return nil, unimplemented("UnderlyingConn")
}
