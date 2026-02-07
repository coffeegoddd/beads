// Package embeddeddolt is a placeholder storage backend for a future embedded Dolt implementation.
//
// For now, it satisfies the storage.Storage interface but returns "unimplemented" errors
// for all operations. This allows plumbing and CLI selection to land before the
// real embedded implementation is built.
package embeddeddolt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/types"
)

// ErrUnimplemented is returned by all EmbeddedDoltStore methods for now.
var ErrUnimplemented = errors.New("embedded-dolt backend is not implemented")

func unimplemented(method string) error {
	return fmt.Errorf("%w: %s", ErrUnimplemented, method)
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
	exec *Executor
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
		return dolt.InitSchemaOnDB(ctx, db)
	}); err != nil {
		return nil, fmt.Errorf("failed to initialize embedded dolt schema: %w", err)
	}

	// Commit schema changes (best-effort: ignore "nothing to commit").
	if _, err := exec.ExecContext(ctx, "beads", "CALL DOLT_COMMIT('-Am', 'schema: init')"); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "nothing to commit") {
			return nil, fmt.Errorf("failed to dolt_commit schema: %w", err)
		}
	}

	return &EmbeddedDoltStore{
		path:      absPath,
		readOnly:  cfg.ReadOnly,
		exec:      exec,
	}, nil
}

// =============================================================================
// Issues
// =============================================================================

func (s *EmbeddedDoltStore) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	_, _, _ = ctx, issue, actor
	return unimplemented("CreateIssue")
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
	_, _, _ = ctx, dep, actor
	return unimplemented("AddDependency")
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
	_, _, _, _ = ctx, issueID, label, actor
	return unimplemented("AddLabel")
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
	return nil, unimplemented("GetDirtyIssues")
}

func (s *EmbeddedDoltStore) GetDirtyIssueHash(ctx context.Context, issueID string) (string, error) {
	_, _ = ctx, issueID
	return "", unimplemented("GetDirtyIssueHash")
}

func (s *EmbeddedDoltStore) ClearDirtyIssuesByID(ctx context.Context, issueIDs []string) error {
	_, _ = ctx, issueIDs
	return unimplemented("ClearDirtyIssuesByID")
}

// =============================================================================
// Export hash tracking
// =============================================================================

func (s *EmbeddedDoltStore) GetExportHash(ctx context.Context, issueID string) (string, error) {
	_, _ = ctx, issueID
	return "", unimplemented("GetExportHash")
}

func (s *EmbeddedDoltStore) SetExportHash(ctx context.Context, issueID, contentHash string) error {
	_, _, _ = ctx, issueID, contentHash
	return unimplemented("SetExportHash")
}

func (s *EmbeddedDoltStore) ClearAllExportHashes(ctx context.Context) error {
	_ = ctx
	return unimplemented("ClearAllExportHashes")
}

// =============================================================================
// JSONL file integrity
// =============================================================================

func (s *EmbeddedDoltStore) GetJSONLFileHash(ctx context.Context) (string, error) {
	_ = ctx
	return "", unimplemented("GetJSONLFileHash")
}

func (s *EmbeddedDoltStore) SetJSONLFileHash(ctx context.Context, fileHash string) error {
	_, _ = ctx, fileHash
	return unimplemented("SetJSONLFileHash")
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
	_, _, _ = ctx, key, value
	return unimplemented("SetConfig")
}

func (s *EmbeddedDoltStore) GetConfig(ctx context.Context, key string) (string, error) {
	_, _ = ctx, key
	return "", unimplemented("GetConfig")
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
	_, _, _ = ctx, key, value
	return unimplemented("SetMetadata")
}

func (s *EmbeddedDoltStore) GetMetadata(ctx context.Context, key string) (string, error) {
	_, _ = ctx, key
	return "", unimplemented("GetMetadata")
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

