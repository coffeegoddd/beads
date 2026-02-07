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

func parseTimeString(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseNullableTimeString(ns sql.NullString) *time.Time {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, ns.String); err == nil {
			return &t
		}
	}
	return nil
}

func parseJSONStringArray(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	return out
}

func globToLike(pattern string) string {
	p := strings.ReplaceAll(pattern, "*", "%")
	p = strings.ReplaceAll(p, "?", "_")
	return p
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
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("id is required")
	}

	var issue *types.Issue
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		issues, err := s.getIssuesByIDs(ctx, db, []string{id})
		if err != nil {
			return err
		}
		if len(issues) == 0 {
			issue = nil
			return nil
		}
		issue = issues[0]
		return nil
	})
	if err != nil {
		return nil, err
	}
	if issue == nil {
		return nil, nil
	}

	labels, err := s.GetLabels(ctx, issue.ID)
	if err == nil {
		issue.Labels = labels
	}
	return issue, nil
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
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}

	var out []*types.Issue
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		whereClauses := []string{}
		args := []any{}

		if query != "" {
			whereClauses = append(whereClauses, "(title LIKE ? OR description LIKE ? OR id LIKE ?)")
			p := "%" + query + "%"
			args = append(args, p, p, p)
		}

		if filter.TitleSearch != "" {
			whereClauses = append(whereClauses, "title LIKE ?")
			args = append(args, "%"+filter.TitleSearch+"%")
		}
		if filter.TitleContains != "" {
			whereClauses = append(whereClauses, "title LIKE ?")
			args = append(args, "%"+filter.TitleContains+"%")
		}
		if filter.DescriptionContains != "" {
			whereClauses = append(whereClauses, "description LIKE ?")
			args = append(args, "%"+filter.DescriptionContains+"%")
		}
		if filter.NotesContains != "" {
			whereClauses = append(whereClauses, "notes LIKE ?")
			args = append(args, "%"+filter.NotesContains+"%")
		}

		if filter.Status != nil {
			whereClauses = append(whereClauses, "status = ?")
			args = append(args, string(*filter.Status))
		} else if !filter.IncludeTombstones {
			whereClauses = append(whereClauses, "status != ?")
			args = append(args, string(types.StatusTombstone))
		}

		if len(filter.ExcludeStatus) > 0 {
			ph := make([]string, 0, len(filter.ExcludeStatus))
			for _, st := range filter.ExcludeStatus {
				ph = append(ph, "?")
				args = append(args, string(st))
			}
			whereClauses = append(whereClauses, fmt.Sprintf("status NOT IN (%s)", strings.Join(ph, ",")))
		}

		if len(filter.ExcludeTypes) > 0 {
			ph := make([]string, 0, len(filter.ExcludeTypes))
			for _, t := range filter.ExcludeTypes {
				ph = append(ph, "?")
				args = append(args, string(t))
			}
			whereClauses = append(whereClauses, fmt.Sprintf("id IN (SELECT id FROM issues WHERE issue_type NOT IN (%s))", strings.Join(ph, ",")))
		}

		if filter.Priority != nil {
			whereClauses = append(whereClauses, "priority = ?")
			args = append(args, *filter.Priority)
		}
		if filter.PriorityMin != nil {
			whereClauses = append(whereClauses, "priority >= ?")
			args = append(args, *filter.PriorityMin)
		}
		if filter.PriorityMax != nil {
			whereClauses = append(whereClauses, "priority <= ?")
			args = append(args, *filter.PriorityMax)
		}

		if filter.IssueType != nil {
			whereClauses = append(whereClauses, "issue_type = ?")
			args = append(args, string(*filter.IssueType))
		}
		if filter.Assignee != nil {
			whereClauses = append(whereClauses, "assignee = ?")
			args = append(args, *filter.Assignee)
		}

		// Date ranges
		if filter.CreatedAfter != nil {
			whereClauses = append(whereClauses, "created_at > ?")
			args = append(args, filter.CreatedAfter.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.CreatedBefore != nil {
			whereClauses = append(whereClauses, "created_at < ?")
			args = append(args, filter.CreatedBefore.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.UpdatedAfter != nil {
			whereClauses = append(whereClauses, "updated_at > ?")
			args = append(args, filter.UpdatedAfter.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.UpdatedBefore != nil {
			whereClauses = append(whereClauses, "updated_at < ?")
			args = append(args, filter.UpdatedBefore.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.ClosedAfter != nil {
			whereClauses = append(whereClauses, "closed_at > ?")
			args = append(args, filter.ClosedAfter.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.ClosedBefore != nil {
			whereClauses = append(whereClauses, "closed_at < ?")
			args = append(args, filter.ClosedBefore.UTC().Format("2006-01-02 15:04:05"))
		}

		// Empty/null checks
		if filter.EmptyDescription {
			whereClauses = append(whereClauses, "(description IS NULL OR description = '')")
		}
		if filter.NoAssignee {
			whereClauses = append(whereClauses, "(assignee IS NULL OR assignee = '')")
		}
		if filter.NoLabels {
			whereClauses = append(whereClauses, "id NOT IN (SELECT DISTINCT issue_id FROM labels)")
		}

		// Label filtering (AND)
		for _, l := range filter.Labels {
			whereClauses = append(whereClauses, "id IN (SELECT issue_id FROM labels WHERE label = ?)")
			args = append(args, l)
		}
		// Label filtering (OR)
		if len(filter.LabelsAny) > 0 {
			ph := make([]string, 0, len(filter.LabelsAny))
			for _, l := range filter.LabelsAny {
				ph = append(ph, "?")
				args = append(args, l)
			}
			whereClauses = append(whereClauses, fmt.Sprintf("id IN (SELECT issue_id FROM labels WHERE label IN (%s))", strings.Join(ph, ", ")))
		}
		if filter.LabelPattern != "" {
			whereClauses = append(whereClauses, "id IN (SELECT issue_id FROM labels WHERE label LIKE ?)")
			args = append(args, globToLike(filter.LabelPattern))
		}
		if filter.LabelRegex != "" {
			whereClauses = append(whereClauses, "id IN (SELECT issue_id FROM labels WHERE label REGEXP ?)")
			args = append(args, filter.LabelRegex)
		}

		// ID filtering
		if len(filter.IDs) > 0 {
			ph := make([]string, 0, len(filter.IDs))
			for _, id := range filter.IDs {
				ph = append(ph, "?")
				args = append(args, id)
			}
			whereClauses = append(whereClauses, fmt.Sprintf("id IN (%s)", strings.Join(ph, ", ")))
		}
		if filter.IDPrefix != "" {
			whereClauses = append(whereClauses, "id LIKE ?")
			args = append(args, filter.IDPrefix+"%")
		}
		if filter.SpecIDPrefix != "" {
			whereClauses = append(whereClauses, "spec_id LIKE ?")
			args = append(args, filter.SpecIDPrefix+"%")
		}

		// Wisp/pinned/template
		if filter.Ephemeral != nil {
			if *filter.Ephemeral {
				whereClauses = append(whereClauses, "ephemeral = 1")
			} else {
				whereClauses = append(whereClauses, "(ephemeral = 0 OR ephemeral IS NULL)")
			}
		}
		if filter.Pinned != nil {
			if *filter.Pinned {
				whereClauses = append(whereClauses, "pinned = 1")
			} else {
				whereClauses = append(whereClauses, "(pinned = 0 OR pinned IS NULL)")
			}
		}
		if filter.IsTemplate != nil {
			if *filter.IsTemplate {
				whereClauses = append(whereClauses, "is_template = 1")
			} else {
				whereClauses = append(whereClauses, "(is_template = 0 OR is_template IS NULL)")
			}
		}

		// Parent filtering
		if filter.ParentID != nil {
			whereClauses = append(whereClauses, "id IN (SELECT issue_id FROM dependencies WHERE type = 'parent-child' AND depends_on_id = ?)")
			args = append(args, *filter.ParentID)
		}

		if filter.MolType != nil {
			whereClauses = append(whereClauses, "mol_type = ?")
			args = append(args, string(*filter.MolType))
		}
		if filter.WispType != nil {
			whereClauses = append(whereClauses, "wisp_type = ?")
			args = append(args, string(*filter.WispType))
		}

		// Time scheduling
		if filter.Deferred {
			whereClauses = append(whereClauses, "defer_until IS NOT NULL")
		}
		if filter.DeferAfter != nil {
			whereClauses = append(whereClauses, "defer_until > ?")
			args = append(args, filter.DeferAfter.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.DeferBefore != nil {
			whereClauses = append(whereClauses, "defer_until < ?")
			args = append(args, filter.DeferBefore.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.DueAfter != nil {
			whereClauses = append(whereClauses, "due_at > ?")
			args = append(args, filter.DueAfter.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.DueBefore != nil {
			whereClauses = append(whereClauses, "due_at < ?")
			args = append(args, filter.DueBefore.UTC().Format("2006-01-02 15:04:05"))
		}
		if filter.Overdue {
			whereClauses = append(whereClauses, "due_at IS NOT NULL AND due_at < ? AND status != ?")
			args = append(args, time.Now().UTC().Format("2006-01-02 15:04:05"), string(types.StatusClosed))
		}

		whereSQL := ""
		if len(whereClauses) > 0 {
			whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
		}
		limitSQL := ""
		if filter.Limit > 0 {
			limitSQL = fmt.Sprintf(" LIMIT %d", filter.Limit)
		}

		// nolint:gosec // whereSQL built from safe predicates with ?, limitSQL is int
		q := fmt.Sprintf(`
			SELECT id FROM issues
			%s
			ORDER BY priority ASC, created_at DESC
			%s
		`, whereSQL, limitSQL)

		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("failed to search issues: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("failed to scan issue id: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()

		issues, err := s.getIssuesByIDs(ctx, db, ids)
		if err != nil {
			return err
		}
		out = issues
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}

	out := map[string][]*types.Dependency{}
	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		rows, err := db.QueryContext(ctx, `
			SELECT issue_id, depends_on_id, type, CAST(metadata AS CHAR), thread_id
			FROM dependencies
		`)
		if err != nil {
			return fmt.Errorf("failed to query dependencies: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var issueID, dependsOnID, typ, meta, threadID sql.NullString
			if err := rows.Scan(&issueID, &dependsOnID, &typ, &meta, &threadID); err != nil {
				return err
			}
			d := &types.Dependency{
				IssueID:     issueID.String,
				DependsOnID: dependsOnID.String,
				Type:        types.DependencyType(typ.String),
				Metadata:    meta.String,
				ThreadID:    threadID.String,
			}
			out[d.IssueID] = append(out[d.IssueID], d)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EmbeddedDoltStore) GetDependencyRecordsForIssues(ctx context.Context, issueIDs []string) (map[string][]*types.Dependency, error) {
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}

	out := map[string][]*types.Dependency{}
	if len(issueIDs) == 0 {
		return out, nil
	}

	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		ph := make([]string, len(issueIDs))
		args := make([]any, len(issueIDs))
		for i, id := range issueIDs {
			ph[i] = "?"
			args[i] = id
		}
		// nolint:gosec // placeholders only
		q := fmt.Sprintf(`
			SELECT issue_id, depends_on_id, type, CAST(metadata AS CHAR), thread_id
			FROM dependencies
			WHERE issue_id IN (%s)
		`, strings.Join(ph, ","))
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("failed to query dependencies: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var issueID, dependsOnID, typ, meta, threadID sql.NullString
			if err := rows.Scan(&issueID, &dependsOnID, &typ, &meta, &threadID); err != nil {
				return err
			}
			d := &types.Dependency{
				IssueID:     issueID.String,
				DependsOnID: dependsOnID.String,
				Type:        types.DependencyType(typ.String),
				Metadata:    meta.String,
				ThreadID:    threadID.String,
			}
			out[d.IssueID] = append(out[d.IssueID], d)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *EmbeddedDoltStore) GetDependencyCounts(ctx context.Context, issueIDs []string) (map[string]*types.DependencyCounts, error) {
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}

	out := map[string]*types.DependencyCounts{}
	if len(issueIDs) == 0 {
		return out, nil
	}

	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		ph := make([]string, len(issueIDs))
		args := make([]any, len(issueIDs))
		for i, id := range issueIDs {
			ph[i] = "?"
			args[i] = id
		}
		in := strings.Join(ph, ",")

		// Outgoing dependencies
		// nolint:gosec // placeholders only
		q1 := fmt.Sprintf(`SELECT issue_id, COUNT(*) FROM dependencies WHERE issue_id IN (%s) GROUP BY issue_id`, in)
		rows, err := db.QueryContext(ctx, q1, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			var c int
			if err := rows.Scan(&id, &c); err != nil {
				_ = rows.Close()
				return err
			}
			if out[id] == nil {
				out[id] = &types.DependencyCounts{}
			}
			out[id].DependencyCount = c
		}
		_ = rows.Close()

		// Incoming dependencies (dependents)
		// nolint:gosec // placeholders only
		q2 := fmt.Sprintf(`SELECT depends_on_id, COUNT(*) FROM dependencies WHERE depends_on_id IN (%s) GROUP BY depends_on_id`, in)
		rows2, err := db.QueryContext(ctx, q2, args...)
		if err != nil {
			return err
		}
		for rows2.Next() {
			var id string
			var c int
			if err := rows2.Scan(&id, &c); err != nil {
				_ = rows2.Close()
				return err
			}
			if out[id] == nil {
				out[id] = &types.DependencyCounts{}
			}
			out[id].DependentCount = c
		}
		_ = rows2.Close()

		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
	m, err := s.GetLabelsForIssues(ctx, []string{issueID})
	if err != nil {
		return nil, err
	}
	return m[issueID], nil
}

func (s *EmbeddedDoltStore) GetLabelsForIssues(ctx context.Context, issueIDs []string) (map[string][]string, error) {
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}
	out := map[string][]string{}
	if len(issueIDs) == 0 {
		return out, nil
	}

	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		ph := make([]string, len(issueIDs))
		args := make([]any, len(issueIDs))
		for i, id := range issueIDs {
			ph[i] = "?"
			args[i] = id
		}
		// nolint:gosec // placeholders only
		q := fmt.Sprintf(`SELECT issue_id, label FROM labels WHERE issue_id IN (%s) ORDER BY issue_id, label`, strings.Join(ph, ","))
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var id, label string
			if err := rows.Scan(&id, &label); err != nil {
				return err
			}
			out[id] = append(out[id], label)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
	if s == nil || s.exec == nil {
		return nil, fmt.Errorf("embedded-dolt store is not initialized")
	}
	out := map[string]int{}
	if len(issueIDs) == 0 {
		return out, nil
	}

	err := s.exec.withDB(ctx, "beads", func(db *sql.DB) error {
		ph := make([]string, len(issueIDs))
		args := make([]any, len(issueIDs))
		for i, id := range issueIDs {
			ph[i] = "?"
			args[i] = id
		}
		// nolint:gosec // placeholders only
		q := fmt.Sprintf(`SELECT issue_id, COUNT(*) FROM comments WHERE issue_id IN (%s) GROUP BY issue_id`, strings.Join(ph, ","))
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var id string
			var c int
			if err := rows.Scan(&id, &c); err != nil {
				return err
			}
			out[id] = c
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// =============================================================================
// Statistics
// =============================================================================

func (s *EmbeddedDoltStore) GetStatistics(ctx context.Context) (*types.Statistics, error) {
	_ = ctx
	return nil, unimplemented("GetStatistics")
}

// =============================================================================
// Embedded-dolt read helpers
// =============================================================================

func (s *EmbeddedDoltStore) getIssuesByIDs(ctx context.Context, db *sql.DB, ids []string) ([]*types.Issue, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}

	// Cast timestamps + JSON to strings for robust scanning across Dolt driver modes.
	// nolint:gosec // placeholders only
	q := fmt.Sprintf(`
		SELECT
			id, content_hash, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type, assignee, estimated_minutes,
			CAST(created_at AS CHAR), created_by, owner, CAST(updated_at AS CHAR), CAST(closed_at AS CHAR), closed_by_session,
			external_ref, spec_id, source_repo, close_reason,
			CAST(deleted_at AS CHAR), deleted_by, delete_reason, original_type,
			sender, ephemeral, wisp_type, pinned, is_template, crystallizes,
			await_type, await_id, timeout_ns, waiters,
			hook_bead, role_bead, agent_state, CAST(last_activity AS CHAR), role_type, rig, mol_type,
			event_kind, actor, target, payload,
			CAST(due_at AS CHAR), CAST(defer_until AS CHAR),
			quality_score, work_type, source_system,
			CAST(metadata AS CHAR)
		FROM issues
		WHERE id IN (%s)
	`, strings.Join(ph, ","))

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get issues by IDs: %w", err)
	}
	defer rows.Close()

	var out []*types.Issue
	for rows.Next() {
		iss, err := scanEmbeddedIssueRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, iss)
	}
	return out, rows.Err()
}

func scanEmbeddedIssueRow(rows *sql.Rows) (*types.Issue, error) {
	var issue types.Issue

	var contentHash, status, issueType sql.NullString
	var assignee sql.NullString
	var estimatedMinutes sql.NullInt64
	var createdAtStr, updatedAtStr sql.NullString
	var closedAtStr, deletedAtStr, lastActivityStr, dueAtStr, deferUntilStr sql.NullString
	var closedBySession, externalRef, specID, sourceRepo, closeReason sql.NullString
	var deletedBy, deleteReason, originalType sql.NullString
	var sender, wispType, molType, eventKind, actor, target, payload sql.NullString
	var awaitType, awaitID, waiters sql.NullString
	var hookBead, roleBead, agentState, roleType, rig sql.NullString
	var ephemeral, pinned, isTemplate, crystallizes sql.NullInt64
	var priority int
	var timeoutNs sql.NullInt64
	var qualityScore sql.NullFloat64
	var workType, sourceSystem sql.NullString
	var metadataStr sql.NullString
	var createdBy, owner sql.NullString

	if err := rows.Scan(
		&issue.ID, &contentHash, &issue.Title, &issue.Description, &issue.Design, &issue.AcceptanceCriteria, &issue.Notes,
		&status, &priority, &issueType, &assignee, &estimatedMinutes,
		&createdAtStr, &createdBy, &owner, &updatedAtStr, &closedAtStr, &closedBySession,
		&externalRef, &specID, &sourceRepo, &closeReason,
		&deletedAtStr, &deletedBy, &deleteReason, &originalType,
		&sender, &ephemeral, &wispType, &pinned, &isTemplate, &crystallizes,
		&awaitType, &awaitID, &timeoutNs, &waiters,
		&hookBead, &roleBead, &agentState, &lastActivityStr, &roleType, &rig, &molType,
		&eventKind, &actor, &target, &payload,
		&dueAtStr, &deferUntilStr,
		&qualityScore, &workType, &sourceSystem,
		&metadataStr,
	); err != nil {
		return nil, fmt.Errorf("failed to scan issue row: %w", err)
	}

	issue.Priority = priority
	if contentHash.Valid {
		issue.ContentHash = contentHash.String
	}
	if status.Valid {
		issue.Status = types.Status(status.String)
	}
	if issueType.Valid {
		issue.IssueType = types.IssueType(issueType.String)
	}
	if assignee.Valid {
		issue.Assignee = assignee.String
	}
	if estimatedMinutes.Valid {
		v := int(estimatedMinutes.Int64)
		issue.EstimatedMinutes = &v
	}
	if createdAtStr.Valid {
		issue.CreatedAt = parseTimeString(createdAtStr.String)
	}
	if updatedAtStr.Valid {
		issue.UpdatedAt = parseTimeString(updatedAtStr.String)
	}
	issue.ClosedAt = parseNullableTimeString(closedAtStr)
	issue.DeletedAt = parseNullableTimeString(deletedAtStr)
	if lastActivity := parseNullableTimeString(lastActivityStr); lastActivity != nil {
		issue.LastActivity = lastActivity
	}
	if due := parseNullableTimeString(dueAtStr); due != nil {
		issue.DueAt = due
	}
	if deferUntil := parseNullableTimeString(deferUntilStr); deferUntil != nil {
		issue.DeferUntil = deferUntil
	}
	if createdBy.Valid {
		issue.CreatedBy = createdBy.String
	}
	if owner.Valid {
		issue.Owner = owner.String
	}
	if closedBySession.Valid {
		issue.ClosedBySession = closedBySession.String
	}
	if externalRef.Valid && externalRef.String != "" {
		s := externalRef.String
		issue.ExternalRef = &s
	}
	if specID.Valid {
		issue.SpecID = specID.String
	}
	if sourceRepo.Valid {
		issue.SourceRepo = sourceRepo.String
	}
	if closeReason.Valid {
		issue.CloseReason = closeReason.String
	}
	if deletedBy.Valid {
		issue.DeletedBy = deletedBy.String
	}
	if deleteReason.Valid {
		issue.DeleteReason = deleteReason.String
	}
	if originalType.Valid {
		issue.OriginalType = originalType.String
	}
	if sender.Valid {
		issue.Sender = sender.String
	}
	if ephemeral.Valid && ephemeral.Int64 != 0 {
		issue.Ephemeral = true
	}
	if wispType.Valid {
		issue.WispType = types.WispType(wispType.String)
	}
	if pinned.Valid && pinned.Int64 != 0 {
		issue.Pinned = true
	}
	if isTemplate.Valid && isTemplate.Int64 != 0 {
		issue.IsTemplate = true
	}
	if crystallizes.Valid && crystallizes.Int64 != 0 {
		issue.Crystallizes = true
	}
	if awaitType.Valid {
		issue.AwaitType = awaitType.String
	}
	if awaitID.Valid {
		issue.AwaitID = awaitID.String
	}
	if timeoutNs.Valid {
		issue.Timeout = time.Duration(timeoutNs.Int64)
	}
	if waiters.Valid {
		issue.Waiters = parseJSONStringArray(waiters.String)
	}
	if hookBead.Valid {
		issue.HookBead = hookBead.String
	}
	if roleBead.Valid {
		issue.RoleBead = roleBead.String
	}
	if agentState.Valid {
		issue.AgentState = types.AgentState(agentState.String)
	}
	if roleType.Valid {
		issue.RoleType = roleType.String
	}
	if rig.Valid {
		issue.Rig = rig.String
	}
	if molType.Valid {
		issue.MolType = types.MolType(molType.String)
	}
	if eventKind.Valid {
		issue.EventKind = eventKind.String
	}
	if actor.Valid {
		issue.Actor = actor.String
	}
	if target.Valid {
		issue.Target = target.String
	}
	if payload.Valid {
		issue.Payload = payload.String
	}
	if qualityScore.Valid {
		v := float32(qualityScore.Float64)
		issue.QualityScore = &v
	}
	if workType.Valid {
		issue.WorkType = types.WorkType(workType.String)
	}
	if sourceSystem.Valid {
		issue.SourceSystem = sourceSystem.String
	}
	if metadataStr.Valid && metadataStr.String != "" {
		if json.Valid([]byte(metadataStr.String)) {
			issue.Metadata = json.RawMessage([]byte(metadataStr.String))
		}
	}

	return &issue, nil
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
