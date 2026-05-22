package issueops

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/steveyegge/beads/internal/types"
)

// readyWorkIssueColumns is IssueSelectColumns rewritten with the "i." alias
// used by the mega-query. Kept in sync with IssueSelectColumns by deriving it
// at init time from the canonical constant.
var readyWorkIssueColumns = func() string {
	raw := strings.ReplaceAll(IssueSelectColumns, "\n", " ")
	raw = strings.ReplaceAll(raw, "\t", " ")
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		parts[i] = "i." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}()

// readyWorkDepJSONObject is the JSON_OBJECT(...) expression embedded inside
// JSON_ARRAYAGG to serialize a Dependency row in the mega-query. The field
// names mirror types.Dependency json tags so the result can be Unmarshal'd
// directly into []*types.Dependency.
//
//   - created_at is DATETIME; Dolt's default JSON serialization renders it
//     as "2006-01-02 15:04:05.000000" which Go's time.Time RFC3339 unmarshal
//     cannot parse, so DATE_FORMAT it to RFC3339.
//   - metadata is JSON; Dolt would embed the parsed JSON value, but
//     types.Dependency.Metadata is a string. CAST AS CHAR converts the JSON
//     value back into its string form so the outer JSON_OBJECT serializes
//     it as a quoted string.
const readyWorkDepJSONObject = `JSON_OBJECT(
	'issue_id', issue_id,
	'depends_on_id', COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external),
	'type', type,
	'created_at', DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ'),
	'created_by', created_by,
	'metadata', CAST(metadata AS CHAR),
	'thread_id', thread_id
)`

// GetReadyWorkWithCountsInTx returns ready-work issues with all the per-issue
// hydration (labels, dependency records, dependency / dependent / comment
// counts, parent ID) attached as a single SQL statement.
//
// Replaces the legacy 5-call sequence (GetReadyWork + GetLabelsForIssues +
// GetDependencyCounts + GetDependencyRecordsForIssues + GetCommentCounts) for
// the bd ready --json path. When the wisps table is non-empty the wisp
// candidates are merged in via the slower per-call hydration path so wisp
// rows still appear in the result.
//
//nolint:gosec // G201: SQL fragments come from buildReadyWorkPredicates (hardcoded shapes + ? placeholders).
func GetReadyWorkWithCountsInTx(ctx context.Context, tx *sql.Tx, filter types.WorkFilter) ([]*types.IssueWithCounts, error) {
	preds, err := buildReadyWorkPredicates(ctx, tx, filter)
	if err != nil {
		return nil, err
	}

	// Pre-aggregate each child table once via LEFT JOIN rather than via
	// correlated subqueries — those re-execute per outer row, and the
	// reverse-dependency lookup has a COALESCE on the join key that defeats
	// indexes, so the correlated form was O(N) full scans of dependencies on
	// projects with ~1k ready issues.
	//
	// NULL labels_json / deps_json are treated as "no labels" / "no deps" in
	// scanReadyWorkRowWithCounts. We do not wrap with COALESCE(...,
	// JSON_ARRAY()) here because Dolt under only_full_group_by mode flags the
	// JSON_ARRAY() literal as a non-aggregated expression in the same SELECT.
	//
	// Aggregated JSON arrays come back in arbitrary order; callers that need
	// stable ordering (labels alphabetized for display) sort Go-side after
	// unmarshaling.
	query := fmt.Sprintf(`
		SELECT %s,
			l.labels_json    AS labels_json,
			COALESCE(dc.cnt, 0) AS dep_count,
			COALESCE(rc.cnt, 0) AS rdep_count,
			COALESCE(cc.cnt, 0) AS comment_count,
			pc.parent_id     AS parent_id,
			d.deps_json      AS deps_json
		FROM issues i
		LEFT JOIN (
			SELECT issue_id, JSON_ARRAYAGG(label) AS labels_json
			FROM labels
			GROUP BY issue_id
		) l ON l.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id, COUNT(*) AS cnt
			FROM dependencies
			WHERE type = 'blocks'
			GROUP BY issue_id
		) dc ON dc.issue_id = i.id
		LEFT JOIN (
			SELECT COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external) AS dep_id,
			       COUNT(*) AS cnt
			FROM dependencies
			WHERE type = 'blocks'
			GROUP BY dep_id
		) rc ON rc.dep_id = i.id
		LEFT JOIN (
			SELECT issue_id, COUNT(*) AS cnt
			FROM comments
			GROUP BY issue_id
		) cc ON cc.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id,
			       MIN(COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external)) AS parent_id
			FROM dependencies
			WHERE type = 'parent-child'
			GROUP BY issue_id
		) pc ON pc.issue_id = i.id
		LEFT JOIN (
			SELECT issue_id, JSON_ARRAYAGG(%s) AS deps_json
			FROM dependencies
			GROUP BY issue_id
		) d ON d.issue_id = i.id
		%s
		%s
		%s
	`, readyWorkIssueColumns, readyWorkDepJSONObject, preds.whereSQL, preds.orderBySQL, preds.limitSQL)

	rows, err := tx.QueryContext(ctx, query, preds.args...)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*types.IssueWithCounts
	for rows.Next() {
		iwc, scanErr := scanReadyWorkRowWithCounts(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, iwc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get ready work with counts: rows: %w", err)
	}

	// Merge in wisp candidates. getReadyWispsInTx short-circuits to nil when
	// the wisps table is missing or empty, so this is a single cheap probe
	// on projects that never use wisps.
	wisps, err := getReadyWispsInTx(ctx, tx, filter, preds.deferredChildIDs)
	if err != nil {
		return nil, err
	}
	if len(wisps) > 0 {
		hydrated, err := hydrateWispsWithCountsInTx(ctx, tx, wisps)
		if err != nil {
			return nil, err
		}
		out = mergeReadyWithCounts(out, hydrated, filter)
	}

	return out, nil
}

// hydrateWispsWithCountsInTx attaches labels / dep records / counts /
// parent / comment counts to a wisp candidate slice using the per-issue
// helpers. Only invoked when the wisps table is non-empty, which is rare in
// practice, so the extra round-trips do not affect the dominant path.
func hydrateWispsWithCountsInTx(ctx context.Context, tx *sql.Tx, wisps []*types.Issue) ([]*types.IssueWithCounts, error) {
	if len(wisps) == 0 {
		return nil, nil
	}
	ids := make([]string, len(wisps))
	for i, w := range wisps {
		ids[i] = w.ID
	}
	labels, err := GetLabelsForIssuesInTx(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: wisp labels: %w", err)
	}
	depCounts, err := GetDependencyCountsInTx(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: wisp dep counts: %w", err)
	}
	allDeps, err := GetDependencyRecordsForIssuesInTx(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: wisp deps: %w", err)
	}
	commentCounts, err := GetCommentCountsInTx(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: wisp comments: %w", err)
	}

	out := make([]*types.IssueWithCounts, 0, len(wisps))
	for _, wisp := range wisps {
		wisp.Labels = labels[wisp.ID]
		wisp.Dependencies = allDeps[wisp.ID]
		counts := depCounts[wisp.ID]
		if counts == nil {
			counts = &types.DependencyCounts{}
		}
		var parent *string
		for _, dep := range allDeps[wisp.ID] {
			if dep.Type == types.DepParentChild {
				p := dep.DependsOnID
				parent = &p
				break
			}
		}
		out = append(out, &types.IssueWithCounts{
			Issue:           wisp,
			DependencyCount: counts.DependencyCount,
			DependentCount:  counts.DependentCount,
			CommentCount:    commentCounts[wisp.ID],
			Parent:          parent,
		})
	}
	return out, nil
}

// mergeReadyWithCounts mirrors mergeReadyWisps for the IssueWithCounts shape:
// dedupes by issue ID, sorts according to filter.SortPolicy, then trims to
// filter.Limit when set.
func mergeReadyWithCounts(issues, wisps []*types.IssueWithCounts, filter types.WorkFilter) []*types.IssueWithCounts {
	if len(wisps) == 0 {
		return issues
	}
	seen := make(map[string]struct{}, len(issues))
	for _, iwc := range issues {
		if iwc != nil && iwc.Issue != nil {
			seen[iwc.Issue.ID] = struct{}{}
		}
	}
	for _, w := range wisps {
		if w == nil || w.Issue == nil {
			continue
		}
		if _, exists := seen[w.Issue.ID]; exists {
			continue
		}
		issues = append(issues, w)
	}
	sortReadyWithCounts(issues, filter.SortPolicy)
	if filter.Limit > 0 && len(issues) > filter.Limit {
		issues = issues[:filter.Limit]
	}
	return issues
}

// sortReadyWithCounts sorts ready issues using the same policy as the legacy
// path (sortReadyIssues) but operating on []*IssueWithCounts.
func sortReadyWithCounts(items []*types.IssueWithCounts, policy types.SortPolicy) {
	if len(items) <= 1 {
		return
	}
	issues := make([]*types.Issue, 0, len(items))
	for _, item := range items {
		if item == nil || item.Issue == nil {
			continue
		}
		issues = append(issues, item.Issue)
	}
	if len(issues) != len(items) {
		// Fallback: leave order alone if any nils slipped through.
		return
	}
	sortReadyIssues(issues, policy)
	byID := make(map[string]int, len(issues))
	for i, iss := range issues {
		byID[iss.ID] = i
	}
	// Rebuild items in sorted order. Allocate fresh to avoid in-place swaps
	// that would require remapping pointers twice.
	sorted := make([]*types.IssueWithCounts, len(items))
	for _, item := range items {
		sorted[byID[item.Issue.ID]] = item
	}
	copy(items, sorted)
}

// scanReadyWorkRowWithCounts scans a single mega-query row, hydrating Labels
// and Dependencies on the embedded Issue from the JSON aggregates and
// populating the per-issue count fields.
func scanReadyWorkRowWithCounts(rows *sql.Rows) (*types.IssueWithCounts, error) {
	// Build a scanner that drains IssueSelectColumns first via ScanIssueFrom,
	// then consumes the appended aggregate columns.
	var labelsJSON, depsJSON sql.NullString
	var parentID sql.NullString
	var depCount, rdepCount, commentCount sql.NullInt64

	composite := &compositeReadyRow{
		row: rows,
		extra: []any{
			&labelsJSON,
			&depCount,
			&rdepCount,
			&commentCount,
			&parentID,
			&depsJSON,
		},
	}
	issue, err := ScanIssueFrom(composite)
	if err != nil {
		return nil, fmt.Errorf("get ready work with counts: scan issue: %w", err)
	}

	if labelsJSON.Valid && labelsJSON.String != "" {
		var labels []string
		if err := json.Unmarshal([]byte(labelsJSON.String), &labels); err != nil {
			return nil, fmt.Errorf("get ready work with counts: parse labels_json: %w", err)
		}
		// The pre-aggregated JSON_ARRAYAGG does not preserve input order;
		// alphabetize so the JSON output is stable and matches the legacy
		// per-batch label SELECT which used ORDER BY issue_id, label.
		sort.Strings(labels)
		issue.Labels = labels
	}

	if depsJSON.Valid && depsJSON.String != "" {
		var deps []*types.Dependency
		if err := json.Unmarshal([]byte(depsJSON.String), &deps); err != nil {
			return nil, fmt.Errorf("get ready work with counts: parse deps_json: %w", err)
		}
		issue.Dependencies = deps
	}

	iwc := &types.IssueWithCounts{
		Issue:           issue,
		DependencyCount: int(depCount.Int64),
		DependentCount:  int(rdepCount.Int64),
		CommentCount:    int(commentCount.Int64),
	}
	if parentID.Valid {
		s := parentID.String
		iwc.Parent = &s
	}
	return iwc, nil
}

// compositeReadyRow adapts *sql.Rows + a tail of extra destination pointers
// to the IssueScanner interface expected by ScanIssueFrom. ScanIssueFrom
// calls Scan with the IssueSelectColumns destinations; we append the
// aggregate-column destinations transparently.
type compositeReadyRow struct {
	row   *sql.Rows
	extra []any
}

func (c *compositeReadyRow) Scan(dest ...any) error {
	combined := make([]any, 0, len(dest)+len(c.extra))
	combined = append(combined, dest...)
	combined = append(combined, c.extra...)
	return c.row.Scan(combined...)
}
