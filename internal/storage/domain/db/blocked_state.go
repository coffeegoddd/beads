package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func NewBlockedStateSQLRepository(runner Runner) domain.BlockedStateSQLRepository {
	return &blockedStateSQLRepositoryImpl{runner: runner}
}

type blockedStateSQLRepositoryImpl struct {
	runner Runner
}

var _ domain.BlockedStateSQLRepository = (*blockedStateSQLRepositoryImpl)(nil)

const waitsForGateBlockedSQL = `
		(
		  EXISTS (
		    SELECT 1 FROM dependencies cd JOIN issues child ON child.id = cd.issue_id
		    WHERE cd.type = 'parent-child'
		      AND ((d.depends_on_issue_id IS NOT NULL AND cd.depends_on_issue_id = d.depends_on_issue_id)
		        OR (d.depends_on_wisp_id IS NOT NULL AND cd.depends_on_wisp_id = d.depends_on_wisp_id))
		      AND child.status <> 'closed' AND child.status <> 'pinned'
		  )
		  OR EXISTS (
		    SELECT 1 FROM wisp_dependencies cd JOIN wisps child ON child.id = cd.issue_id
		    WHERE cd.type = 'parent-child'
		      AND ((d.depends_on_issue_id IS NOT NULL AND cd.depends_on_issue_id = d.depends_on_issue_id)
		        OR (d.depends_on_wisp_id IS NOT NULL AND cd.depends_on_wisp_id = d.depends_on_wisp_id))
		      AND child.status <> 'closed' AND child.status <> 'pinned'
		  )
		)
		AND NOT (
		  JSON_UNQUOTE(JSON_EXTRACT(d.metadata, '$.gate')) = 'any-children'
		  AND (
		    EXISTS (
		      SELECT 1 FROM dependencies cd JOIN issues child ON child.id = cd.issue_id
		      WHERE cd.type = 'parent-child'
		        AND ((d.depends_on_issue_id IS NOT NULL AND cd.depends_on_issue_id = d.depends_on_issue_id)
		          OR (d.depends_on_wisp_id IS NOT NULL AND cd.depends_on_wisp_id = d.depends_on_wisp_id))
		        AND child.status = 'closed'
		    )
		    OR EXISTS (
		      SELECT 1 FROM wisp_dependencies cd JOIN wisps child ON child.id = cd.issue_id
		      WHERE cd.type = 'parent-child'
		        AND ((d.depends_on_issue_id IS NOT NULL AND cd.depends_on_issue_id = d.depends_on_issue_id)
		          OR (d.depends_on_wisp_id IS NOT NULL AND cd.depends_on_wisp_id = d.depends_on_wisp_id))
		        AND child.status = 'closed'
		    )
		  )
		)
`

func (r *blockedStateSQLRepositoryImpl) Recompute(ctx context.Context, affected domain.AffectedIDs) error {
	if len(affected.IssueIDs) == 0 && len(affected.WispIDs) == 0 {
		return nil
	}
	for {
		var changed int64

		n, err := recomputeIssuesPass(ctx, r.runner, affected.IssueIDs)
		if err != nil {
			return err
		}
		changed += n

		n, err = recomputeWispsPass(ctx, r.runner, affected.WispIDs)
		if err != nil {
			return err
		}
		changed += n

		if changed == 0 {
			return nil
		}
	}
}

func (r *blockedStateSQLRepositoryImpl) MarkOnly(ctx context.Context, affected domain.AffectedIDs) error {
	if len(affected.IssueIDs) == 0 && len(affected.WispIDs) == 0 {
		return nil
	}
	for {
		var changed int64

		n, err := markIssuesPass(ctx, r.runner, affected.IssueIDs)
		if err != nil {
			return err
		}
		changed += n

		n, err = markWispsPass(ctx, r.runner, affected.WispIDs)
		if err != nil {
			return err
		}
		changed += n

		if changed == 0 {
			return nil
		}
	}
}

func (r *blockedStateSQLRepositoryImpl) AffectedByStatusChange(ctx context.Context, id string, isWisp bool) (domain.AffectedIDs, error) {
	if id == "" {
		return domain.AffectedIDs{}, errors.New("db: AffectedByStatusChange: id must not be empty")
	}
	var issueSeed, wispSeed []string
	issueSeen := make(map[string]bool)
	wispSeen := make(map[string]bool)
	if isWisp {
		wispSeed = []string{id}
		wispSeen[id] = true
	} else {
		issueSeed = []string{id}
		issueSeen[id] = true
	}

	targetCol := "depends_on_issue_id"
	if isWisp {
		targetCol = "depends_on_wisp_id"
	}
	if err := r.loadBlockingDependersForIDs(ctx, targetCol, []string{id}, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}
	if err := r.loadWaitersWhoseSpawnerIsParentOf(ctx, id, isWisp, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}
	return r.expandByParentChildDescendants(ctx, issueSeed, wispSeed, issueSeen, wispSeen)
}

func (r *blockedStateSQLRepositoryImpl) AffectedByDepChange(ctx context.Context, source, target string, depType types.DependencyType, isWispSource bool) (domain.AffectedIDs, error) {
	switch depType {
	case types.DepBlocks, types.DepConditionalBlocks, types.DepWaitsFor, types.DepParentChild:
	default:
		return domain.AffectedIDs{}, nil
	}
	if source == "" {
		return domain.AffectedIDs{}, errors.New("db: AffectedByDepChange: source must not be empty")
	}

	var issueSeed, wispSeed []string
	issueSeen := make(map[string]bool)
	wispSeen := make(map[string]bool)
	if isWispSource {
		wispSeed = []string{source}
		wispSeen[source] = true
	} else {
		issueSeed = []string{source}
		issueSeen[source] = true
	}

	if depType == types.DepParentChild && target != "" {
		if err := r.loadWaitersOnSpawnerIDs(ctx, []string{target}, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
			return domain.AffectedIDs{}, err
		}
	}
	return r.expandByParentChildDescendants(ctx, issueSeed, wispSeed, issueSeen, wispSeen)
}

func (r *blockedStateSQLRepositoryImpl) AffectedByDeletion(ctx context.Context, deletedIssues, deletedWisps []string) (domain.AffectedIDs, error) {
	if len(deletedIssues) == 0 && len(deletedWisps) == 0 {
		return domain.AffectedIDs{}, nil
	}

	issueSeen := make(map[string]bool, len(deletedIssues))
	wispSeen := make(map[string]bool, len(deletedWisps))
	for _, id := range deletedIssues {
		issueSeen[id] = true
	}
	for _, id := range deletedWisps {
		wispSeen[id] = true
	}
	var issueSeed, wispSeed []string

	if err := r.loadBlockingDependersForIDs(ctx, "depends_on_issue_id", deletedIssues, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}
	if err := r.loadBlockingDependersForIDs(ctx, "depends_on_wisp_id", deletedWisps, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}

	if err := r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_issue_id", deletedIssues, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}
	if err := r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_wisp_id", deletedWisps, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
		return domain.AffectedIDs{}, err
	}
	for _, id := range deletedIssues {
		if err := r.loadWaitersWhoseSpawnerIsParentOf(ctx, id, false, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
			return domain.AffectedIDs{}, err
		}
	}
	for _, id := range deletedWisps {
		if err := r.loadWaitersWhoseSpawnerIsParentOf(ctx, id, true, &issueSeed, issueSeen, &wispSeed, wispSeen); err != nil {
			return domain.AffectedIDs{}, err
		}
	}

	childExpansions := []struct {
		depTable, parentCol string
		parentIDs           []string
		seed                *[]string
		seen                map[string]bool
	}{
		{"dependencies", "depends_on_issue_id", deletedIssues, &issueSeed, issueSeen},
		{"wisp_dependencies", "depends_on_issue_id", deletedIssues, &wispSeed, wispSeen},
		{"dependencies", "depends_on_wisp_id", deletedWisps, &issueSeed, issueSeen},
		{"wisp_dependencies", "depends_on_wisp_id", deletedWisps, &wispSeed, wispSeen},
	}
	for _, w := range childExpansions {
		if err := r.appendChildren(ctx, w.depTable, w.parentCol, w.parentIDs, w.seen, w.seed); err != nil {
			return domain.AffectedIDs{}, err
		}
	}

	return r.expandByParentChildDescendants(ctx, issueSeed, wispSeed, issueSeen, wispSeen)
}

func recomputeIssuesPass(ctx context.Context, runner Runner, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return runMarkUnmarkBatched(ctx, runner, markBlockedTemplateForIssues(), unmarkBlockedTemplateForIssues(), ids)
}

func recomputeWispsPass(ctx context.Context, runner Runner, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return runMarkUnmarkBatched(ctx, runner, markBlockedTemplateForWisps(), unmarkBlockedTemplateForWisps(), ids)
}

func markIssuesPass(ctx context.Context, runner Runner, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return runMarkBatched(ctx, runner, markBlockedTemplateForIssues(), ids)
}

func markWispsPass(ctx context.Context, runner Runner, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return runMarkBatched(ctx, runner, markBlockedTemplateForWisps(), ids)
}

func markBlockedTemplateForIssues() string {
	return fmt.Sprintf(`
		UPDATE issues i SET i.is_blocked = 1
		WHERE i.id IN (%%s)
		  AND i.is_blocked = 0
		  AND i.status <> 'closed' AND i.status <> 'pinned'
		  AND (
		    EXISTS (
		      SELECT 1 FROM dependencies d
		      JOIN issues t ON t.id = d.depends_on_issue_id
		      WHERE d.issue_id = i.id
		        AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		        AND t.status <> 'closed' AND t.status <> 'pinned'
		    )
		    OR EXISTS (
		      SELECT 1 FROM dependencies d
		      JOIN wisps t ON t.id = d.depends_on_wisp_id
		      WHERE d.issue_id = i.id
		        AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		        AND t.status <> 'closed' AND t.status <> 'pinned'
		    )
		    OR EXISTS (
		      SELECT 1 FROM dependencies d
		      JOIN issues p ON p.id = d.depends_on_issue_id
		      WHERE d.issue_id = i.id
		        AND d.type = 'parent-child'
		        AND p.is_blocked = 1
		    )
		    OR EXISTS (
		      SELECT 1 FROM dependencies d
		      JOIN wisps p ON p.id = d.depends_on_wisp_id
		      WHERE d.issue_id = i.id
		        AND d.type = 'parent-child'
		        AND p.is_blocked = 1
		    )
		    OR EXISTS (
		      SELECT 1 FROM dependencies d
		      WHERE d.issue_id = i.id AND d.type = 'waits-for'
		        AND (%s)
		    )
		  )
	`, waitsForGateBlockedSQL)
}

func unmarkBlockedTemplateForIssues() string {
	return fmt.Sprintf(`
		UPDATE issues i SET i.is_blocked = 0
		WHERE i.id IN (%%s)
		  AND i.is_blocked = 1
		  AND (
		    i.status = 'closed' OR i.status = 'pinned'
		    OR (
		      NOT EXISTS (
		        SELECT 1 FROM dependencies d
		        JOIN issues t ON t.id = d.depends_on_issue_id
		        WHERE d.issue_id = i.id
		          AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		          AND t.status <> 'closed' AND t.status <> 'pinned'
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM dependencies d
		        JOIN wisps t ON t.id = d.depends_on_wisp_id
		        WHERE d.issue_id = i.id
		          AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		          AND t.status <> 'closed' AND t.status <> 'pinned'
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM dependencies d
		        JOIN issues p ON p.id = d.depends_on_issue_id
		        WHERE d.issue_id = i.id
		          AND d.type = 'parent-child'
		          AND p.is_blocked = 1
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM dependencies d
		        JOIN wisps p ON p.id = d.depends_on_wisp_id
		        WHERE d.issue_id = i.id
		          AND d.type = 'parent-child'
		          AND p.is_blocked = 1
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM dependencies d
		        WHERE d.issue_id = i.id AND d.type = 'waits-for'
		          AND (%s)
		      )
		    )
		  )
	`, waitsForGateBlockedSQL)
}

func markBlockedTemplateForWisps() string {
	return fmt.Sprintf(`
		UPDATE wisps w SET w.is_blocked = 1
		WHERE w.id IN (%%s)
		  AND w.is_blocked = 0
		  AND w.status <> 'closed' AND w.status <> 'pinned'
		  AND (
		    EXISTS (
		      SELECT 1 FROM wisp_dependencies d
		      JOIN issues t ON t.id = d.depends_on_issue_id
		      WHERE d.issue_id = w.id
		        AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		        AND t.status <> 'closed' AND t.status <> 'pinned'
		    )
		    OR EXISTS (
		      SELECT 1 FROM wisp_dependencies d
		      JOIN wisps t ON t.id = d.depends_on_wisp_id
		      WHERE d.issue_id = w.id
		        AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		        AND t.status <> 'closed' AND t.status <> 'pinned'
		    )
		    OR EXISTS (
		      SELECT 1 FROM wisp_dependencies d
		      JOIN issues p ON p.id = d.depends_on_issue_id
		      WHERE d.issue_id = w.id
		        AND d.type = 'parent-child'
		        AND p.is_blocked = 1
		    )
		    OR EXISTS (
		      SELECT 1 FROM wisp_dependencies d
		      JOIN wisps p ON p.id = d.depends_on_wisp_id
		      WHERE d.issue_id = w.id
		        AND d.type = 'parent-child'
		        AND p.is_blocked = 1
		    )
		    OR EXISTS (
		      SELECT 1 FROM wisp_dependencies d
		      WHERE d.issue_id = w.id AND d.type = 'waits-for'
		        AND (%s)
		    )
		  )
	`, waitsForGateBlockedSQL)
}

func unmarkBlockedTemplateForWisps() string {
	return fmt.Sprintf(`
		UPDATE wisps w SET w.is_blocked = 0
		WHERE w.id IN (%%s)
		  AND w.is_blocked = 1
		  AND (
		    w.status = 'closed' OR w.status = 'pinned'
		    OR (
		      NOT EXISTS (
		        SELECT 1 FROM wisp_dependencies d
		        JOIN issues t ON t.id = d.depends_on_issue_id
		        WHERE d.issue_id = w.id
		          AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		          AND t.status <> 'closed' AND t.status <> 'pinned'
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM wisp_dependencies d
		        JOIN wisps t ON t.id = d.depends_on_wisp_id
		        WHERE d.issue_id = w.id
		          AND (d.type = 'blocks' OR d.type = 'conditional-blocks')
		          AND t.status <> 'closed' AND t.status <> 'pinned'
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM wisp_dependencies d
		        JOIN issues p ON p.id = d.depends_on_issue_id
		        WHERE d.issue_id = w.id
		          AND d.type = 'parent-child'
		          AND p.is_blocked = 1
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM wisp_dependencies d
		        JOIN wisps p ON p.id = d.depends_on_wisp_id
		        WHERE d.issue_id = w.id
		          AND d.type = 'parent-child'
		          AND p.is_blocked = 1
		      )
		      AND NOT EXISTS (
		        SELECT 1 FROM wisp_dependencies d
		        WHERE d.issue_id = w.id AND d.type = 'waits-for'
		          AND (%s)
		      )
		    )
		  )
	`, waitsForGateBlockedSQL)
}

//nolint:gosec // G201: templates are constant; only IN-clause placeholders are formatted in.
func runMarkUnmarkBatched(ctx context.Context, runner Runner, markTmpl, unmarkTmpl string, ids []string) (int64, error) {
	var changed int64
	for start := 0; start < len(ids); start += queryBatchSize {
		end := start + queryBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		placeholders, args := buildInPlaceholders(ids[start:end])

		res, err := runner.ExecContext(ctx, fmt.Sprintf(markTmpl, placeholders), args...)
		if err != nil {
			return changed, fmt.Errorf("recompute is_blocked (mark): %w", err)
		}
		n, _ := res.RowsAffected()
		changed += n

		res, err = runner.ExecContext(ctx, fmt.Sprintf(unmarkTmpl, placeholders), args...)
		if err != nil {
			return changed, fmt.Errorf("recompute is_blocked (unmark): %w", err)
		}
		n, _ = res.RowsAffected()
		changed += n
	}
	return changed, nil
}

//nolint:gosec // G201: targetCol is one of two constant column names.
func (r *blockedStateSQLRepositoryImpl) loadBlockingDependersForIDs(
	ctx context.Context,
	targetCol string, ids []string,
	issueSeed *[]string, issueSeen map[string]bool,
	wispSeed *[]string, wispSeen map[string]bool,
) error {
	if len(ids) == 0 {
		return nil
	}
	tables := []struct {
		table  string
		seed   *[]string
		seen   map[string]bool
		errCtx string
	}{
		{"dependencies", issueSeed, issueSeen, "load issue dependers"},
		{"wisp_dependencies", wispSeed, wispSeen, "load wisp dependers"},
	}
	for _, id := range ids {
		for _, t := range tables {
			query := fmt.Sprintf(`
				SELECT issue_id FROM %s
				WHERE %s = ?
				  AND (type = 'blocks' OR type = 'conditional-blocks')
			`, t.table, targetCol)
			rows, err := r.runner.QueryContext(ctx, query, id)
			if err != nil {
				return fmt.Errorf("%s: query: %w", t.errCtx, err)
			}
			for rows.Next() {
				var dependerID string
				if err := rows.Scan(&dependerID); err != nil {
					_ = rows.Close()
					return fmt.Errorf("%s: scan: %w", t.errCtx, err)
				}
				if !t.seen[dependerID] {
					t.seen[dependerID] = true
					*t.seed = append(*t.seed, dependerID)
				}
			}
			_ = rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("%s: rows: %w", t.errCtx, err)
			}
		}
	}
	return nil
}

func (r *blockedStateSQLRepositoryImpl) loadWaitersWhoseSpawnerIsParentOf(
	ctx context.Context,
	childID string, childIsWisp bool,
	issueSeed *[]string, issueSeen map[string]bool,
	wispSeed *[]string, wispSeen map[string]bool,
) error {
	depTable := "dependencies"
	if childIsWisp {
		depTable = "wisp_dependencies"
	}
	//nolint:gosec // G201: depTable is one of two constant values.
	rows, err := r.runner.QueryContext(ctx, fmt.Sprintf(`
		SELECT depends_on_issue_id, depends_on_wisp_id
		FROM %s
		WHERE issue_id = ? AND type = 'parent-child'
	`, depTable), childID)
	if err != nil {
		return fmt.Errorf("waiters on parent of %s: load parents: %w", childID, err)
	}
	var issueParentIDs, wispParentIDs []string
	for rows.Next() {
		var ip, wp sql.NullString
		if err := rows.Scan(&ip, &wp); err != nil {
			_ = rows.Close()
			return fmt.Errorf("waiters on parent of %s: scan: %w", childID, err)
		}
		if ip.Valid {
			issueParentIDs = append(issueParentIDs, ip.String)
		}
		if wp.Valid {
			wispParentIDs = append(wispParentIDs, wp.String)
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("waiters on parent of %s: rows: %w", childID, err)
	}

	if len(issueParentIDs) > 0 {
		if err := r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_issue_id", issueParentIDs, issueSeed, issueSeen, wispSeed, wispSeen); err != nil {
			return err
		}
	}
	if len(wispParentIDs) > 0 {
		if err := r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_wisp_id", wispParentIDs, issueSeed, issueSeen, wispSeed, wispSeen); err != nil {
			return err
		}
	}
	return nil
}

func (r *blockedStateSQLRepositoryImpl) loadWaitersOnSpawnerIDs(
	ctx context.Context,
	spawnerIDs []string,
	issueSeed *[]string, issueSeen map[string]bool,
	wispSeed *[]string, wispSeen map[string]bool,
) error {
	if err := r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_issue_id", spawnerIDs, issueSeed, issueSeen, wispSeed, wispSeen); err != nil {
		return err
	}
	return r.loadWaitersOnSpawnerIDsByCol(ctx, "depends_on_wisp_id", spawnerIDs, issueSeed, issueSeen, wispSeed, wispSeen)
}

//nolint:gosec // G201: targetCol is one of two constant column names.
func (r *blockedStateSQLRepositoryImpl) loadWaitersOnSpawnerIDsByCol(
	ctx context.Context,
	targetCol string, spawnerIDs []string,
	issueSeed *[]string, issueSeen map[string]bool,
	wispSeed *[]string, wispSeen map[string]bool,
) error {
	if len(spawnerIDs) == 0 {
		return nil
	}
	tables := []struct {
		table  string
		seed   *[]string
		seen   map[string]bool
		errCtx string
	}{
		{"dependencies", issueSeed, issueSeen, "load issue waiters"},
		{"wisp_dependencies", wispSeed, wispSeen, "load wisp waiters"},
	}
	for _, spawnerID := range spawnerIDs {
		for _, t := range tables {
			query := fmt.Sprintf(`
				SELECT issue_id FROM %s
				WHERE type = 'waits-for' AND %s = ?
			`, t.table, targetCol)
			rows, err := r.runner.QueryContext(ctx, query, spawnerID)
			if err != nil {
				return fmt.Errorf("%s: query: %w", t.errCtx, err)
			}
			for rows.Next() {
				var waiterID string
				if err := rows.Scan(&waiterID); err != nil {
					_ = rows.Close()
					return fmt.Errorf("%s: scan: %w", t.errCtx, err)
				}
				if !t.seen[waiterID] {
					t.seen[waiterID] = true
					*t.seed = append(*t.seed, waiterID)
				}
			}
			_ = rows.Close()
			if err := rows.Err(); err != nil {
				return fmt.Errorf("%s: rows: %w", t.errCtx, err)
			}
		}
	}
	return nil
}

func (r *blockedStateSQLRepositoryImpl) expandByParentChildDescendants(
	ctx context.Context,
	issueSeed, wispSeed []string,
	issueSeen, wispSeen map[string]bool,
) (domain.AffectedIDs, error) {
	issueQueue := issueSeed
	wispQueue := wispSeed
	issueHead, wispHead := 0, 0

	for issueHead < len(issueQueue) || wispHead < len(wispQueue) {
		if issueHead < len(issueQueue) {
			end := issueHead + queryBatchSize
			if end > len(issueQueue) {
				end = len(issueQueue)
			}
			batch := issueQueue[issueHead:end]
			issueHead = end

			if err := r.appendChildren(ctx, "dependencies", "depends_on_issue_id", batch, issueSeen, &issueQueue); err != nil {
				return domain.AffectedIDs{}, err
			}
			if err := r.appendChildren(ctx, "wisp_dependencies", "depends_on_issue_id", batch, wispSeen, &wispQueue); err != nil {
				return domain.AffectedIDs{}, err
			}
		}
		if wispHead < len(wispQueue) {
			end := wispHead + queryBatchSize
			if end > len(wispQueue) {
				end = len(wispQueue)
			}
			batch := wispQueue[wispHead:end]
			wispHead = end

			if err := r.appendChildren(ctx, "dependencies", "depends_on_wisp_id", batch, issueSeen, &issueQueue); err != nil {
				return domain.AffectedIDs{}, err
			}
			if err := r.appendChildren(ctx, "wisp_dependencies", "depends_on_wisp_id", batch, wispSeen, &wispQueue); err != nil {
				return domain.AffectedIDs{}, err
			}
		}
	}
	return domain.AffectedIDs{IssueIDs: issueQueue, WispIDs: wispQueue}, nil
}

//nolint:gosec // G201: depTable and parentCol come from constant call sites.
func (r *blockedStateSQLRepositoryImpl) appendChildren(
	ctx context.Context,
	depTable, parentCol string,
	parentIDs []string,
	seen map[string]bool, queue *[]string,
) error {
	if len(parentIDs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`
		SELECT issue_id FROM %s
		WHERE type = 'parent-child'
		  AND %s = ?
	`, depTable, parentCol)
	for _, parentID := range parentIDs {
		rows, err := r.runner.QueryContext(ctx, query, parentID)
		if err != nil {
			return fmt.Errorf("expand children from %s on %s: %w", depTable, parentCol, err)
		}
		for rows.Next() {
			var childID string
			if err := rows.Scan(&childID); err != nil {
				_ = rows.Close()
				return fmt.Errorf("expand children: scan: %w", err)
			}
			if !seen[childID] {
				seen[childID] = true
				*queue = append(*queue, childID)
			}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("expand children: rows: %w", err)
		}
	}
	return nil
}

//nolint:gosec // G201: templates are constant; only IN-clause placeholders are formatted in.
func runMarkBatched(ctx context.Context, runner Runner, markTmpl string, ids []string) (int64, error) {
	var changed int64
	for start := 0; start < len(ids); start += queryBatchSize {
		end := start + queryBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		placeholders, args := buildInPlaceholders(ids[start:end])

		res, err := runner.ExecContext(ctx, fmt.Sprintf(markTmpl, placeholders), args...)
		if err != nil {
			return changed, fmt.Errorf("mark is_blocked: %w", err)
		}
		n, _ := res.RowsAffected()
		changed += n
	}
	return changed, nil
}
