package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage/dberrors"
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func NewDependencySQLRepository(runner Runner) domain.DependencySQLRepository {
	return &dependencySQLRepositoryImpl{
		runner:  runner,
		blocked: NewBlockedStateSQLRepository(runner),
	}
}

type dependencySQLRepositoryImpl struct {
	runner  Runner
	blocked domain.BlockedStateSQLRepository
}

var _ domain.DependencySQLRepository = (*dependencySQLRepositoryImpl)(nil)

const depTargetExpr = "COALESCE(depends_on_issue_id, depends_on_wisp_id, depends_on_external)"

const depSelectColumns = "issue_id, " + depTargetExpr + " AS depends_on_id, type, created_at, created_by, metadata, thread_id"

func pickDepTable(useWisps bool) string {
	if useWisps {
		return "wisp_dependencies"
	}
	return "dependencies"
}

func (r *dependencySQLRepositoryImpl) pickDepTargetColumn(ctx context.Context, dependsOnID string) (string, error) {
	if strings.HasPrefix(dependsOnID, "external:") {
		return "depends_on_external", nil
	}
	var probe int
	err := r.runner.QueryRowContext(ctx, "SELECT 1 FROM wisps WHERE id = ? LIMIT 1", dependsOnID).Scan(&probe)
	switch {
	case err == nil:
		return "depends_on_wisp_id", nil
	case errors.Is(err, sql.ErrNoRows):
		return "depends_on_issue_id", nil
	case dberrors.IsTableNotExist(err):
		return "depends_on_issue_id", nil
	default:
		return "", fmt.Errorf("classify dep target %s: %w", dependsOnID, err)
	}
}

func (r *dependencySQLRepositoryImpl) Insert(ctx context.Context, dep *types.Dependency, actor string, opts domain.DepInsertOpts) error {
	if dep == nil {
		return errors.New("db: DependencySQLRepository.Insert: dep must not be nil")
	}
	if dep.IssueID == "" {
		return errors.New("db: DependencySQLRepository.Insert: IssueID must not be empty")
	}
	if dep.DependsOnID == "" {
		return errors.New("db: DependencySQLRepository.Insert: DependsOnID must not be empty")
	}
	if dep.IssueID == dep.DependsOnID {
		return fmt.Errorf("db: DependencySQLRepository.Insert: %s cannot depend on itself", dep.IssueID)
	}

	metadata := dep.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	table := pickDepTable(opts.UseWispsTable)

	var existingType string
	err := r.runner.QueryRowContext(ctx,
		//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
		fmt.Sprintf("SELECT type FROM %s WHERE issue_id = ? AND %s = ?", table, depTargetExpr),
		dep.IssueID, dep.DependsOnID,
	).Scan(&existingType)
	switch {
	case err == nil:
		if existingType == string(dep.Type) {
			//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
			if _, err := r.runner.ExecContext(ctx,
				fmt.Sprintf("UPDATE %s SET metadata = ? WHERE issue_id = ? AND %s = ?", table, depTargetExpr),
				metadata, dep.IssueID, dep.DependsOnID,
			); err != nil {
				return fmt.Errorf("db: DependencySQLRepository.Insert: refresh metadata: %w", err)
			}
			return nil
		}
		return fmt.Errorf("db: DependencySQLRepository.Insert: %s -> %s already exists with type %q (requested %q)",
			dep.IssueID, dep.DependsOnID, existingType, dep.Type)
	case errors.Is(err, sql.ErrNoRows):
	default:
		return fmt.Errorf("db: DependencySQLRepository.Insert: check existing: %w", err)
	}

	targetCol, err := r.pickDepTargetColumn(ctx, dep.DependsOnID)
	if err != nil {
		return fmt.Errorf("db: DependencySQLRepository.Insert: %w", err)
	}

	//nolint:gosec // G201: table is one of two hardcoded constants; targetCol is from pickDepTargetColumn
	if _, err := r.runner.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (issue_id, %s, type, created_at, created_by, metadata, thread_id)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, table, targetCol),
		dep.IssueID, dep.DependsOnID, string(dep.Type),
		time.Now().UTC(), actor, metadata, dep.ThreadID,
	); err != nil {
		return fmt.Errorf("db: DependencySQLRepository.Insert: %w", err)
	}
	return nil
}

func (r *dependencySQLRepositoryImpl) HasCycle(ctx context.Context, issueID, dependsOnID string) (bool, error) {
	if issueID == "" || dependsOnID == "" {
		return false, errors.New("db: DependencySQLRepository.HasCycle: issueID and dependsOnID must not be empty")
	}

	var one int
	err := r.runner.QueryRowContext(ctx, `
		SELECT 1 FROM dependencies
		WHERE issue_id = ? AND depends_on_issue_id = ?
		  AND type IN ('blocks', 'conditional-blocks')
		LIMIT 1
	`, dependsOnID, issueID).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("db: DependencySQLRepository.HasCycle: direct probe (dependencies): %w", err)
	}
	err = r.runner.QueryRowContext(ctx, `
		SELECT 1 FROM wisp_dependencies
		WHERE issue_id = ? AND depends_on_issue_id = ?
		  AND type IN ('blocks', 'conditional-blocks')
		LIMIT 1
	`, dependsOnID, issueID).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case !errors.Is(err, sql.ErrNoRows):
		return false, fmt.Errorf("db: DependencySQLRepository.HasCycle: direct probe (wisp_dependencies): %w", err)
	}

	var count int
	err = r.runner.QueryRowContext(ctx, `
		WITH RECURSIVE reachable(node) AS (
			SELECT ?
			UNION
			SELECT d.depends_on_issue_id FROM (
				SELECT issue_id, depends_on_issue_id, type FROM dependencies
				UNION ALL
				SELECT issue_id, depends_on_issue_id, type FROM wisp_dependencies
			) d
			JOIN reachable r ON d.issue_id = r.node
			WHERE d.type IN ('blocks', 'conditional-blocks')
			  AND d.depends_on_issue_id IS NOT NULL
		)
		SELECT COUNT(*) FROM reachable WHERE node = ?
	`, dependsOnID, issueID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("db: DependencySQLRepository.HasCycle: %w", err)
	}
	return count > 0, nil
}

func (r *dependencySQLRepositoryImpl) ListByIssueIDs(ctx context.Context, issueIDs []string, opts domain.DepListOpts) (domain.DepBulkResult, error) {
	result := domain.DepBulkResult{
		Outgoing: make(map[string][]*types.Dependency),
		Incoming: make(map[string][]*types.Dependency),
	}
	if len(issueIDs) == 0 {
		return result, nil
	}

	idPlaceholders, idArgs := buildInPlaceholders(issueIDs)
	typeWhere, typeArgs := buildTypeFilter(opts.Types)
	table := pickDepTable(opts.UseWispsTable)

	if opts.Direction == domain.DepDirectionBoth || opts.Direction == domain.DepDirectionOut {
		//nolint:gosec // G201: table and depSelectColumns are hardcoded
		q := fmt.Sprintf(
			`SELECT %s FROM %s WHERE issue_id IN (%s)%s ORDER BY issue_id`,
			depSelectColumns, table, idPlaceholders, typeWhere,
		)
		args := combineArgs(idArgs, typeArgs)
		if err := r.queryDeps(ctx, q, args, result.Outgoing, true); err != nil {
			return domain.DepBulkResult{}, fmt.Errorf("db: DependencySQLRepository.ListByIssueIDs (out): %w", err)
		}
	}

	if opts.Direction == domain.DepDirectionBoth || opts.Direction == domain.DepDirectionIn {
		//nolint:gosec // G201: table, depSelectColumns, depTargetExpr are hardcoded
		q := fmt.Sprintf(
			`SELECT %s FROM %s WHERE %s IN (%s)%s ORDER BY issue_id`,
			depSelectColumns, table, depTargetExpr, idPlaceholders, typeWhere,
		)
		args := combineArgs(idArgs, typeArgs)
		if err := r.queryDeps(ctx, q, args, result.Incoming, false); err != nil {
			return domain.DepBulkResult{}, fmt.Errorf("db: DependencySQLRepository.ListByIssueIDs (in): %w", err)
		}
	}

	return result, nil
}

func (r *dependencySQLRepositoryImpl) CountsByIssueIDs(ctx context.Context, issueIDs []string, opts domain.DepCountsOpts) (map[string]*types.DependencyCounts, error) {
	result := make(map[string]*types.DependencyCounts)
	if len(issueIDs) == 0 {
		return result, nil
	}
	for _, id := range issueIDs {
		result[id] = &types.DependencyCounts{}
	}

	idPlaceholders, idArgs := buildInPlaceholders(issueIDs)
	table := pickDepTable(opts.UseWispsTable)

	//nolint:gosec // G201: table is one of two hardcoded constants
	outQ := fmt.Sprintf(
		`SELECT issue_id, COUNT(*) FROM %s WHERE issue_id IN (%s) AND type = 'blocks' GROUP BY issue_id`,
		table, idPlaceholders,
	)
	if err := scanCounts(ctx, r.runner, outQ, idArgs, result, func(c *types.DependencyCounts, n int) { c.DependencyCount = n }); err != nil {
		return nil, fmt.Errorf("db: DependencySQLRepository.CountsByIssueIDs (out): %w", err)
	}

	//nolint:gosec // G201: table and depTargetExpr are hardcoded
	inQ := fmt.Sprintf(
		`SELECT %s AS depends_on_id, COUNT(*) FROM %s WHERE %s IN (%s) AND type = 'blocks' GROUP BY %s`,
		depTargetExpr, table, depTargetExpr, idPlaceholders, depTargetExpr,
	)
	if err := scanCounts(ctx, r.runner, inQ, idArgs, result, func(c *types.DependencyCounts, n int) { c.DependentCount = n }); err != nil {
		return nil, fmt.Errorf("db: DependencySQLRepository.CountsByIssueIDs (in): %w", err)
	}

	return result, nil
}

func (r *dependencySQLRepositoryImpl) GetBlockingInfo(ctx context.Context, issueIDs []string, opts domain.DepListOpts) (domain.BlockingInfo, error) {
	info := domain.BlockingInfo{
		BlockedBy: make(map[string][]string),
		Blocks:    make(map[string][]string),
		Parent:    make(map[string]string),
	}
	if len(issueIDs) == 0 {
		return info, nil
	}

	table := pickDepTable(opts.UseWispsTable)
	idPlaceholders, idArgs := buildInPlaceholders(issueIDs)

	//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
	outQ := fmt.Sprintf(
		"SELECT issue_id, %s AS depends_on_id, type FROM %s WHERE issue_id IN (%s) AND type IN ('blocks', 'parent-child')",
		depTargetExpr, table, idPlaceholders,
	)
	outRows, err := r.scanBlockingRows(ctx, outQ, idArgs)
	if err != nil {
		return domain.BlockingInfo{}, fmt.Errorf("db: DependencySQLRepository.GetBlockingInfo: outbound: %w", err)
	}

	//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
	inQ := fmt.Sprintf(
		"SELECT issue_id, %s AS depends_on_id, type FROM %s WHERE %s IN (%s) AND type = 'blocks'",
		depTargetExpr, table, depTargetExpr, idPlaceholders,
	)
	inRows, err := r.scanBlockingRows(ctx, inQ, idArgs)
	if err != nil {
		return domain.BlockingInfo{}, fmt.Errorf("db: DependencySQLRepository.GetBlockingInfo: inbound: %w", err)
	}

	statusIDs := make(map[string]struct{})
	for _, row := range outRows {
		statusIDs[row.dependsOnID] = struct{}{}
	}
	for _, row := range inRows {
		statusIDs[row.dependsOnID] = struct{}{}
	}
	statusByID, err := r.loadStatusByID(ctx, statusIDs)
	if err != nil {
		return domain.BlockingInfo{}, fmt.Errorf("db: DependencySQLRepository.GetBlockingInfo: status lookup: %w", err)
	}

	for _, row := range outRows {
		if statusByID[row.dependsOnID] == types.StatusClosed {
			continue
		}
		if row.depType == "parent-child" {
			info.Parent[row.issueID] = row.dependsOnID
		} else {
			info.BlockedBy[row.issueID] = append(info.BlockedBy[row.issueID], row.dependsOnID)
		}
	}
	for _, row := range inRows {
		if statusByID[row.dependsOnID] == types.StatusClosed {
			continue
		}
		info.Blocks[row.dependsOnID] = append(info.Blocks[row.dependsOnID], row.issueID)
	}

	return info, nil
}

func (r *dependencySQLRepositoryImpl) GetBlockingInfoAcrossIssuesAndWisps(ctx context.Context, issueIDs []string) (domain.BlockingInfo, error) {
	perm, err := r.GetBlockingInfo(ctx, issueIDs, domain.DepListOpts{UseWispsTable: false})
	if err != nil {
		return domain.BlockingInfo{}, err
	}
	wisp, err := r.GetBlockingInfo(ctx, issueIDs, domain.DepListOpts{UseWispsTable: true})
	if err != nil {
		if !dberrors.IsTableNotExist(err) {
			return domain.BlockingInfo{}, err
		}
		wisp = domain.BlockingInfo{
			BlockedBy: map[string][]string{},
			Blocks:    map[string][]string{},
			Parent:    map[string]string{},
		}
	}
	for k, v := range wisp.BlockedBy {
		perm.BlockedBy[k] = append(perm.BlockedBy[k], v...)
	}
	for k, v := range wisp.Blocks {
		perm.Blocks[k] = append(perm.Blocks[k], v...)
	}
	for k, v := range wisp.Parent {
		if _, ok := perm.Parent[k]; !ok {
			perm.Parent[k] = v
		}
	}
	return perm, nil
}

type blockingRow struct {
	issueID, dependsOnID, depType string
}

func (r *dependencySQLRepositoryImpl) scanBlockingRows(ctx context.Context, q string, args []any) ([]blockingRow, error) {
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []blockingRow
	for rows.Next() {
		var row blockingRow
		if err := rows.Scan(&row.issueID, &row.dependsOnID, &row.depType); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (r *dependencySQLRepositoryImpl) loadStatusByID(ctx context.Context, idSet map[string]struct{}) (map[string]types.Status, error) {
	statusByID := make(map[string]types.Status, len(idSet))
	if len(idSet) == 0 {
		return statusByID, nil
	}
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}
	placeholders, args := buildInPlaceholders(ids)
	sourceByID := make(map[string]string, len(idSet))
	for _, table := range []string{"issues", "wisps"} {
		//nolint:gosec // G201: table is a hardcoded constant
		q := fmt.Sprintf("SELECT id, status FROM %s WHERE id IN (%s)", table, placeholders)
		if err := r.scanStatusRows(ctx, q, args, table, statusByID, sourceByID); err != nil {
			return nil, err
		}
	}
	return statusByID, nil
}

func (r *dependencySQLRepositoryImpl) scanStatusRows(ctx context.Context, q string, args []any, table string, statusByID map[string]types.Status, sourceByID map[string]string) error {
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		if dberrors.IsTableNotExist(err) {
			return nil
		}
		return fmt.Errorf("status from %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var status types.Status
		if err := rows.Scan(&id, &status); err != nil {
			return fmt.Errorf("status from %s: scan: %w", table, err)
		}
		if existing, dup := sourceByID[id]; dup {
			return fmt.Errorf("status id %q exists in both %s and %s", id, existing, table)
		}
		sourceByID[id] = table
		statusByID[id] = status
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("status rows from %s: %w", table, err)
	}
	return nil
}

func (r *dependencySQLRepositoryImpl) queryDeps(ctx context.Context, q string, args []any, into map[string][]*types.Dependency, keyByIssueID bool) error {
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var d types.Dependency
		var typ string
		var createdBy, metadata, threadID sql.NullString
		var createdAt sql.NullTime
		if err := rows.Scan(&d.IssueID, &d.DependsOnID, &typ, &createdAt, &createdBy, &metadata, &threadID); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		d.Type = types.DependencyType(typ)
		if createdAt.Valid {
			d.CreatedAt = createdAt.Time
		}
		if createdBy.Valid {
			d.CreatedBy = createdBy.String
		}
		if metadata.Valid && metadata.String != "" && metadata.String != "{}" {
			d.Metadata = metadata.String
		}
		if threadID.Valid {
			d.ThreadID = threadID.String
		}
		dd := d
		var key string
		if keyByIssueID {
			key = d.IssueID
		} else {
			key = d.DependsOnID
		}
		into[key] = append(into[key], &dd)
	}
	return rows.Err()
}

func scanCounts(ctx context.Context, runner Runner, q string, args []any, into map[string]*types.DependencyCounts, assign func(c *types.DependencyCounts, n int)) error {
	rows, err := runner.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		if c, ok := into[id]; ok {
			assign(c, n)
		}
	}
	return rows.Err()
}

func buildInPlaceholders[T ~string](values []T) (string, []any) {
	ph := make([]string, len(values))
	args := make([]any, len(values))
	for i, v := range values {
		ph[i] = "?"
		args[i] = string(v)
	}
	return strings.Join(ph, ","), args
}

func buildTypeFilter(depTypes []types.DependencyType) (string, []any) {
	if len(depTypes) == 0 {
		return "", nil
	}
	ph := make([]string, len(depTypes))
	args := make([]any, len(depTypes))
	for i, t := range depTypes {
		ph[i] = "?"
		args[i] = string(t)
	}
	return " AND type IN (" + strings.Join(ph, ",") + ")", args
}

func (r *dependencySQLRepositoryImpl) Remove(ctx context.Context, issueID, dependsOnID, actor string, opts domain.DepInsertOpts) error {
	_ = actor
	if issueID == "" || dependsOnID == "" {
		return errors.New("db: Remove: issueID and dependsOnID must not be empty")
	}

	table := pickDepTable(opts.UseWispsTable)

	var depType string
	//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
	row := r.runner.QueryRowContext(ctx,
		fmt.Sprintf("SELECT type FROM %s WHERE issue_id = ? AND %s = ?", table, depTargetExpr),
		issueID, dependsOnID,
	)
	if err := row.Scan(&depType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("db: Remove %s -> %s: lookup type: %w", issueID, dependsOnID, err)
	}

	//nolint:gosec // G201: table and depTargetExpr are hardcoded constants
	if _, err := r.runner.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE issue_id = ? AND %s = ?", table, depTargetExpr),
		issueID, dependsOnID,
	); err != nil {
		return fmt.Errorf("db: Remove %s -> %s: %w", issueID, dependsOnID, err)
	}

	affected, err := r.blocked.AffectedByDepChange(ctx, issueID, dependsOnID, types.DependencyType(depType), opts.UseWispsTable)
	if err != nil {
		return fmt.Errorf("db: Remove %s -> %s: affected by dep change: %w", issueID, dependsOnID, err)
	}
	if err := r.blocked.Recompute(ctx, affected); err != nil {
		return fmt.Errorf("db: Remove %s -> %s: recompute is_blocked: %w", issueID, dependsOnID, err)
	}
	return nil
}

func (r *dependencySQLRepositoryImpl) ListRecordsForIssue(ctx context.Context, issueID string, opts domain.DepListOpts) ([]*types.Dependency, error) {
	if issueID == "" {
		return nil, errors.New("db: ListRecordsForIssue: issueID must not be empty")
	}
	bulk, err := r.ListByIssueIDs(ctx, []string{issueID}, domain.DepListOpts{
		Types:         opts.Types,
		Direction:     domain.DepDirectionOut,
		UseWispsTable: opts.UseWispsTable,
	})
	if err != nil {
		return nil, fmt.Errorf("db: ListRecordsForIssue %s: %w", issueID, err)
	}
	return bulk.Outgoing[issueID], nil
}

func (r *dependencySQLRepositoryImpl) ListIssuesDependedOn(ctx context.Context, issueID string, opts domain.DepListOpts) ([]*types.Issue, error) {
	_ = opts
	if issueID == "" {
		return nil, errors.New("db: ListIssuesDependedOn: issueID must not be empty")
	}
	return r.fetchRelatedIssues(ctx, issueID, depRelationOutgoing)
}

func (r *dependencySQLRepositoryImpl) ListIssueDependents(ctx context.Context, issueID string, opts domain.DepListOpts) ([]*types.Issue, error) {
	_ = opts
	if issueID == "" {
		return nil, errors.New("db: ListIssueDependents: issueID must not be empty")
	}
	return r.fetchRelatedIssues(ctx, issueID, depRelationIncoming)
}

func (r *dependencySQLRepositoryImpl) ListDependentsWithMetadata(ctx context.Context, issueID string, opts domain.DepListOpts) ([]*types.IssueWithDependencyMetadata, error) {
	_ = opts
	if issueID == "" {
		return nil, errors.New("db: ListDependentsWithMetadata: issueID must not be empty")
	}

	var fromIssues, fromWisps []dependentMeta

	collect := func(depTable string, sink *[]dependentMeta) error {
		//nolint:gosec // G201: depTable is a hardcoded constant
		q := fmt.Sprintf(
			"SELECT issue_id, type FROM %s WHERE depends_on_issue_id = ? OR depends_on_wisp_id = ?",
			depTable,
		)
		rows, err := r.runner.QueryContext(ctx, q, issueID, issueID)
		if err != nil {
			return fmt.Errorf("query %s: %w", depTable, err)
		}
		for rows.Next() {
			var id, depType string
			if err := rows.Scan(&id, &depType); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan %s: %w", depTable, err)
			}
			*sink = append(*sink, dependentMeta{depID: id, depType: types.DependencyType(depType)})
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate %s: %w", depTable, err)
		}
		return nil
	}

	if err := collect("dependencies", &fromIssues); err != nil {
		return nil, fmt.Errorf("db: ListDependentsWithMetadata: %w", err)
	}
	if err := collect("wisp_dependencies", &fromWisps); err != nil {
		return nil, fmt.Errorf("db: ListDependentsWithMetadata: %w", err)
	}

	if len(fromIssues) == 0 && len(fromWisps) == 0 {
		return nil, nil
	}

	issueByID, err := r.fetchIssuesByIDs(ctx, "issues", depIDs(fromIssues))
	if err != nil {
		return nil, fmt.Errorf("db: ListDependentsWithMetadata: %w", err)
	}
	wispByID, err := r.fetchIssuesByIDs(ctx, "wisps", depIDs(fromWisps))
	if err != nil {
		return nil, fmt.Errorf("db: ListDependentsWithMetadata: %w", err)
	}

	var out []*types.IssueWithDependencyMetadata
	for _, m := range fromIssues {
		iss, ok := issueByID[m.depID]
		if !ok {
			continue
		}
		out = append(out, &types.IssueWithDependencyMetadata{
			Issue:          *iss,
			DependencyType: m.depType,
		})
	}
	for _, m := range fromWisps {
		iss, ok := wispByID[m.depID]
		if !ok {
			continue
		}
		out = append(out, &types.IssueWithDependencyMetadata{
			Issue:          *iss,
			DependencyType: m.depType,
		})
	}
	return out, nil
}

//nolint:gosec // G201: table is a hardcoded constant; placeholders contain only ? markers
func (r *dependencySQLRepositoryImpl) fetchIssuesByIDs(ctx context.Context, table string, ids []string) (map[string]*types.Issue, error) {
	out := make(map[string]*types.Issue, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	placeholders, args := buildInPlaceholders(ids)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE id IN (%s)", issueSelectColumns, table, placeholders)
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch from %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		iss, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		out[iss.ID] = iss
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", table, err)
	}
	return out, nil
}

type dependentMeta struct {
	depID   string
	depType types.DependencyType
}

func depIDs(meta []dependentMeta) []string {
	out := make([]string, 0, len(meta))
	seen := make(map[string]bool, len(meta))
	for _, m := range meta {
		if !seen[m.depID] {
			seen[m.depID] = true
			out = append(out, m.depID)
		}
	}
	return out
}

func (r *dependencySQLRepositoryImpl) IsBlocked(ctx context.Context, issueID string, opts domain.DepListOpts) (bool, []string, error) {
	_ = opts
	if issueID == "" {
		return false, nil, errors.New("db: IsBlocked: issueID must not be empty")
	}

	var (
		blocked bool
		found   bool
	)
	for _, table := range []string{"issues", "wisps"} {
		var b int
		//nolint:gosec // G201: table is a hardcoded constant
		err := r.runner.QueryRowContext(ctx, "SELECT is_blocked FROM "+table+" WHERE id = ?", issueID).Scan(&b)
		switch {
		case err == nil:
			blocked = b != 0
			found = true
		case errors.Is(err, sql.ErrNoRows):
			continue
		default:
			return false, nil, fmt.Errorf("db: IsBlocked: read is_blocked from %s: %w", table, err)
		}
		if found {
			break
		}
	}
	if !found || !blocked {
		return false, nil, nil
	}

	type depEdge struct {
		dependsOnID string
		depType     string
	}
	var edges []depEdge
	for _, depTable := range []string{"dependencies", "wisp_dependencies"} {
		//nolint:gosec // G201: depTable is a hardcoded constant
		q := fmt.Sprintf(
			"SELECT %s AS depends_on_id, type FROM %s WHERE issue_id = ? AND type IN ('blocks', 'waits-for', 'conditional-blocks')",
			depTargetExpr, depTable,
		)
		rows, err := r.runner.QueryContext(ctx, q, issueID)
		if err != nil {
			return false, nil, fmt.Errorf("db: IsBlocked: query %s: %w", depTable, err)
		}
		for rows.Next() {
			var e depEdge
			if err := rows.Scan(&e.dependsOnID, &e.depType); err != nil {
				_ = rows.Close()
				return false, nil, fmt.Errorf("db: IsBlocked: scan %s: %w", depTable, err)
			}
			edges = append(edges, e)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return false, nil, fmt.Errorf("db: IsBlocked: iterate %s: %w", depTable, err)
		}
	}

	if len(edges) == 0 {
		return true, nil, nil
	}

	blockerIDSet := make(map[string]struct{}, len(edges))
	for _, e := range edges {
		blockerIDSet[e.dependsOnID] = struct{}{}
	}
	statusByID, err := r.loadStatusByID(ctx, blockerIDSet)
	if err != nil {
		return false, nil, fmt.Errorf("db: IsBlocked: status lookup: %w", err)
	}

	var blockers []string
	for _, e := range edges {
		status, ok := statusByID[e.dependsOnID]
		if !ok {
			continue
		}
		if status == types.StatusClosed || status == types.StatusPinned {
			continue
		}
		if e.depType != "blocks" {
			blockers = append(blockers, e.dependsOnID+" ("+e.depType+")")
		} else {
			blockers = append(blockers, e.dependsOnID)
		}
	}
	return true, blockers, nil
}

func (r *dependencySQLRepositoryImpl) ListNewlyUnblockedByClose(ctx context.Context, closedIssueID string, opts domain.DepListOpts) ([]*types.Issue, error) {
	_ = opts
	if closedIssueID == "" {
		return nil, errors.New("db: ListNewlyUnblockedByClose: closedIssueID must not be empty")
	}

	candidatesFromIssues := make(map[string]bool)
	candidatesFromWisps := make(map[string]bool)
	collect := func(depTable string, sink map[string]bool) error {
		//nolint:gosec // G201: depTable is a hardcoded constant
		q := fmt.Sprintf(
			"SELECT issue_id FROM %s WHERE (depends_on_issue_id = ? OR depends_on_wisp_id = ?) AND type = 'blocks'",
			depTable,
		)
		rows, err := r.runner.QueryContext(ctx, q, closedIssueID, closedIssueID)
		if err != nil {
			return fmt.Errorf("query %s: %w", depTable, err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan %s: %w", depTable, err)
			}
			sink[id] = true
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate %s: %w", depTable, err)
		}
		return nil
	}

	if err := collect("dependencies", candidatesFromIssues); err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: %w", err)
	}
	if err := collect("wisp_dependencies", candidatesFromWisps); err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: %w", err)
	}
	if len(candidatesFromIssues) == 0 && len(candidatesFromWisps) == 0 {
		return nil, nil
	}

	statusIDSet := make(map[string]struct{}, len(candidatesFromIssues)+len(candidatesFromWisps))
	for id := range candidatesFromIssues {
		statusIDSet[id] = struct{}{}
	}
	for id := range candidatesFromWisps {
		statusIDSet[id] = struct{}{}
	}
	statusByID, err := r.loadStatusByID(ctx, statusIDSet)
	if err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: candidate status: %w", err)
	}

	activeIssueCandidates := make([]string, 0, len(candidatesFromIssues))
	for id := range candidatesFromIssues {
		st, ok := statusByID[id]
		if !ok || st == types.StatusClosed || st == types.StatusPinned {
			continue
		}
		activeIssueCandidates = append(activeIssueCandidates, id)
	}
	activeWispCandidates := make([]string, 0, len(candidatesFromWisps))
	for id := range candidatesFromWisps {
		st, ok := statusByID[id]
		if !ok || st == types.StatusClosed || st == types.StatusPinned {
			continue
		}
		activeWispCandidates = append(activeWispCandidates, id)
	}
	sort.Strings(activeIssueCandidates)
	sort.Strings(activeWispCandidates)

	if len(activeIssueCandidates) == 0 && len(activeWispCandidates) == 0 {
		return nil, nil
	}

	stillBlocked, err := r.candidatesStillBlocked(ctx, "dependencies", activeIssueCandidates, closedIssueID)
	if err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: %w", err)
	}
	wispStillBlocked, err := r.candidatesStillBlocked(ctx, "wisp_dependencies", activeWispCandidates, closedIssueID)
	if err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: %w", err)
	}

	keepIssues := make([]string, 0, len(activeIssueCandidates))
	for _, id := range activeIssueCandidates {
		if !stillBlocked[id] {
			keepIssues = append(keepIssues, id)
		}
	}
	keepWisps := make([]string, 0, len(activeWispCandidates))
	for _, id := range activeWispCandidates {
		if !wispStillBlocked[id] {
			keepWisps = append(keepWisps, id)
		}
	}

	issueByID, err := r.fetchIssuesByIDs(ctx, "issues", keepIssues)
	if err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: hydrate issues: %w", err)
	}
	wispByID, err := r.fetchIssuesByIDs(ctx, "wisps", keepWisps)
	if err != nil {
		return nil, fmt.Errorf("db: ListNewlyUnblockedByClose: hydrate wisps: %w", err)
	}

	out := make([]*types.Issue, 0, len(keepIssues)+len(keepWisps))
	for _, id := range keepIssues {
		if iss, ok := issueByID[id]; ok {
			out = append(out, iss)
		}
	}
	for _, id := range keepWisps {
		if iss, ok := wispByID[id]; ok {
			out = append(out, iss)
		}
	}
	return out, nil
}

//nolint:gosec // G201: depTable is a hardcoded constant
func (r *dependencySQLRepositoryImpl) candidatesStillBlocked(ctx context.Context, depTable string, candidateIDs []string, closedIssueID string) (map[string]bool, error) {
	stillBlocked := make(map[string]bool)
	if len(candidateIDs) == 0 {
		return stillBlocked, nil
	}
	const batch = 200
	for start := 0; start < len(candidateIDs); start += batch {
		end := start + batch
		if end > len(candidateIDs) {
			end = len(candidateIDs)
		}
		placeholders, args := buildInPlaceholders(candidateIDs[start:end])
		q := fmt.Sprintf(
			"SELECT issue_id, %s AS blocker_id FROM %s WHERE issue_id IN (%s) AND type = 'blocks' AND %s != ?",
			depTargetExpr, depTable, placeholders, depTargetExpr,
		)
		queryArgs := append([]any{}, args...)
		queryArgs = append(queryArgs, closedIssueID)

		type pair struct {
			candidate, blocker string
		}
		var pairs []pair
		blockerSet := make(map[string]struct{})
		rows, err := r.runner.QueryContext(ctx, q, queryArgs...)
		if err != nil {
			return nil, fmt.Errorf("query %s remaining blockers: %w", depTable, err)
		}
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.candidate, &p.blocker); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan %s remaining blocker: %w", depTable, err)
			}
			pairs = append(pairs, p)
			blockerSet[p.blocker] = struct{}{}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate %s remaining blockers: %w", depTable, err)
		}

		statusByID, err := r.loadStatusByID(ctx, blockerSet)
		if err != nil {
			return nil, fmt.Errorf("status of remaining %s blockers: %w", depTable, err)
		}
		for _, p := range pairs {
			if stillBlocked[p.candidate] {
				continue
			}
			st, ok := statusByID[p.blocker]
			if ok && st != types.StatusClosed && st != types.StatusPinned {
				stillBlocked[p.candidate] = true
			}
		}
	}
	return stillBlocked, nil
}

type depRelationDirection int

const (
	depRelationOutgoing depRelationDirection = iota
	depRelationIncoming
)

//nolint:gosec // G201: all SQL fragments come from package constants
func (r *dependencySQLRepositoryImpl) fetchRelatedIssues(ctx context.Context, issueID string, dir depRelationDirection) ([]*types.Issue, error) {
	var (
		issueTargetClause string
		wispTargetClause  string
		filterCol         string
	)
	switch dir {
	case depRelationOutgoing:
		issueTargetClause = "depends_on_issue_id"
		wispTargetClause = "depends_on_wisp_id"
		filterCol = "issue_id"
	case depRelationIncoming:
		issueTargetClause = "issue_id"
		wispTargetClause = "issue_id"
		filterCol = "" // handled below per-table
	}

	var out []*types.Issue

	{
		var subqIssues string
		var args []any
		switch dir {
		case depRelationOutgoing:
			subqIssues = fmt.Sprintf(`
				SELECT %s FROM dependencies WHERE %s = ? AND %s IS NOT NULL
				UNION
				SELECT %s FROM wisp_dependencies WHERE %s = ? AND %s IS NOT NULL
			`, issueTargetClause, filterCol, issueTargetClause,
				issueTargetClause, filterCol, issueTargetClause)
			args = []any{issueID, issueID}
		case depRelationIncoming:
			subqIssues = `
				SELECT issue_id FROM dependencies
				WHERE depends_on_issue_id = ? OR depends_on_wisp_id = ?
			`
			args = []any{issueID, issueID}
		}

		q := fmt.Sprintf("SELECT %s FROM issues WHERE id IN (%s)", issueSelectColumns, subqIssues)
		rows, err := r.runner.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("db: fetchRelatedIssues (issues side): %w", err)
		}
		for rows.Next() {
			issue, err := scanIssue(rows)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("db: fetchRelatedIssues: scan issue: %w", err)
			}
			out = append(out, issue)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("db: fetchRelatedIssues: iterate issues: %w", err)
		}
	}

	{
		var subqWisps string
		var args []any
		switch dir {
		case depRelationOutgoing:
			subqWisps = fmt.Sprintf(`
				SELECT %s FROM dependencies WHERE %s = ? AND %s IS NOT NULL
				UNION
				SELECT %s FROM wisp_dependencies WHERE %s = ? AND %s IS NOT NULL
			`, wispTargetClause, filterCol, wispTargetClause,
				wispTargetClause, filterCol, wispTargetClause)
			args = []any{issueID, issueID}
		case depRelationIncoming:
			subqWisps = `
				SELECT issue_id FROM wisp_dependencies
				WHERE depends_on_issue_id = ? OR depends_on_wisp_id = ?
			`
			args = []any{issueID, issueID}
		}

		q := fmt.Sprintf("SELECT %s FROM wisps WHERE id IN (%s)", issueSelectColumns, subqWisps)
		rows, err := r.runner.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("db: fetchRelatedIssues (wisps side): %w", err)
		}
		for rows.Next() {
			issue, err := scanIssue(rows)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("db: fetchRelatedIssues: scan wisp: %w", err)
			}
			out = append(out, issue)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("db: fetchRelatedIssues: iterate wisps: %w", err)
		}
	}

	return out, nil
}

func combineArgs(a, b []any) []any {
	out := make([]any, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}
