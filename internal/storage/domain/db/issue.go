package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func NewIssueSQLRepository(runner Runner) domain.IssueSQLRepository {
	return &issueSQLRepositoryImpl{
		runner:  runner,
		events:  NewEventsSQLRepository(runner),
		blocked: NewBlockedStateSQLRepository(runner),
	}
}

type issueSQLRepositoryImpl struct {
	runner  Runner
	events  domain.EventsSQLRepository
	blocked domain.BlockedStateSQLRepository
}

var _ domain.IssueSQLRepository = (*issueSQLRepositoryImpl)(nil)

const issueSelectColumns = `id, content_hash, title, description, design, acceptance_criteria, notes,
	status, priority, issue_type, assignee, estimated_minutes,
	created_at, created_by, owner, updated_at, started_at, closed_at, external_ref, spec_id,
	compaction_level, compacted_at, compacted_at_commit, original_size, source_repo, close_reason,
	sender, ephemeral, no_history, wisp_type, pinned, is_template,
	await_type, await_id, timeout_ns, waiters,
	mol_type,
	event_kind, actor, target, payload,
	due_at, defer_until,
	work_type, source_system, metadata`

var allowedUpdateFields = map[string]struct{}{
	"status": {}, "priority": {}, "title": {}, "assignee": {},
	"description": {}, "design": {}, "acceptance_criteria": {}, "notes": {},
	"issue_type": {}, "estimated_minutes": {}, "external_ref": {}, "spec_id": {},
	"started_at": {}, "closed_at": {}, "close_reason": {}, "closed_by_session": {},
	"source_repo": {}, "sender": {}, "wisp_type": {}, "no_history": {}, "pinned": {},
	"mol_type": {}, "event_kind": {}, "actor": {}, "target": {}, "payload": {},
	"due_at": {}, "defer_until": {}, "await_id": {}, "waiters": {},
	"metadata": {},
}

func (r *issueSQLRepositoryImpl) Insert(ctx context.Context, issue *types.Issue, actor string, opts domain.InsertIssueOpts) error {
	if issue == nil {
		return errors.New("db: Insert: issue must not be nil")
	}

	normalizeIssueTimestamps(issue)
	if issue.ContentHash == "" {
		issue.ContentHash = issue.ComputeContentHash()
	}

	if issue.ID == "" {
		return errors.New("db: Insert: explicit ID required (ID generation belongs to CreateIssueUseCase)")
	}

	table := pickIssueTable(opts.UseWispsTable)
	if err := insertIssueRow(ctx, r.runner, table, issue); err != nil {
		return err
	}
	return r.events.Record(ctx, domain.Event{
		IssueID: issue.ID,
		Type:    types.EventCreated,
		Actor:   actor,
	}, domain.RecordEventOpts{UseWispsTable: opts.UseWispsTable})
}

func (r *issueSQLRepositoryImpl) InsertBatch(ctx context.Context, issues []*types.Issue, actor string, opts domain.InsertIssueOpts) error {
	for _, issue := range issues {
		if err := r.Insert(ctx, issue, actor, opts); err != nil {
			return err
		}
	}
	return nil
}

func (r *issueSQLRepositoryImpl) Update(ctx context.Context, id string, updates map[string]any, actor string, opts domain.IssueTableOpts) error {
	if id == "" {
		return errors.New("db: Update: id must not be empty")
	}
	if len(updates) == 0 {
		return nil
	}

	setClauses := make([]string, 0, len(updates))
	args := make([]any, 0, len(updates)+1)
	for key, value := range updates {
		if _, ok := allowedUpdateFields[key]; !ok {
			return fmt.Errorf("db: Update: field %q is not allowed", key)
		}
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", key))
		args = append(args, normalizeUpdateValue(key, value))
	}
	setClauses = append(setClauses, "updated_at = ?")
	args = append(args, time.Now().UTC())
	args = append(args, id)

	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	q := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", table, strings.Join(setClauses, ", "))
	res, err := r.runner.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("db: Update %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("db: Update %s: rows affected: %w", id, err)
	}
	if rows == 0 {
		return fmt.Errorf("db: Update %s: %w", id, sql.ErrNoRows)
	}

	return r.events.Record(ctx, domain.Event{
		IssueID: id,
		Type:    types.EventUpdated,
		Actor:   actor,
	}, domain.RecordEventOpts{UseWispsTable: opts.UseWispsTable})
}

func (r *issueSQLRepositoryImpl) Get(ctx context.Context, id string, opts domain.IssueTableOpts) (*types.Issue, error) {
	if id == "" {
		return nil, errors.New("db: Get: id must not be empty")
	}
	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	row := r.runner.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", issueSelectColumns, table), id)
	issue, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, fmt.Errorf("db: Get %s: %w", id, err)
	}
	return issue, nil
}

func (r *issueSQLRepositoryImpl) GetByIDs(ctx context.Context, ids []string, opts domain.IssueTableOpts) ([]*types.Issue, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	q := fmt.Sprintf("SELECT %s FROM %s WHERE id IN (%s)", issueSelectColumns, table, strings.Join(placeholders, ","))
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("db: GetByIDs: %w", err)
	}
	defer rows.Close()

	var out []*types.Issue
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("db: GetByIDs: scan: %w", err)
		}
		out = append(out, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: GetByIDs: rows: %w", err)
	}
	return out, nil
}

func (r *issueSQLRepositoryImpl) Exists(ctx context.Context, id string, opts domain.IssueTableOpts) (bool, error) {
	if id == "" {
		return false, errors.New("db: Exists: id must not be empty")
	}
	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	row := r.runner.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE id = ? LIMIT 1", table), id)
	var one int
	err := row.Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("db: Exists %s: %w", id, err)
	}
	return true, nil
}

func (r *issueSQLRepositoryImpl) CountForPrefix(ctx context.Context, prefix string, opts domain.IssueTableOpts) (int, error) {
	if prefix == "" {
		return 0, errors.New("db: CountForPrefix: prefix must not be empty")
	}
	table := pickIssueTable(opts.UseWispsTable)
	var count int
	//nolint:gosec // G201: table is one of two hardcoded constants
	err := r.runner.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s
		WHERE id LIKE CONCAT(?, '-%%')
		  AND INSTR(SUBSTRING(id, LENGTH(?) + 2), '.') = 0
	`, table), prefix, prefix).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("db: CountForPrefix %s: %w", prefix, err)
	}
	return count, nil
}

func (r *issueSQLRepositoryImpl) NextCounterID(ctx context.Context, prefix string) (int, error) {
	if prefix == "" {
		return 0, errors.New("db: NextCounterID: prefix must not be empty")
	}

	res, err := r.runner.ExecContext(ctx, "UPDATE issue_counter SET last_id = last_id + 1 WHERE prefix = ?", prefix)
	if err != nil {
		return 0, fmt.Errorf("db: NextCounterID: increment %q: %w", prefix, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("db: NextCounterID: rows affected %q: %w", prefix, err)
	}

	if rows == 0 {
		if err := r.seedCounterFromExisting(ctx, prefix); err != nil {
			return 0, fmt.Errorf("db: NextCounterID: seed %q: %w", prefix, err)
		}
		res, err = r.runner.ExecContext(ctx, "UPDATE issue_counter SET last_id = last_id + 1 WHERE prefix = ?", prefix)
		if err != nil {
			return 0, fmt.Errorf("db: NextCounterID: increment after seed %q: %w", prefix, err)
		}
		rows, err = res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("db: NextCounterID: rows affected after seed %q: %w", prefix, err)
		}
		if rows == 0 {
			if _, err := r.runner.ExecContext(ctx, "INSERT INTO issue_counter (prefix, last_id) VALUES (?, 1)", prefix); err != nil {
				return 0, fmt.Errorf("db: NextCounterID: insert initial %q: %w", prefix, err)
			}
		}
	}

	var nextID int
	if err := r.runner.QueryRowContext(ctx, "SELECT last_id FROM issue_counter WHERE prefix = ?", prefix).Scan(&nextID); err != nil {
		return 0, fmt.Errorf("db: NextCounterID: read last_id %q: %w", prefix, err)
	}
	return nextID, nil
}

func (r *issueSQLRepositoryImpl) seedCounterFromExisting(ctx context.Context, prefix string) error {
	var existing int
	err := r.runner.QueryRowContext(ctx, "SELECT last_id FROM issue_counter WHERE prefix = ?", prefix).Scan(&existing)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read existing counter %q: %w", prefix, err)
	}

	rows, err := r.runner.QueryContext(ctx, "SELECT id FROM issues WHERE id LIKE CONCAT(?, '-%')", prefix)
	if err != nil {
		return fmt.Errorf("scan issues for %q: %w", prefix, err)
	}
	defer rows.Close()

	maxNum := 0
	pfxDash := prefix + "-"
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		suffix := strings.TrimPrefix(id, pfxDash)
		if strings.Contains(suffix, ".") {
			continue
		}
		if n, err := strconv.Atoi(suffix); err == nil && n > maxNum {
			maxNum = n
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate issues for %q: %w", prefix, err)
	}

	if maxNum > 0 {
		if _, err := r.runner.ExecContext(ctx, "INSERT INTO issue_counter (prefix, last_id) VALUES (?, ?)", prefix, maxNum); err != nil {
			return fmt.Errorf("seed counter %q at %d: %w", prefix, maxNum, err)
		}
	}
	return nil
}

func (r *issueSQLRepositoryImpl) Search(ctx context.Context, filter types.IssueFilter, opts domain.IssueTableOpts) ([]*types.Issue, error) {
	q, args := buildSearchQuery(filter, pickIssueTable(opts.UseWispsTable))
	rows, err := r.runner.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("db: Search: %w", err)
	}
	defer rows.Close()

	var out []*types.Issue
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("db: Search: scan: %w", err)
		}
		out = append(out, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: Search: rows: %w", err)
	}
	return out, nil
}

func buildSearchQuery(filter types.IssueFilter, table string) (string, []any) {
	var where []string
	var args []any

	if filter.Status != nil {
		where = append(where, "status = ?")
		args = append(args, string(*filter.Status))
	}
	if len(filter.Statuses) > 0 {
		ph := make([]string, len(filter.Statuses))
		for i, s := range filter.Statuses {
			ph[i] = "?"
			args = append(args, string(s))
		}
		where = append(where, fmt.Sprintf("status IN (%s)", strings.Join(ph, ",")))
	}
	if filter.Priority != nil {
		where = append(where, "priority = ?")
		args = append(args, *filter.Priority)
	}
	if filter.IssueType != nil {
		where = append(where, "issue_type = ?")
		args = append(args, string(*filter.IssueType))
	}
	if filter.Assignee != nil {
		where = append(where, "assignee = ?")
		args = append(args, *filter.Assignee)
	}
	if filter.TitleSearch != "" {
		where = append(where, "title LIKE ?")
		args = append(args, "%"+filter.TitleSearch+"%")
	}
	if len(filter.IDs) > 0 {
		ph := make([]string, len(filter.IDs))
		for i, id := range filter.IDs {
			ph[i] = "?"
			args = append(args, id)
		}
		where = append(where, fmt.Sprintf("id IN (%s)", strings.Join(ph, ",")))
	}
	if filter.IDPrefix != "" {
		where = append(where, "id LIKE ?")
		args = append(args, filter.IDPrefix+"%")
	}
	if filter.SpecIDPrefix != "" {
		where = append(where, "spec_id LIKE ?")
		args = append(args, filter.SpecIDPrefix+"%")
	}

	//nolint:gosec // G201: table is one of two hardcoded constants
	q := fmt.Sprintf("SELECT %s FROM %s", issueSelectColumns, table)
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY priority ASC, created_at DESC"
	if filter.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}
	return q, args
}

func normalizeIssueTimestamps(issue *types.Issue) {
	now := time.Now().UTC()
	if issue.CreatedAt.IsZero() {
		issue.CreatedAt = now
	} else {
		issue.CreatedAt = issue.CreatedAt.UTC()
	}
	if issue.UpdatedAt.IsZero() {
		issue.UpdatedAt = now
	} else {
		issue.UpdatedAt = issue.UpdatedAt.UTC()
	}
}

func pickIssueTable(useWisps bool) string {
	if useWisps {
		return "wisps"
	}
	return "issues"
}

//nolint:gosec // G201: table is a hardcoded constant ("issues" or "wisps")
func insertIssueRow(ctx context.Context, runner Runner, table string, issue *types.Issue) error {
	_, err := runner.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			id, content_hash, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type, assignee, estimated_minutes,
			created_at, created_by, owner, updated_at, started_at, closed_at, external_ref, spec_id,
			compaction_level, compacted_at, compacted_at_commit, original_size,
			sender, ephemeral, no_history, wisp_type, pinned, is_template,
			mol_type, work_type, source_system, source_repo, close_reason,
			event_kind, actor, target, payload,
			await_type, await_id, timeout_ns, waiters,
			due_at, defer_until, metadata
		) VALUES (
			?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?, ?,
			?, ?, ?
		)
		ON DUPLICATE KEY UPDATE
			content_hash = VALUES(content_hash),
			title = VALUES(title),
			description = VALUES(description),
			design = VALUES(design),
			acceptance_criteria = VALUES(acceptance_criteria),
			notes = VALUES(notes),
			status = VALUES(status),
			priority = VALUES(priority),
			issue_type = VALUES(issue_type),
			assignee = VALUES(assignee),
			estimated_minutes = VALUES(estimated_minutes),
			updated_at = VALUES(updated_at),
			started_at = VALUES(started_at),
			closed_at = VALUES(closed_at),
			external_ref = VALUES(external_ref),
			source_repo = VALUES(source_repo),
			close_reason = VALUES(close_reason),
			metadata = VALUES(metadata)
	`, table),
		issue.ID, issue.ContentHash, issue.Title, issue.Description, issue.Design, issue.AcceptanceCriteria, issue.Notes,
		string(issue.Status), issue.Priority, string(issue.IssueType), nullString(issue.Assignee), nullIntPtr(issue.EstimatedMinutes),
		issue.CreatedAt, issue.CreatedBy, issue.Owner, issue.UpdatedAt, issue.StartedAt, issue.ClosedAt, nullStringPtr(issue.ExternalRef), issue.SpecID,
		issue.CompactionLevel, issue.CompactedAt, nullStringPtr(issue.CompactedAtCommit), nullIntVal(issue.OriginalSize),
		issue.Sender, issue.Ephemeral, issue.NoHistory, string(issue.WispType), issue.Pinned, issue.IsTemplate,
		string(issue.MolType), string(issue.WorkType), issue.SourceSystem, issue.SourceRepo, issue.CloseReason,
		issue.EventKind, issue.Actor, issue.Target, issue.Payload,
		issue.AwaitType, issue.AwaitID, issue.Timeout.Nanoseconds(), formatJSONStringArray(issue.Waiters),
		issue.DueAt, issue.DeferUntil, jsonMetadata(issue.Metadata),
	)
	if err != nil {
		return fmt.Errorf("db: insert into %s: %w", table, err)
	}
	return nil
}

type issueScanner interface {
	Scan(dest ...any) error
}

func scanIssue(s issueScanner, extra ...any) (*types.Issue, error) {
	var issue types.Issue
	var startedAt, closedAt, compactedAt, dueAt, deferUntil sql.NullTime
	var estimatedMinutes, originalSize, timeoutNs sql.NullInt64
	var contentHash, createdBy, owner sql.NullString
	var assignee, externalRef, specID, compactedAtCommit sql.NullString
	var sourceRepo, closeReason sql.NullString
	var workType, sourceSystem sql.NullString
	var sender, wispType, molType, eventKind, actorCol, target, payload sql.NullString
	var awaitType, awaitID, waiters sql.NullString
	var ephemeral, noHistory, pinned, isTemplate sql.NullInt64
	var metadata sql.NullString
	var createdAt, updatedAt sql.NullTime

	dests := []any{
		&issue.ID, &contentHash, &issue.Title, &issue.Description, &issue.Design,
		&issue.AcceptanceCriteria, &issue.Notes, &issue.Status,
		&issue.Priority, &issue.IssueType, &assignee, &estimatedMinutes,
		&createdAt, &createdBy, &owner, &updatedAt, &startedAt, &closedAt, &externalRef, &specID,
		&issue.CompactionLevel, &compactedAt, &compactedAtCommit, &originalSize, &sourceRepo, &closeReason,
		&sender, &ephemeral, &noHistory, &wispType, &pinned, &isTemplate,
		&awaitType, &awaitID, &timeoutNs, &waiters,
		&molType,
		&eventKind, &actorCol, &target, &payload,
		&dueAt, &deferUntil,
		&workType, &sourceSystem, &metadata,
	}
	dests = append(dests, extra...)
	if err := s.Scan(dests...); err != nil {
		return nil, err
	}

	if createdAt.Valid {
		issue.CreatedAt = createdAt.Time
	}
	if updatedAt.Valid {
		issue.UpdatedAt = updatedAt.Time
	}
	if contentHash.Valid {
		issue.ContentHash = contentHash.String
	}
	if startedAt.Valid {
		issue.StartedAt = &startedAt.Time
	}
	if closedAt.Valid {
		issue.ClosedAt = &closedAt.Time
	}
	if estimatedMinutes.Valid {
		mins := int(estimatedMinutes.Int64)
		issue.EstimatedMinutes = &mins
	}
	if assignee.Valid {
		issue.Assignee = assignee.String
	}
	if createdBy.Valid {
		issue.CreatedBy = createdBy.String
	}
	if owner.Valid {
		issue.Owner = owner.String
	}
	if externalRef.Valid {
		issue.ExternalRef = &externalRef.String
	}
	if specID.Valid {
		issue.SpecID = specID.String
	}
	if compactedAt.Valid {
		issue.CompactedAt = &compactedAt.Time
	}
	if compactedAtCommit.Valid {
		issue.CompactedAtCommit = &compactedAtCommit.String
	}
	if originalSize.Valid {
		issue.OriginalSize = int(originalSize.Int64)
	}
	if sourceRepo.Valid {
		issue.SourceRepo = sourceRepo.String
	}
	if closeReason.Valid {
		issue.CloseReason = closeReason.String
	}
	if sender.Valid {
		issue.Sender = sender.String
	}
	if ephemeral.Valid && ephemeral.Int64 != 0 {
		issue.Ephemeral = true
	}
	if noHistory.Valid && noHistory.Int64 != 0 {
		issue.NoHistory = true
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
	if awaitType.Valid {
		issue.AwaitType = awaitType.String
	}
	if awaitID.Valid {
		issue.AwaitID = awaitID.String
	}
	if timeoutNs.Valid {
		issue.Timeout = time.Duration(timeoutNs.Int64)
	}
	if waiters.Valid && waiters.String != "" {
		var parsed []string
		if err := json.Unmarshal([]byte(waiters.String), &parsed); err == nil {
			issue.Waiters = parsed
		}
	}
	if molType.Valid {
		issue.MolType = types.MolType(molType.String)
	}
	if eventKind.Valid {
		issue.EventKind = eventKind.String
	}
	if actorCol.Valid {
		issue.Actor = actorCol.String
	}
	if target.Valid {
		issue.Target = target.String
	}
	if payload.Valid {
		issue.Payload = payload.String
	}
	if dueAt.Valid {
		issue.DueAt = &dueAt.Time
	}
	if deferUntil.Valid {
		issue.DeferUntil = &deferUntil.Time
	}
	if workType.Valid {
		issue.WorkType = types.WorkType(workType.String)
	}
	if sourceSystem.Valid {
		issue.SourceSystem = sourceSystem.String
	}
	if metadata.Valid && metadata.String != "" && metadata.String != "{}" {
		issue.Metadata = []byte(metadata.String)
	}

	return &issue, nil
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullStringPtr(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}

func nullIntPtr(i *int) any {
	if i == nil {
		return nil
	}
	return *i
}

func nullIntVal(i int) any {
	if i == 0 {
		return nil
	}
	return i
}

func jsonMetadata(raw json.RawMessage) any {
	if len(raw) == 0 {
		return "{}"
	}
	return string(raw)
}

func formatJSONStringArray(items []string) string {
	if len(items) == 0 {
		return ""
	}
	b, err := json.Marshal(items)
	if err != nil {
		return ""
	}
	return string(b)
}

var timestampUpdateFields = map[string]struct{}{
	"started_at": {}, "closed_at": {}, "due_at": {}, "defer_until": {},
}

func normalizeUpdateValue(key string, value any) any {
	if _, ok := timestampUpdateFields[key]; ok {
		switch v := value.(type) {
		case time.Time:
			return v.UTC()
		case *time.Time:
			if v == nil {
				return nil
			}
			t := v.UTC()
			return t
		}
		return value
	}
	switch key {
	case "status":
		if s, ok := value.(types.Status); ok {
			return string(s)
		}
	case "issue_type":
		if t, ok := value.(types.IssueType); ok {
			return string(t)
		}
	case "metadata":
		switch v := value.(type) {
		case json.RawMessage:
			return string(v)
		case []byte:
			return string(v)
		}
	}
	return value
}

func (r *issueSQLRepositoryImpl) SearchAcrossIssuesAndWisps(ctx context.Context, query string, filter types.IssueFilter) (domain.SearchPage, error) {
	return r.searchAcrossIssuesAndWisps(ctx, query, filter)
}

func (r *issueSQLRepositoryImpl) SearchAcrossIssuesAndWispsWithCounts(ctx context.Context, query string, filter types.IssueFilter) (domain.SearchCountsPage, error) {
	return r.searchAcrossIssuesAndWispsWithCounts(ctx, query, filter)
}

func (r *issueSQLRepositoryImpl) GetReadyWork(ctx context.Context, filter types.WorkFilter) (domain.SearchPage, error) {
	return r.getReadyWorkUnion(ctx, filter)
}

func (r *issueSQLRepositoryImpl) GetReadyWorkWithCounts(ctx context.Context, filter types.WorkFilter) (domain.SearchCountsPage, error) {
	return r.getReadyWorkWithCountsUnion(ctx, filter)
}

func (r *issueSQLRepositoryImpl) Close(ctx context.Context, id, reason, actor, session string, opts domain.IssueTableOpts) error {
	if id == "" {
		return errors.New("db: Close: id must not be empty")
	}

	now := time.Now().UTC()
	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	q := fmt.Sprintf(`
		UPDATE %s
		SET status = ?, closed_at = ?, updated_at = ?, close_reason = ?, closed_by_session = ?
		WHERE id = ?
	`, table)
	res, err := r.runner.ExecContext(ctx, q, string(types.StatusClosed), now, now, reason, session, id)
	if err != nil {
		return fmt.Errorf("db: Close %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("db: Close %s: rows affected: %w", id, err)
	}

	if rows == 0 {
		var existingStatus string
		//nolint:gosec // G201: table is one of two hardcoded constants
		qerr := r.runner.QueryRowContext(ctx,
			fmt.Sprintf("SELECT status FROM %s WHERE id = ?", table), id,
		).Scan(&existingStatus)
		if errors.Is(qerr, sql.ErrNoRows) {
			return fmt.Errorf("db: Close %s: %w", id, sql.ErrNoRows)
		}
		if qerr != nil {
			return fmt.Errorf("db: Close %s: check existence: %w", id, qerr)
		}
		if types.Status(existingStatus) == types.StatusClosed {
			return nil
		}
		return fmt.Errorf("db: Close %s: no rows updated but status is %q", id, existingStatus)
	}

	if err := r.events.Record(ctx, domain.Event{
		IssueID:  id,
		Type:     types.EventClosed,
		Actor:    actor,
		NewValue: reason,
	}, domain.RecordEventOpts{UseWispsTable: opts.UseWispsTable}); err != nil {
		return err
	}

	affected, err := r.blocked.AffectedByStatusChange(ctx, id, opts.UseWispsTable)
	if err != nil {
		return fmt.Errorf("db: Close %s: affected by status change: %w", id, err)
	}
	if err := r.blocked.Recompute(ctx, affected); err != nil {
		return fmt.Errorf("db: Close %s: recompute is_blocked: %w", id, err)
	}
	return nil
}

func (r *issueSQLRepositoryImpl) Delete(ctx context.Context, id string, opts domain.IssueTableOpts) error {
	if id == "" {
		return errors.New("db: Delete: id must not be empty")
	}

	var deletedIssues, deletedWisps []string
	if opts.UseWispsTable {
		deletedWisps = []string{id}
	} else {
		deletedIssues = []string{id}
	}
	affected, err := r.blocked.AffectedByDeletion(ctx, deletedIssues, deletedWisps)
	if err != nil {
		return fmt.Errorf("db: Delete %s: affected by deletion: %w", id, err)
	}

	table := pickIssueTable(opts.UseWispsTable)
	//nolint:gosec // G201: table is one of two hardcoded constants
	res, err := r.runner.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", table), id)
	if err != nil {
		return fmt.Errorf("db: Delete %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("db: Delete %s: rows affected: %w", id, err)
	}
	if rows == 0 {
		return fmt.Errorf("db: Delete %s: %w", id, sql.ErrNoRows)
	}

	if opts.UseWispsTable {
		if _, err := r.runner.ExecContext(ctx,
			"DELETE FROM dependencies WHERE depends_on_wisp_id = ?", id,
		); err != nil {
			return fmt.Errorf("db: Delete %s: scrub orphan wisp dep refs: %w", id, err)
		}
	}

	if err := r.blocked.Recompute(ctx, affected); err != nil {
		return fmt.Errorf("db: Delete %s: recompute is_blocked: %w", id, err)
	}
	return nil
}

func (r *issueSQLRepositoryImpl) Claim(ctx context.Context, id, actor string, opts domain.IssueTableOpts) error {
	if id == "" {
		return errors.New("db: Claim: id must not be empty")
	}
	if actor == "" {
		return errors.New("db: Claim: actor must not be empty")
	}

	oldIssue, err := r.Get(ctx, id, opts)
	if err != nil {
		return fmt.Errorf("db: Claim %s: read prior state: %w", id, err)
	}

	now := time.Now().UTC()
	table := pickIssueTable(opts.UseWispsTable)

	var res sql.Result
	if oldIssue.StartedAt == nil {
		//nolint:gosec // G201: table is one of two hardcoded constants
		res, err = r.runner.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET assignee = ?, status = 'in_progress', updated_at = ?, started_at = ?
			WHERE id = ? AND status = 'open' AND (assignee = '' OR assignee IS NULL OR assignee = ?)
		`, table), actor, now, now, id, actor)
	} else {
		//nolint:gosec // G201: table is one of two hardcoded constants
		res, err = r.runner.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET assignee = ?, status = 'in_progress', updated_at = ?
			WHERE id = ? AND status = 'open' AND (assignee = '' OR assignee IS NULL OR assignee = ?)
		`, table), actor, now, id, actor)
	}
	if err != nil {
		return fmt.Errorf("db: Claim %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("db: Claim %s: rows affected: %w", id, err)
	}

	if rows == 0 {
		var currentAssignee sql.NullString
		var currentStatus string
		//nolint:gosec // G201: table is one of two hardcoded constants
		if err := r.runner.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT assignee, status FROM %s WHERE id = ?", table), id,
		).Scan(&currentAssignee, &currentStatus); err != nil {
			return fmt.Errorf("db: Claim %s: read current state: %w", id, err)
		}
		assignee := ""
		if currentAssignee.Valid {
			assignee = currentAssignee.String
		}
		if assignee == actor && types.Status(currentStatus) == types.StatusInProgress {
			return nil
		}
		if assignee != "" && assignee != actor {
			return fmt.Errorf("%w by %s", domain.ErrAlreadyClaimed, assignee)
		}
		return fmt.Errorf("%w: status %s", domain.ErrNotClaimable, currentStatus)
	}

	oldData, err := json.Marshal(oldIssue)
	if err != nil {
		return fmt.Errorf("db: Claim %s: marshal old: %w", id, err)
	}
	newData, err := json.Marshal(map[string]any{"assignee": actor, "status": "in_progress"})
	if err != nil {
		return fmt.Errorf("db: Claim %s: marshal new: %w", id, err)
	}

	return r.events.Record(ctx, domain.Event{
		IssueID:  id,
		Type:     types.EventType("claimed"),
		Actor:    actor,
		OldValue: string(oldData),
		NewValue: string(newData),
	}, domain.RecordEventOpts{UseWispsTable: opts.UseWispsTable})
}

const deleteBatchSize = 50

const maxCascadeResults = 10000

func (r *issueSQLRepositoryImpl) DeleteBatch(ctx context.Context, ids []string, force, dryRun bool, opts domain.IssueTableOpts) (*types.DeleteIssuesResult, error) {
	_ = force
	_ = opts
	if len(ids) == 0 {
		return &types.DeleteIssuesResult{}, nil
	}

	initialWisps, initialIssues, err := r.partitionByTable(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: partition: %w", err)
	}

	expanded, err := r.expandDependentsRecursive(ctx, initialIssues, initialWisps)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: cascade: %w", err)
	}

	finalWisps, finalIssues, err := r.partitionByTable(ctx, expanded)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: re-partition: %w", err)
	}

	depsCount, err := countRowsForIDs(ctx, r.runner, "dependencies", "issue_id", finalIssues)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count dependencies: %w", err)
	}
	wispDepsCount, err := countRowsForIDs(ctx, r.runner, "wisp_dependencies", "issue_id", finalWisps)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count wisp dependencies: %w", err)
	}
	depsCount += wispDepsCount

	labelsCount, err := countRowsForIDs(ctx, r.runner, "labels", "issue_id", finalIssues)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count labels: %w", err)
	}
	wispLabelsCount, err := countRowsForIDs(ctx, r.runner, "wisp_labels", "issue_id", finalWisps)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count wisp labels: %w", err)
	}
	labelsCount += wispLabelsCount

	eventsCount, err := countRowsForIDs(ctx, r.runner, "events", "issue_id", finalIssues)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count events: %w", err)
	}
	wispEventsCount, err := countRowsForIDs(ctx, r.runner, "wisp_events", "issue_id", finalWisps)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: count wisp events: %w", err)
	}
	eventsCount += wispEventsCount

	result := &types.DeleteIssuesResult{
		DeletedCount:      len(finalIssues) + len(finalWisps),
		DependenciesCount: depsCount,
		LabelsCount:       labelsCount,
		EventsCount:       eventsCount,
	}

	if dryRun {
		return result, nil
	}

	affected, err := r.blocked.AffectedByDeletion(ctx, finalIssues, finalWisps)
	if err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: affected by deletion: %w", err)
	}

	if err := deleteRowsBatched(ctx, r.runner, "wisps", finalWisps); err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: %w", err)
	}
	if len(finalWisps) > 0 {
		if err := scrubOrphanWispDepRefs(ctx, r.runner, finalWisps); err != nil {
			return nil, fmt.Errorf("db: DeleteBatch: %w", err)
		}
	}
	if err := deleteRowsBatched(ctx, r.runner, "issues", finalIssues); err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: %w", err)
	}

	if err := r.blocked.Recompute(ctx, affected); err != nil {
		return nil, fmt.Errorf("db: DeleteBatch: recompute is_blocked: %w", err)
	}
	return result, nil
}

func (r *issueSQLRepositoryImpl) partitionByTable(ctx context.Context, ids []string) (wispIDs, issueIDs []string, err error) {
	if len(ids) == 0 {
		return nil, nil, nil
	}
	wispSet := make(map[string]bool, len(ids))
	for start := 0; start < len(ids); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		placeholders, args := buildInPlaceholders(batch)
		//nolint:gosec // G201: placeholders contain only ? markers
		rows, qerr := r.runner.QueryContext(ctx,
			fmt.Sprintf("SELECT id FROM wisps WHERE id IN (%s)", placeholders), args...)
		if qerr != nil {
			return nil, nil, fmt.Errorf("probe wisps: %w", qerr)
		}
		for rows.Next() {
			var id string
			if serr := rows.Scan(&id); serr != nil {
				_ = rows.Close()
				return nil, nil, fmt.Errorf("scan wisp id: %w", serr)
			}
			wispSet[id] = true
		}
		_ = rows.Close()
		if rerr := rows.Err(); rerr != nil {
			return nil, nil, fmt.Errorf("iterate wisp probe: %w", rerr)
		}
	}
	for _, id := range ids {
		if wispSet[id] {
			wispIDs = append(wispIDs, id)
		} else {
			issueIDs = append(issueIDs, id)
		}
	}
	return wispIDs, issueIDs, nil
}

func (r *issueSQLRepositoryImpl) expandDependentsRecursive(ctx context.Context, seedIssues, seedWisps []string) ([]string, error) {
	seen := make(map[string]bool, len(seedIssues)+len(seedWisps))
	queue := make([]string, 0, len(seedIssues)+len(seedWisps))
	for _, id := range seedIssues {
		if !seen[id] {
			seen[id] = true
			queue = append(queue, id)
		}
	}
	for _, id := range seedWisps {
		if !seen[id] {
			seen[id] = true
			queue = append(queue, id)
		}
	}

	head := 0
	for head < len(queue) {
		if len(seen) > maxCascadeResults {
			return nil, fmt.Errorf("cascade traversal discovered over %d issues; aborting to prevent runaway deletion", maxCascadeResults)
		}
		end := head + deleteBatchSize
		if end > len(queue) {
			end = len(queue)
		}
		batch := queue[head:end]
		head = end

		placeholders, args := buildInPlaceholders(batch)
		for _, depTable := range []string{"dependencies", "wisp_dependencies"} {
			//nolint:gosec // G201: depTable is a hardcoded constant; placeholders contain only ? markers
			q := fmt.Sprintf(`
				SELECT issue_id FROM %s
				WHERE depends_on_issue_id IN (%s)
				   OR depends_on_wisp_id IN (%s)
			`, depTable, placeholders, placeholders)
			doubled := make([]any, 0, len(args)*2)
			doubled = append(doubled, args...)
			doubled = append(doubled, args...)
			rows, qerr := r.runner.QueryContext(ctx, q, doubled...)
			if qerr != nil {
				return nil, fmt.Errorf("query dependents from %s: %w", depTable, qerr)
			}
			for rows.Next() {
				var id string
				if serr := rows.Scan(&id); serr != nil {
					_ = rows.Close()
					return nil, fmt.Errorf("scan dependent: %w", serr)
				}
				if !seen[id] {
					seen[id] = true
					queue = append(queue, id)
				}
			}
			_ = rows.Close()
			if rerr := rows.Err(); rerr != nil {
				return nil, fmt.Errorf("iterate dependents from %s: %w", depTable, rerr)
			}
		}
	}
	return queue, nil
}

//nolint:gosec // G201: table and column come from constant call sites; placeholders contain only ? markers
func countRowsForIDs(ctx context.Context, runner Runner, table, idColumn string, ids []string) (int, error) {
	total := 0
	for start := 0; start < len(ids); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		placeholders, args := buildInPlaceholders(ids[start:end])
		var n int
		if err := runner.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IN (%s)", table, idColumn, placeholders),
			args...,
		).Scan(&n); err != nil {
			return 0, fmt.Errorf("count %s: %w", table, err)
		}
		total += n
	}
	return total, nil
}

//nolint:gosec // G201: table is a hardcoded constant; placeholders contain only ? markers
func deleteRowsBatched(ctx context.Context, runner Runner, table string, ids []string) error {
	for start := 0; start < len(ids); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		placeholders, args := buildInPlaceholders(ids[start:end])
		if _, err := runner.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", table, placeholders),
			args...,
		); err != nil {
			return fmt.Errorf("delete from %s: %w", table, err)
		}
	}
	return nil
}

//nolint:gosec // G201: placeholders contain only ? markers
func scrubOrphanWispDepRefs(ctx context.Context, runner Runner, wispIDs []string) error {
	for start := 0; start < len(wispIDs); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(wispIDs) {
			end = len(wispIDs)
		}
		placeholders, args := buildInPlaceholders(wispIDs[start:end])
		if _, err := runner.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM dependencies WHERE depends_on_wisp_id IN (%s)", placeholders),
			args...,
		); err != nil {
			return fmt.Errorf("scrub orphan wisp dep refs: %w", err)
		}
	}
	return nil
}
