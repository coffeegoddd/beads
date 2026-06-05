package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func (s *testSuite) TestIssueSQLRepository() {
	s.Run("Insert", func() {
		s.Run("RoundTripWithGet", s.issueInsertRoundTrip)
		s.Run("RequiresExplicitID", s.issueInsertRequiresID)
		s.Run("IdempotentOnDuplicateKey", s.issueInsertIdempotent)
		s.Run("RecordsCreatedEvent", s.issueInsertRecordsEvent)
		s.Run("RoutesToWispsTable", s.issueInsertWispRouting)
		s.Run("ComputesContentHashWhenMissing", s.issueInsertComputesHash)
	})
	s.Run("InsertBatch", func() {
		s.Run("AllIssuesInserted", s.issueInsertBatchAll)
		s.Run("StopsOnFirstError", s.issueInsertBatchStopsOnError)
	})
	s.Run("Update", func() {
		s.Run("UpdatesAllowedFields", s.issueUpdateAllowedFields)
		s.Run("RejectsUnknownFields", s.issueUpdateRejectsUnknownFields)
		s.Run("MissingIDReturnsErrNoRows", s.issueUpdateMissingID)
		s.Run("EmptyUpdatesIsNoop", s.issueUpdateEmpty)
		s.Run("NormalizesStatusType", s.issueUpdateStatusType)
		s.Run("NormalizesTimestampToUTC", s.issueUpdateNormalizesTimestamp)
	})
	s.Run("Get", func() {
		s.Run("MissingIDReturnsErrNoRows", s.issueGetMissing)
		s.Run("EmptyIDReturnsError", s.issueGetEmptyID)
	})
	s.Run("GetByIDs", func() {
		s.Run("EmptySliceReturnsNil", s.issueGetByIDsEmpty)
		s.Run("ReturnsOnlyExistingRows", s.issueGetByIDsPartial)
	})
	s.Run("Search", func() {
		s.Run("NoFilterReturnsAll", s.issueSearchAll)
		s.Run("FilterByStatus", s.issueSearchByStatus)
		s.Run("FilterByIssueType", s.issueSearchByIssueType)
		s.Run("FilterByIDPrefix", s.issueSearchByIDPrefix)
		s.Run("FilterByIDs", s.issueSearchByIDs)
		s.Run("LimitRespected", s.issueSearchLimit)
	})
	s.Run("Wisp", func() {
		s.Run("InsertRoutesToWispsTable", s.issueWispInsertRouting)
		s.Run("GetReadsFromWispsTable", s.issueWispGet)
		s.Run("UpdateWritesToWispsTable", s.issueWispUpdate)
		s.Run("SearchReadsFromWispsTable", s.issueWispSearch)
		s.Run("CrossRoutedLookupsAreEmpty", s.issueWispIsolated)
	})
	s.Run("Exists", func() {
		s.Run("MissingReturnsFalse", s.issueExistsMissing)
		s.Run("PresentReturnsTrue", s.issueExistsPresent)
		s.Run("EmptyIDReturnsError", s.issueExistsEmptyID)
		s.Run("RoutedToWisps", s.issueExistsWispRouting)
	})
	s.Run("CountForPrefix", func() {
		s.Run("EmptyTableReturnsZero", s.issueCountForPrefixEmpty)
		s.Run("CountsMatching", s.issueCountForPrefixMatches)
		s.Run("ExcludesChildIDs", s.issueCountForPrefixExcludesChildren)
		s.Run("RoutedToWisps", s.issueCountForPrefixWispRouting)
		s.Run("EmptyPrefixReturnsError", s.issueCountForPrefixEmptyPrefix)
	})
	s.Run("NextCounterID", func() {
		s.Run("FreshDBInsertsAtOne", s.issueNextCounterIDFresh)
		s.Run("MonotonicIncrement", s.issueNextCounterIDIncrement)
		s.Run("SeedsFromMaxExisting", s.issueNextCounterIDSeedsFromMax)
		s.Run("IgnoresChildIDsWhenSeeding", s.issueNextCounterIDSeedSkipsChildren)
		s.Run("EmptyPrefixReturnsError", s.issueNextCounterIDEmptyPrefix)
	})
	s.Run("Close", func() {
		s.Run("SetsStatusAndCloseFields", s.issueCloseSetsFields)
		s.Run("PersistsCloseReason", s.issueClosePersistsReason)
		s.Run("PersistsClosedBySession", s.issueClosePersistsSession)
		s.Run("RecordsClosedEvent", s.issueCloseRecordsEvent)
		s.Run("MissingIDReturnsErrNoRows", s.issueCloseMissingID)
		s.Run("EmptyIDReturnsError", s.issueCloseEmptyID)
		s.Run("IdempotentOnAlreadyClosed", s.issueCloseIdempotent)
		s.Run("RoutesToWispsTable", s.issueCloseWispRouting)
		s.Run("UnblocksBlockingDependents", s.issueCloseUnblocksDependents)
	})
	s.Run("Delete", func() {
		s.Run("RowGoneAfterDelete", s.issueDeleteRowGone)
		s.Run("MissingIDReturnsErrNoRows", s.issueDeleteMissingID)
		s.Run("EmptyIDReturnsError", s.issueDeleteEmptyID)
		s.Run("RoutesToWispsTable", s.issueDeleteWispRouting)
		s.Run("CascadesOutgoingDeps", s.issueDeleteCascadesOutgoingDeps)
		s.Run("CascadesIncomingDeps", s.issueDeleteCascadesIncomingDeps)
		s.Run("CascadesLabels", s.issueDeleteCascadesLabels)
		s.Run("CascadesEvents", s.issueDeleteCascadesEvents)
		s.Run("UnblocksBlockingDependents", s.issueDeleteUnblocksDependents)
		s.Run("WispScrubsOrphanDepRefs", s.issueDeleteWispScrubsOrphans)
	})
	s.Run("DeleteBatch", func() {
		s.Run("EmptyInputReturnsEmptyResult", s.issueDeleteBatchEmpty)
		s.Run("SingleLeafDelete", s.issueDeleteBatchSingleLeaf)
		s.Run("CascadesThroughParentChild", s.issueDeleteBatchCascadesParentChild)
		s.Run("CascadesThroughBlockingDeps", s.issueDeleteBatchCascadesBlocks)
		s.Run("HandlesCircularDeps", s.issueDeleteBatchCircular)
		s.Run("DryRunReturnsCountsWithoutWriting", s.issueDeleteBatchDryRun)
		s.Run("CountsLabelsAndEvents", s.issueDeleteBatchCountsLabelsEvents)
		s.Run("MixedIssueAndWispInput", s.issueDeleteBatchMixedInput)
		s.Run("CascadeIntoWisps", s.issueDeleteBatchCascadeIntoWisps)
		s.Run("WispOrphanDepRefsScrubbed", s.issueDeleteBatchWispOrphanScrub)
		s.Run("RecomputesIsBlockedForExternalDependents", s.issueDeleteBatchRecomputesExternal)
		s.Run("BatchLargerThanDeleteBatchSize", s.issueDeleteBatchOverBatchSize)
	})
	s.Run("Claim", func() {
		s.Run("UnassignedToClaimed", s.issueClaimUnassigned)
		s.Run("SetsStartedAtOnFirstClaim", s.issueClaimSetsStartedAt)
		s.Run("PreservesStartedAtOnReClaim", s.issueClaimPreservesStartedAt)
		s.Run("IdempotentSameActor", s.issueClaimIdempotent)
		s.Run("AlreadyClaimedByOther", s.issueClaimAlreadyClaimedByOther)
		s.Run("NotClaimableWhenClosed", s.issueClaimNotClaimableClosed)
		s.Run("NotClaimableWhenInProgressByOther", s.issueClaimNotClaimableInProgress)
		s.Run("ClaimAfterUnassignedReassignmentSucceeds", s.issueClaimAfterUnassign)
		s.Run("RecordsClaimedEventWithOldAndNew", s.issueClaimRecordsEvent)
		s.Run("IdempotentReClaimEmitsNoNewEvent", s.issueClaimIdempotentNoEvent)
		s.Run("EmptyIDReturnsError", s.issueClaimEmptyID)
		s.Run("EmptyActorReturnsError", s.issueClaimEmptyActor)
		s.Run("RoutesToWispsTable", s.issueClaimWispRouting)
	})
}

func (s *testSuite) issueRepo() domain.IssueSQLRepository {
	return NewIssueSQLRepository(s.Runner())
}

func newTestIssue(id, title string) *types.Issue {
	return &types.Issue{
		ID:        id,
		Title:     title,
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
	}
}

func (s *testSuite) issueInsertRoundTrip() {
	r := s.issueRepo()
	in := newTestIssue("bd-test-1", "round trip")
	in.Description = "desc body"
	in.Assignee = "alice"
	in.Labels = []string{"ignored-in-this-impl"}
	mins := 45
	in.EstimatedMinutes = &mins

	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{}))

	out, err := r.Get(s.Ctx(), "bd-test-1", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal("bd-test-1", out.ID)
	s.Equal("round trip", out.Title)
	s.Equal("desc body", out.Description)
	s.Equal("alice", out.Assignee)
	s.Equal(types.StatusOpen, out.Status)
	s.Equal(2, out.Priority)
	s.Equal(types.TypeTask, out.IssueType)
	s.Require().NotNil(out.EstimatedMinutes)
	s.Equal(45, *out.EstimatedMinutes)
}

func (s *testSuite) issueInsertRequiresID() {
	r := s.issueRepo()
	err := r.Insert(s.Ctx(), newTestIssue("", "no id"), "tester", domain.InsertIssueOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "explicit ID required")
}

func (s *testSuite) issueInsertIdempotent() {
	r := s.issueRepo()
	in := newTestIssue("bd-test-dup", "v1")
	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{}))

	in.Title = "v2"
	in.Description = "added on second pass"
	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{}))

	out, err := r.Get(s.Ctx(), "bd-test-dup", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal("v2", out.Title)
	s.Equal("added on second pass", out.Description)
}

func (s *testSuite) issueInsertRecordsEvent() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-test-evt", "event check"), "tester", domain.InsertIssueOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-test-evt", string(types.EventCreated),
	).Scan(&count))
	s.Equal(1, count, "expected exactly one created event")
}

func (s *testSuite) issueInsertWispRouting() {
	r := s.issueRepo()
	wisp := newTestIssue("bd-test-wisp", "wisp issue")
	wisp.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), wisp, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(), "SELECT COUNT(*) FROM wisps WHERE id = ?", "bd-test-wisp").Scan(&count))
	s.Equal(1, count, "expected row in wisps table")

	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(), "SELECT COUNT(*) FROM issues WHERE id = ?", "bd-test-wisp").Scan(&count))
	s.Equal(0, count, "expected no row in issues table")

	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_events WHERE issue_id = ?",
		"bd-test-wisp",
	).Scan(&count))
	s.Equal(1, count, "expected created event in wisp_events")
}

func (s *testSuite) issueInsertComputesHash() {
	r := s.issueRepo()
	in := newTestIssue("bd-test-hash", "hash check")
	s.Require().Empty(in.ContentHash)
	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{}))
	s.Require().NotEmpty(in.ContentHash, "Insert should populate ContentHash before writing")

	out, err := r.Get(s.Ctx(), "bd-test-hash", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(in.ContentHash, out.ContentHash)
}

func (s *testSuite) issueInsertBatchAll() {
	r := s.issueRepo()
	batch := []*types.Issue{
		newTestIssue("bd-batch-1", "one"),
		newTestIssue("bd-batch-2", "two"),
		newTestIssue("bd-batch-3", "three"),
	}
	s.Require().NoError(r.InsertBatch(s.Ctx(), batch, "tester", domain.InsertIssueOpts{}))

	got, err := r.GetByIDs(s.Ctx(), []string{"bd-batch-1", "bd-batch-2", "bd-batch-3"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(got, 3)
}

func (s *testSuite) issueInsertBatchStopsOnError() {
	r := s.issueRepo()
	batch := []*types.Issue{
		newTestIssue("bd-stop-1", "ok"),
		newTestIssue("", "bad — missing id"),
		newTestIssue("bd-stop-3", "never reached"),
	}
	err := r.InsertBatch(s.Ctx(), batch, "tester", domain.InsertIssueOpts{})
	s.Require().Error(err)

	got, err := r.GetByIDs(s.Ctx(), []string{"bd-stop-1", "bd-stop-3"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(got, 1, "first issue should be persisted, third should not")
	s.Equal("bd-stop-1", got[0].ID)
}

func (s *testSuite) issueUpdateAllowedFields() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-upd-1", "before"), "tester", domain.InsertIssueOpts{}))

	updates := map[string]any{
		"title":       "after",
		"priority":    0,
		"description": "new desc",
		"assignee":    "bob",
	}
	s.Require().NoError(r.Update(s.Ctx(), "bd-upd-1", updates, "tester", domain.IssueTableOpts{}))

	out, err := r.Get(s.Ctx(), "bd-upd-1", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal("after", out.Title)
	s.Equal(0, out.Priority)
	s.Equal("new desc", out.Description)
	s.Equal("bob", out.Assignee)
}

func (s *testSuite) issueUpdateRejectsUnknownFields() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-upd-bad", "x"), "tester", domain.InsertIssueOpts{}))

	err := r.Update(s.Ctx(), "bd-upd-bad", map[string]any{"id": "rename-attempt"}, "tester", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "not allowed")
}

func (s *testSuite) issueUpdateMissingID() {
	r := s.issueRepo()
	err := r.Update(s.Ctx(), "bd-does-not-exist", map[string]any{"title": "x"}, "tester", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, sql.ErrNoRows), "expected sql.ErrNoRows, got %v", err)
}

func (s *testSuite) issueUpdateEmpty() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-upd-empty", "x"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Update(s.Ctx(), "bd-upd-empty", nil, "tester", domain.IssueTableOpts{}))
}

func (s *testSuite) issueUpdateStatusType() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-upd-status", "x"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Update(s.Ctx(), "bd-upd-status", map[string]any{
		"status":     types.StatusInProgress,
		"issue_type": types.TypeBug,
	}, "tester", domain.IssueTableOpts{}))

	out, err := r.Get(s.Ctx(), "bd-upd-status", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(types.StatusInProgress, out.Status)
	s.Equal(types.TypeBug, out.IssueType)
}

func (s *testSuite) issueUpdateNormalizesTimestamp() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-upd-tz", "tz"), "tester", domain.InsertIssueOpts{}))

	tz, err := time.LoadLocation("America/Los_Angeles")
	s.Require().NoError(err)
	due := time.Date(2030, 6, 15, 10, 0, 0, 0, tz)

	s.Require().NoError(r.Update(s.Ctx(), "bd-upd-tz", map[string]any{"due_at": due}, "tester", domain.IssueTableOpts{}))

	out, err := r.Get(s.Ctx(), "bd-upd-tz", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Require().NotNil(out.DueAt)
	s.Equal(due.UTC().Unix(), out.DueAt.Unix(), "due_at should round-trip via UTC")
}

func (s *testSuite) issueGetMissing() {
	_, err := s.issueRepo().Get(s.Ctx(), "bd-no-such-id", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, sql.ErrNoRows), "expected sql.ErrNoRows, got %v", err)
}

func (s *testSuite) issueGetEmptyID() {
	_, err := s.issueRepo().Get(s.Ctx(), "", domain.IssueTableOpts{})
	s.Require().Error(err)
}

func (s *testSuite) issueGetByIDsEmpty() {
	out, err := s.issueRepo().GetByIDs(s.Ctx(), nil, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Nil(out)
}

func (s *testSuite) issueGetByIDsPartial() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-pres-1", "a"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-pres-2", "b"), "tester", domain.InsertIssueOpts{}))

	out, err := r.GetByIDs(s.Ctx(), []string{"bd-pres-1", "bd-pres-2", "bd-missing"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 2)

	ids := map[string]bool{}
	for _, i := range out {
		ids[i.ID] = true
	}
	s.True(ids["bd-pres-1"])
	s.True(ids["bd-pres-2"])
}

func (s *testSuite) issueSearchAll() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-srch-1", "a"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-srch-2", "b"), "tester", domain.InsertIssueOpts{}))

	out, err := r.Search(s.Ctx(), types.IssueFilter{}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.GreaterOrEqual(len(out), 2)
}

func (s *testSuite) issueSearchByStatus() {
	r := s.issueRepo()

	open1 := newTestIssue("bd-stat-open", "open one")
	s.Require().NoError(r.Insert(s.Ctx(), open1, "tester", domain.InsertIssueOpts{}))

	closed := newTestIssue("bd-stat-closed", "closed one")
	closed.Status = types.StatusClosed
	s.Require().NoError(r.Insert(s.Ctx(), closed, "tester", domain.InsertIssueOpts{}))

	// Scope by ID prefix — earlier subtests share the DB state and may have
	// created closed rows.
	closedStatus := types.StatusClosed
	out, err := r.Search(s.Ctx(), types.IssueFilter{Status: &closedStatus, IDPrefix: "bd-stat-"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 1)
	s.Equal("bd-stat-closed", out[0].ID)
}

func (s *testSuite) issueSearchByIssueType() {
	r := s.issueRepo()

	bug := newTestIssue("bd-type-bug", "bug")
	bug.IssueType = types.TypeBug
	s.Require().NoError(r.Insert(s.Ctx(), bug, "tester", domain.InsertIssueOpts{}))

	task := newTestIssue("bd-type-task", "task")
	s.Require().NoError(r.Insert(s.Ctx(), task, "tester", domain.InsertIssueOpts{}))

	// Scope by ID prefix to isolate from earlier subtests.
	bugType := types.TypeBug
	out, err := r.Search(s.Ctx(), types.IssueFilter{IssueType: &bugType, IDPrefix: "bd-type-"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 1)
	s.Equal("bd-type-bug", out[0].ID)
}

func (s *testSuite) issueSearchByIDPrefix() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-pfx-a", "a"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-pfx-b", "b"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("other-1", "other"), "tester", domain.InsertIssueOpts{}))

	out, err := r.Search(s.Ctx(), types.IssueFilter{IDPrefix: "bd-pfx-"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 2)
}

func (s *testSuite) issueSearchByIDs() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-ids-1", "a"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-ids-2", "b"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-ids-3", "c"), "tester", domain.InsertIssueOpts{}))

	out, err := r.Search(s.Ctx(), types.IssueFilter{IDs: []string{"bd-ids-1", "bd-ids-3"}}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 2)
}

func (s *testSuite) issueSearchLimit() {
	r := s.issueRepo()
	for i := 0; i < 5; i++ {
		s.Require().NoError(r.Insert(s.Ctx(), newTestIssue(fmt.Sprintf("bd-lim-%d", i), "x"), "tester", domain.InsertIssueOpts{}))
	}
	out, err := r.Search(s.Ctx(), types.IssueFilter{Limit: 3, IDPrefix: "bd-lim-"}, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Len(out, 3)
}

func (s *testSuite) issueWispInsertRouting() {
	r := s.issueRepo()
	wisp := newTestIssue("bd-iss-wisp-1", "wisp issue")
	wisp.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), wisp, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	var wispCount, permCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisps WHERE id = ?", "bd-iss-wisp-1").Scan(&wispCount))
	s.Equal(1, wispCount)
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM issues WHERE id = ?", "bd-iss-wisp-1").Scan(&permCount))
	s.Equal(0, permCount)
}

func (s *testSuite) issueWispGet() {
	r := s.issueRepo()
	in := newTestIssue("bd-iss-wisp-get", "wisp get")
	in.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	out, err := r.Get(s.Ctx(), "bd-iss-wisp-get", domain.IssueTableOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Equal("bd-iss-wisp-get", out.ID)
	s.Equal("wisp get", out.Title)
}

func (s *testSuite) issueWispUpdate() {
	r := s.issueRepo()
	in := newTestIssue("bd-iss-wisp-upd", "before")
	in.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), in, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	s.Require().NoError(r.Update(s.Ctx(), "bd-iss-wisp-upd",
		map[string]any{"title": "after"}, "tester",
		domain.IssueTableOpts{UseWispsTable: true},
	))

	out, err := r.Get(s.Ctx(), "bd-iss-wisp-upd", domain.IssueTableOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Equal("after", out.Title)

	// The update event should land in wisp_events, not events.
	var wispEvtCount, permEvtCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_events WHERE issue_id = ? AND event_type = ?",
		"bd-iss-wisp-upd", string(types.EventUpdated)).Scan(&wispEvtCount))
	s.Equal(1, wispEvtCount)
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-iss-wisp-upd", string(types.EventUpdated)).Scan(&permEvtCount))
	s.Equal(0, permEvtCount)
}

func (s *testSuite) issueWispSearch() {
	r := s.issueRepo()
	for i := 0; i < 3; i++ {
		w := newTestIssue(fmt.Sprintf("bd-iss-wsrch-%d", i), "x")
		w.Ephemeral = true
		s.Require().NoError(r.Insert(s.Ctx(), w, "tester", domain.InsertIssueOpts{UseWispsTable: true}))
	}
	out, err := r.Search(s.Ctx(),
		types.IssueFilter{IDPrefix: "bd-iss-wsrch-"},
		domain.IssueTableOpts{UseWispsTable: true},
	)
	s.Require().NoError(err)
	s.Len(out, 3)
}

func (s *testSuite) issueWispIsolated() {
	r := s.issueRepo()
	perm := newTestIssue("bd-iss-iso-perm", "perm")
	s.Require().NoError(r.Insert(s.Ctx(), perm, "tester", domain.InsertIssueOpts{}))
	w := newTestIssue("bd-iss-iso-wisp", "wisp")
	w.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), w, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	// Cross-routed Get should miss in each direction.
	_, err := r.Get(s.Ctx(), "bd-iss-iso-perm", domain.IssueTableOpts{UseWispsTable: true})
	s.Require().Error(err, "permanent issue should not be visible via wisp Get")
	_, err = r.Get(s.Ctx(), "bd-iss-iso-wisp", domain.IssueTableOpts{})
	s.Require().Error(err, "wisp issue should not be visible via permanent Get")

	// GetByIDs across the wrong table returns empty.
	got, err := r.GetByIDs(s.Ctx(), []string{"bd-iss-iso-perm"}, domain.IssueTableOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Empty(got)
}

func (s *testSuite) issueExistsMissing() {
	r := s.issueRepo()
	got, err := r.Exists(s.Ctx(), "bd-not-there", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.False(got)
}

func (s *testSuite) issueExistsPresent() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("bd-exists-yes", "present"), "tester", domain.InsertIssueOpts{}))
	got, err := r.Exists(s.Ctx(), "bd-exists-yes", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.True(got)
}

func (s *testSuite) issueExistsEmptyID() {
	r := s.issueRepo()
	_, err := r.Exists(s.Ctx(), "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "id must not be empty")
}

func (s *testSuite) issueExistsWispRouting() {
	r := s.issueRepo()
	w := newTestIssue("bd-exists-wisp", "wisp")
	w.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), w, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	got, err := r.Exists(s.Ctx(), "bd-exists-wisp", domain.IssueTableOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.True(got, "should find wisp via wisps table")

	got, err = r.Exists(s.Ctx(), "bd-exists-wisp", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.False(got, "should not find wisp via issues table")
}

func (s *testSuite) issueCountForPrefixEmpty() {
	// Use a fresh prefix that no prior test inserts under. The suite shares
	// state across s.Run subtests within a single TestXxx method, so we
	// can't rely on "bd" being empty here.
	r := s.issueRepo()
	got, err := r.CountForPrefix(s.Ctx(), "cfpEmpty", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(0, got)
}

func (s *testSuite) issueCountForPrefixMatches() {
	r := s.issueRepo()
	for _, id := range []string{"cfpMat-c1", "cfpMat-c2", "cfpMat-c3"} {
		s.Require().NoError(r.Insert(s.Ctx(), newTestIssue(id, id), "tester", domain.InsertIssueOpts{}))
	}
	// Decoy with a different prefix should not be counted.
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("cfpMatX-c1", "decoy"), "tester", domain.InsertIssueOpts{}))

	got, err := r.CountForPrefix(s.Ctx(), "cfpMat", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(3, got)
}

func (s *testSuite) issueCountForPrefixExcludesChildren() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("cfpChld-parent", "parent"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("cfpChld-parent.1", "child 1"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("cfpChld-parent.2", "child 2"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("cfpChld-sibling", "sibling"), "tester", domain.InsertIssueOpts{}))

	got, err := r.CountForPrefix(s.Ctx(), "cfpChld", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(2, got, "child IDs containing '.' must not be counted")
}

func (s *testSuite) issueCountForPrefixWispRouting() {
	r := s.issueRepo()
	w := newTestIssue("cfpWisp-c1", "wisp count")
	w.Ephemeral = true
	s.Require().NoError(r.Insert(s.Ctx(), w, "tester", domain.InsertIssueOpts{UseWispsTable: true}))

	got, err := r.CountForPrefix(s.Ctx(), "cfpWisp", domain.IssueTableOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Equal(1, got)
	got, err = r.CountForPrefix(s.Ctx(), "cfpWisp", domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(0, got, "issues table should not see wisp rows")
}

func (s *testSuite) issueCountForPrefixEmptyPrefix() {
	r := s.issueRepo()
	_, err := r.CountForPrefix(s.Ctx(), "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "prefix must not be empty")
}

func (s *testSuite) issueNextCounterIDFresh() {
	// Unique prefix per subtest because subtests share state within a single
	// TestXxx method (testify suite quirk).
	r := s.issueRepo()
	got, err := r.NextCounterID(s.Ctx(), "ctrFresh")
	s.Require().NoError(err)
	s.Equal(1, got, "first counter call on a fresh prefix should yield 1")
}

func (s *testSuite) issueNextCounterIDIncrement() {
	r := s.issueRepo()
	a, err := r.NextCounterID(s.Ctx(), "ctrInc")
	s.Require().NoError(err)
	b, err := r.NextCounterID(s.Ctx(), "ctrInc")
	s.Require().NoError(err)
	c, err := r.NextCounterID(s.Ctx(), "ctrInc")
	s.Require().NoError(err)
	s.Equal(a+1, b)
	s.Equal(b+1, c)

	// Sanity: another prefix is independent.
	other, err := r.NextCounterID(s.Ctx(), "ctrIncAlt")
	s.Require().NoError(err)
	s.Equal(1, other)
}

func (s *testSuite) issueNextCounterIDSeedsFromMax() {
	r := s.issueRepo()
	// Pre-seed two issues with numeric suffixes; no issue_counter row exists.
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("ctrSeed-7", "seven"), "tester", domain.InsertIssueOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("ctrSeed-12", "twelve"), "tester", domain.InsertIssueOpts{}))

	got, err := r.NextCounterID(s.Ctx(), "ctrSeed")
	s.Require().NoError(err)
	s.Equal(13, got, "should seed from max(7,12)=12 and return 13")
}

func (s *testSuite) issueNextCounterIDSeedSkipsChildren() {
	r := s.issueRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("ctrSkip-3", "three"), "tester", domain.InsertIssueOpts{}))
	// A child of ctrSkip-3 with a numeric child suffix — must be skipped during seed.
	s.Require().NoError(r.Insert(s.Ctx(), newTestIssue("ctrSkip-3.99", "child"), "tester", domain.InsertIssueOpts{}))

	got, err := r.NextCounterID(s.Ctx(), "ctrSkip")
	s.Require().NoError(err)
	s.Equal(4, got, "must ignore child IDs when seeding from max")
}

func (s *testSuite) issueNextCounterIDEmptyPrefix() {
	r := s.issueRepo()
	_, err := r.NextCounterID(s.Ctx(), "")
	s.Require().Error(err)
	s.Contains(err.Error(), "prefix must not be empty")
}

func (s *testSuite) issueCloseSetsFields() {
	s.seedIssueWithStatus("bd-cl-set", types.StatusOpen)

	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-set", "fixed", "tester", "sess-1", domain.IssueTableOpts{}))

	var status, closeReason, closedBySession string
	var closedAt, updatedAt time.Time
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT status, close_reason, closed_by_session, closed_at, updated_at FROM issues WHERE id = ?",
		"bd-cl-set",
	).Scan(&status, &closeReason, &closedBySession, &closedAt, &updatedAt))

	s.Equal(string(types.StatusClosed), status)
	s.Equal("fixed", closeReason)
	s.Equal("sess-1", closedBySession)
	s.False(closedAt.IsZero(), "closed_at should be populated")
	s.False(updatedAt.IsZero(), "updated_at should be populated")
}

func (s *testSuite) issueClosePersistsReason() {
	s.seedIssueWithStatus("bd-cl-rsn", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-rsn", "deduped against bd-x", "alice", "", domain.IssueTableOpts{}))

	var reason string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT close_reason FROM issues WHERE id = ?", "bd-cl-rsn",
	).Scan(&reason))
	s.Equal("deduped against bd-x", reason)
}

func (s *testSuite) issueClosePersistsSession() {
	s.seedIssueWithStatus("bd-cl-sess", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-sess", "done", "alice", "claude-sess-xyz", domain.IssueTableOpts{}))

	var session string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT closed_by_session FROM issues WHERE id = ?", "bd-cl-sess",
	).Scan(&session))
	s.Equal("claude-sess-xyz", session)
}

func (s *testSuite) issueCloseRecordsEvent() {
	s.seedIssueWithStatus("bd-cl-evt", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-evt", "shipped", "alice", "", domain.IssueTableOpts{}))

	var actor, newValue string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT actor, new_value FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-cl-evt", string(types.EventClosed),
	).Scan(&actor, &newValue))
	s.Equal("alice", actor)
	s.Equal("shipped", newValue, "event new_value should carry the close reason")
}

func (s *testSuite) issueCloseMissingID() {
	r := s.issueRepo()
	err := r.Close(s.Ctx(), "bd-cl-missing", "x", "tester", "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, sql.ErrNoRows), "expected sql.ErrNoRows, got %v", err)
}

func (s *testSuite) issueCloseEmptyID() {
	r := s.issueRepo()
	err := r.Close(s.Ctx(), "", "x", "tester", "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "id must not be empty")
}

func (s *testSuite) issueCloseIdempotent() {
	s.seedIssueWithStatus("bd-cl-idem", types.StatusClosed)
	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-idem", "still closed", "tester", "", domain.IssueTableOpts{}),
		"closing an already-closed issue should be a no-op")
}

func (s *testSuite) issueCloseWispRouting() {
	s.seedWispWithStatus("bd-cl-wisp", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-wisp", "wisp done", "tester", "", domain.IssueTableOpts{UseWispsTable: true}))

	var status string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT status FROM wisps WHERE id = ?", "bd-cl-wisp",
	).Scan(&status))
	s.Equal(string(types.StatusClosed), status)

	var wispEvtCount, permEvtCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_events WHERE issue_id = ? AND event_type = ?",
		"bd-cl-wisp", string(types.EventClosed),
	).Scan(&wispEvtCount))
	s.Equal(1, wispEvtCount)
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-cl-wisp", string(types.EventClosed),
	).Scan(&permEvtCount))
	s.Equal(0, permEvtCount, "wisp close event must not write to events")
}

func (s *testSuite) issueCloseUnblocksDependents() {
	s.seedIssueWithStatus("bd-cl-ub-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-cl-ub-blocked", types.StatusOpen)
	s.addDep("bd-cl-ub-blocked", "bd-cl-ub-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-cl-ub-blocked", false, 1)

	r := s.issueRepo()
	s.Require().NoError(r.Close(s.Ctx(), "bd-cl-ub-blocker", "done", "tester", "", domain.IssueTableOpts{}))
	s.Equal(0, s.isBlockedFlag("bd-cl-ub-blocked", false),
		"Close should drive AffectedByStatusChange + Recompute so dependents drop is_blocked")
}

func (s *testSuite) issueDeleteRowGone() {
	s.seedIssueWithStatus("bd-del-row", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-row", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM issues WHERE id = ?", "bd-del-row",
	).Scan(&count))
	s.Equal(0, count)
}

func (s *testSuite) issueDeleteMissingID() {
	r := s.issueRepo()
	err := r.Delete(s.Ctx(), "bd-del-missing", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, sql.ErrNoRows), "expected sql.ErrNoRows, got %v", err)
}

func (s *testSuite) issueDeleteEmptyID() {
	r := s.issueRepo()
	err := r.Delete(s.Ctx(), "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "id must not be empty")
}

func (s *testSuite) issueDeleteWispRouting() {
	s.seedIssueWithStatus("bd-del-wp-same", types.StatusOpen)
	s.seedWispWithStatus("bd-del-wp-same", types.StatusOpen)

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-wp-same", domain.IssueTableOpts{UseWispsTable: true}))

	var wispCount, issueCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisps WHERE id = ?", "bd-del-wp-same",
	).Scan(&wispCount))
	s.Equal(0, wispCount, "wisp row deleted")
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM issues WHERE id = ?", "bd-del-wp-same",
	).Scan(&issueCount))
	s.Equal(1, issueCount, "issues row with same id must be untouched")
}

func (s *testSuite) issueDeleteCascadesOutgoingDeps() {
	s.seedIssueWithStatus("bd-del-od-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-del-od-tgt", types.StatusOpen)
	s.addDep("bd-del-od-src", "bd-del-od-tgt", types.DepBlocks, false)

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-od-src", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE issue_id = ?", "bd-del-od-src",
	).Scan(&count))
	s.Equal(0, count, "outgoing deps should be cleaned by FK CASCADE on issue_id")
}

func (s *testSuite) issueDeleteCascadesIncomingDeps() {
	s.seedIssueWithStatus("bd-del-id-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-del-id-tgt", types.StatusOpen)
	s.addDep("bd-del-id-src", "bd-del-id-tgt", types.DepBlocks, false)

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-id-tgt", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE depends_on_issue_id = ?", "bd-del-id-tgt",
	).Scan(&count))
	s.Equal(0, count, "incoming deps should be cleaned by FK CASCADE on depends_on_issue_id")
}

func (s *testSuite) issueDeleteCascadesLabels() {
	s.seedIssueWithStatus("bd-del-lbl", types.StatusOpen)
	lr := NewLabelSQLRepository(s.Runner())
	s.Require().NoError(lr.Insert(s.Ctx(), "bd-del-lbl", "perf", "tester", domain.LabelOpts{}))

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-lbl", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM labels WHERE issue_id = ?", "bd-del-lbl",
	).Scan(&count))
	s.Equal(0, count, "labels should be cleaned by FK CASCADE")
}

func (s *testSuite) issueDeleteCascadesEvents() {
	s.seedIssueWithStatus("bd-del-evt", types.StatusOpen)
	er := NewEventsSQLRepository(s.Runner())
	s.Require().NoError(er.Record(s.Ctx(), domain.Event{
		IssueID: "bd-del-evt",
		Type:    types.EventCreated,
		Actor:   "tester",
	}, domain.RecordEventOpts{}))

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-evt", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ?", "bd-del-evt",
	).Scan(&count))
	s.Equal(0, count, "events should be cleaned by FK CASCADE")
}

func (s *testSuite) issueDeleteUnblocksDependents() {
	s.seedIssueWithStatus("bd-del-ub-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-del-ub-blocked", types.StatusOpen)
	s.addDep("bd-del-ub-blocked", "bd-del-ub-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-del-ub-blocked", false, 1)

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-ub-blocker", domain.IssueTableOpts{}))
	s.Equal(0, s.isBlockedFlag("bd-del-ub-blocked", false),
		"Delete should drive AffectedByDeletion + Recompute so dependents drop is_blocked")
}

func (s *testSuite) issueDeleteWispScrubsOrphans() {
	s.seedWispWithStatus("bd-del-wo-wisp", types.StatusOpen)
	s.seedIssueWithStatus("bd-del-wo-issue", types.StatusOpen)
	dep := &types.Dependency{
		IssueID:     "bd-del-wo-issue",
		DependsOnID: "bd-del-wo-wisp",
		Type:        types.DepBlocks,
	}
	depRepo := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(depRepo.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	var before int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE depends_on_wisp_id = ?", "bd-del-wo-wisp",
	).Scan(&before))
	s.Equal(1, before, "precondition: dep with depends_on_wisp_id should be present")

	r := s.issueRepo()
	s.Require().NoError(r.Delete(s.Ctx(), "bd-del-wo-wisp", domain.IssueTableOpts{UseWispsTable: true}))

	var after int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE depends_on_wisp_id = ?", "bd-del-wo-wisp",
	).Scan(&after))
	s.Equal(0, after, "wisp delete must scrub dependencies.depends_on_wisp_id refs (no FK CASCADE there)")
}

func (s *testSuite) issueClaimUnassigned() {
	s.seedIssueWithStatus("bd-clm-un", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-un", "alice", domain.IssueTableOpts{}))

	var assignee, status string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT assignee, status FROM issues WHERE id = ?", "bd-clm-un",
	).Scan(&assignee, &status))
	s.Equal("alice", assignee)
	s.Equal(string(types.StatusInProgress), status)
}

func (s *testSuite) issueClaimSetsStartedAt() {
	s.seedIssueWithStatus("bd-clm-start", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-start", "alice", domain.IssueTableOpts{}))

	var startedAt sql.NullTime
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT started_at FROM issues WHERE id = ?", "bd-clm-start",
	).Scan(&startedAt))
	s.True(startedAt.Valid, "started_at should be set on first claim")
}

func (s *testSuite) issueClaimPreservesStartedAt() {
	s.seedIssueWithStatus("bd-clm-keep", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-keep", "alice", domain.IssueTableOpts{}))

	var first time.Time
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT started_at FROM issues WHERE id = ?", "bd-clm-keep",
	).Scan(&first))

	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE issues SET status = 'open', assignee = '' WHERE id = ?", "bd-clm-keep")
	s.Require().NoError(err)
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-keep", "alice", domain.IssueTableOpts{}))

	var second time.Time
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT started_at FROM issues WHERE id = ?", "bd-clm-keep",
	).Scan(&second))
	s.Equal(first.Unix(), second.Unix(), "started_at must be preserved across re-claims")
}

func (s *testSuite) issueClaimIdempotent() {
	s.seedIssueWithStatus("bd-clm-idem", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-idem", "alice", domain.IssueTableOpts{}))
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-idem", "alice", domain.IssueTableOpts{}),
		"re-claim by same actor must be idempotent")
}

func (s *testSuite) issueClaimAlreadyClaimedByOther() {
	s.seedIssueWithStatus("bd-clm-other", types.StatusOpen)
	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE issues SET assignee = ? WHERE id = ?", "alice", "bd-clm-other")
	s.Require().NoError(err)

	r := s.issueRepo()
	err = r.Claim(s.Ctx(), "bd-clm-other", "bob", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, domain.ErrAlreadyClaimed), "expected ErrAlreadyClaimed, got %v", err)
	s.Contains(err.Error(), "alice", "error should name the current claimant")
}

func (s *testSuite) issueClaimNotClaimableClosed() {
	s.seedIssueWithStatus("bd-clm-closed", types.StatusClosed)
	r := s.issueRepo()
	err := r.Claim(s.Ctx(), "bd-clm-closed", "alice", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, domain.ErrNotClaimable), "expected ErrNotClaimable, got %v", err)
}

func (s *testSuite) issueClaimNotClaimableInProgress() {
	s.seedIssueWithStatus("bd-clm-inprog", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-inprog", "alice", domain.IssueTableOpts{}))

	err := r.Claim(s.Ctx(), "bd-clm-inprog", "bob", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.True(errors.Is(err, domain.ErrAlreadyClaimed), "expected ErrAlreadyClaimed when in-progress by other, got %v", err)
}

func (s *testSuite) issueClaimAfterUnassign() {
	s.seedIssueWithStatus("bd-clm-reassign", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-reassign", "alice", domain.IssueTableOpts{}))

	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE issues SET status = 'open', assignee = '' WHERE id = ?", "bd-clm-reassign")
	s.Require().NoError(err)

	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-reassign", "bob", domain.IssueTableOpts{}),
		"after unassign + reopen, a different actor must be able to claim")
	var assignee string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT assignee FROM issues WHERE id = ?", "bd-clm-reassign",
	).Scan(&assignee))
	s.Equal("bob", assignee)
}

func (s *testSuite) issueClaimRecordsEvent() {
	s.seedIssueWithStatus("bd-clm-evt", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-evt", "alice", domain.IssueTableOpts{}))

	var actor, oldValue, newValue string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT actor, old_value, new_value FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-clm-evt", "claimed",
	).Scan(&actor, &oldValue, &newValue))

	s.Equal("alice", actor)

	var oldIssue map[string]any
	s.Require().NoError(json.Unmarshal([]byte(oldValue), &oldIssue))
	s.Equal("bd-clm-evt", oldIssue["id"], "old_value should be the full prior issue JSON")
	s.Equal(string(types.StatusOpen), oldIssue["status"])

	var newMap map[string]any
	s.Require().NoError(json.Unmarshal([]byte(newValue), &newMap))
	s.Equal("alice", newMap["assignee"])
	s.Equal("in_progress", newMap["status"])
}

func (s *testSuite) issueClaimIdempotentNoEvent() {
	s.seedIssueWithStatus("bd-clm-noevt", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-noevt", "alice", domain.IssueTableOpts{}))
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-noevt", "alice", domain.IssueTableOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-clm-noevt", "claimed",
	).Scan(&count))
	s.Equal(1, count, "idempotent re-claim must not emit a second claimed event")
}

func (s *testSuite) issueClaimEmptyID() {
	r := s.issueRepo()
	err := r.Claim(s.Ctx(), "", "alice", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "id must not be empty")
}

func (s *testSuite) issueClaimEmptyActor() {
	s.seedIssueWithStatus("bd-clm-noactor", types.StatusOpen)
	r := s.issueRepo()
	err := r.Claim(s.Ctx(), "bd-clm-noactor", "", domain.IssueTableOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "actor must not be empty")
}

func (s *testSuite) issueClaimWispRouting() {
	s.seedWispWithStatus("bd-clm-wisp", types.StatusOpen)
	r := s.issueRepo()
	s.Require().NoError(r.Claim(s.Ctx(), "bd-clm-wisp", "alice", domain.IssueTableOpts{UseWispsTable: true}))

	var assignee, status string
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT assignee, status FROM wisps WHERE id = ?", "bd-clm-wisp",
	).Scan(&assignee, &status))
	s.Equal("alice", assignee)
	s.Equal(string(types.StatusInProgress), status)

	var wispEvtCount, permEvtCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_events WHERE issue_id = ? AND event_type = ?",
		"bd-clm-wisp", "claimed",
	).Scan(&wispEvtCount))
	s.Equal(1, wispEvtCount, "claim event routed to wisp_events")
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = ?",
		"bd-clm-wisp", "claimed",
	).Scan(&permEvtCount))
	s.Equal(0, permEvtCount, "wisp claim must not write to events")
}

func (s *testSuite) countTableRows(table string) int {
	var n int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM "+table,
	).Scan(&n))
	return n
}

func (s *testSuite) rowExists(table, id string) bool {
	var n int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM "+table+" WHERE id = ?", id,
	).Scan(&n))
	return n > 0
}

func (s *testSuite) issueDeleteBatchEmpty() {
	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), nil, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Require().NotNil(res)
	s.Equal(0, res.DeletedCount)
	s.Equal(0, res.DependenciesCount)
	s.Equal(0, res.LabelsCount)
	s.Equal(0, res.EventsCount)
}

func (s *testSuite) issueDeleteBatchSingleLeaf() {
	s.seedIssueWithStatus("bd-db-leaf", types.StatusOpen)
	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-leaf"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(1, res.DeletedCount)
	s.False(s.rowExists("issues", "bd-db-leaf"))
}

func (s *testSuite) issueDeleteBatchCascadesParentChild() {
	s.seedIssueWithStatus("bd-db-pc-root", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-pc-mid", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-pc-leaf", types.StatusOpen)
	s.addDep("bd-db-pc-mid", "bd-db-pc-root", types.DepParentChild, false)
	s.addDep("bd-db-pc-leaf", "bd-db-pc-mid", types.DepParentChild, false)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-pc-root"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(3, res.DeletedCount, "cascade should walk parent-child descendants")
	s.False(s.rowExists("issues", "bd-db-pc-root"))
	s.False(s.rowExists("issues", "bd-db-pc-mid"))
	s.False(s.rowExists("issues", "bd-db-pc-leaf"))
}

func (s *testSuite) issueDeleteBatchCascadesBlocks() {
	s.seedIssueWithStatus("bd-db-bl-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-bl-blocker1", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-bl-blocker2", types.StatusOpen)
	s.addDep("bd-db-bl-blocker1", "bd-db-bl-target", types.DepBlocks, false)
	s.addDep("bd-db-bl-blocker2", "bd-db-bl-blocker1", types.DepBlocks, false)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-bl-target"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(3, res.DeletedCount, "cascade should walk transitive blocking dependents")
}

func (s *testSuite) issueDeleteBatchCircular() {
	s.seedIssueWithStatus("bd-db-circ-a", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-circ-b", types.StatusOpen)
	s.addDep("bd-db-circ-b", "bd-db-circ-a", types.DepParentChild, false)
	s.addDep("bd-db-circ-a", "bd-db-circ-b", types.DepRelated, false)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-circ-a"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err, "circular deps must not deadlock the BFS")
	s.Equal(2, res.DeletedCount)
}

func (s *testSuite) issueDeleteBatchDryRun() {
	s.seedIssueWithStatus("bd-db-dry-root", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-dry-child", types.StatusOpen)
	s.addDep("bd-db-dry-child", "bd-db-dry-root", types.DepParentChild, false)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-dry-root"}, false, true, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(2, res.DeletedCount, "dry-run still reports what would be deleted")
	s.Equal(1, res.DependenciesCount, "the parent-child dep is in the plan")
	s.True(s.rowExists("issues", "bd-db-dry-root"), "dry-run must not write")
	s.True(s.rowExists("issues", "bd-db-dry-child"), "dry-run must not write")
}

func (s *testSuite) issueDeleteBatchCountsLabelsEvents() {
	s.seedIssueWithStatus("bd-db-cnt", types.StatusOpen)
	lr := NewLabelSQLRepository(s.Runner())
	s.Require().NoError(lr.Insert(s.Ctx(), "bd-db-cnt", "perf", "tester", domain.LabelOpts{}))
	s.Require().NoError(lr.Insert(s.Ctx(), "bd-db-cnt", "ux", "tester", domain.LabelOpts{}))
	er := NewEventsSQLRepository(s.Runner())
	s.Require().NoError(er.Record(s.Ctx(), domain.Event{
		IssueID: "bd-db-cnt", Type: types.EventCreated, Actor: "tester",
	}, domain.RecordEventOpts{}))

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-cnt"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(2, res.LabelsCount)
	s.GreaterOrEqual(res.EventsCount, 3, "labels + events created should be counted")
}

func (s *testSuite) issueDeleteBatchMixedInput() {
	s.seedIssueWithStatus("bd-db-mix-i", types.StatusOpen)
	s.seedWispWithStatus("bd-db-mix-w", types.StatusOpen)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(),
		[]string{"bd-db-mix-i", "bd-db-mix-w"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(2, res.DeletedCount, "partition routes each id to its home table")
	s.False(s.rowExists("issues", "bd-db-mix-i"))
	s.False(s.rowExists("wisps", "bd-db-mix-w"))
}

func (s *testSuite) issueDeleteBatchCascadeIntoWisps() {
	s.seedIssueWithStatus("bd-db-civ-root", types.StatusOpen)
	s.seedWispWithStatus("bd-db-civ-wisp", types.StatusOpen)
	dep := &types.Dependency{
		IssueID:     "bd-db-civ-wisp",
		DependsOnID: "bd-db-civ-root",
		Type:        types.DepBlocks,
	}
	depRepo := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(depRepo.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-civ-root"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(2, res.DeletedCount, "cascade should cross the issue→wisp dep table boundary")
	s.False(s.rowExists("wisps", "bd-db-civ-wisp"))
}

func (s *testSuite) issueDeleteBatchWispOrphanScrub() {
	s.seedWispWithStatus("bd-db-os-wisp", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-os-issue", types.StatusOpen)
	dep := &types.Dependency{
		IssueID:     "bd-db-os-issue",
		DependsOnID: "bd-db-os-wisp",
		Type:        types.DepBlocks,
	}
	depRepo := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(depRepo.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	var before int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE depends_on_wisp_id = ?", "bd-db-os-wisp",
	).Scan(&before))
	s.Equal(1, before)

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-os-wisp"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.GreaterOrEqual(res.DeletedCount, 1)

	var after int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE depends_on_wisp_id = ?", "bd-db-os-wisp",
	).Scan(&after))
	s.Equal(0, after, "wisp batch delete must scrub depends_on_wisp_id refs")
}

func (s *testSuite) issueDeleteBatchRecomputesExternal() {
	s.seedIssueWithStatus("bd-db-ext-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-db-ext-blocked", types.StatusOpen)
	s.addDep("bd-db-ext-blocked", "bd-db-ext-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-db-ext-blocked", false, 1)

	r := s.issueRepo()
	_, err := r.DeleteBatch(s.Ctx(), []string{"bd-db-ext-blocker"}, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)

	s.False(s.rowExists("issues", "bd-db-ext-blocker"))
	s.False(s.rowExists("issues", "bd-db-ext-blocked"),
		"the blocked issue cascades along with its blocker, since cascade follows incoming dep edges")
}

func (s *testSuite) issueDeleteBatchOverBatchSize() {
	const n = deleteBatchSize + 25
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("bd-db-batch-%d", i)
		s.seedIssueWithStatus(ids[i], types.StatusOpen)
	}

	r := s.issueRepo()
	res, err := r.DeleteBatch(s.Ctx(), ids, false, false, domain.IssueTableOpts{})
	s.Require().NoError(err)
	s.Equal(n, res.DeletedCount, "batches larger than deleteBatchSize must complete")

	for _, id := range ids {
		s.False(s.rowExists("issues", id))
	}
}
