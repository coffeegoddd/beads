package db

import (
	"sort"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func (s *testSuite) TestBlockedStateSQLRepository() {
	s.Run("Recompute", func() {
		s.Run("MarkBlockedByOpenBlocker", s.blockedRecomputeMarkBlocked)
		s.Run("UnmarkAfterBlockerClosed", s.blockedRecomputeUnmark)
		s.Run("ParentChildCascadeMultiLevel", s.blockedRecomputeParentChildCascade)
		s.Run("WaitsForAllChildrenGate", s.blockedRecomputeAllChildrenGate)
		s.Run("WaitsForAnyChildrenGate", s.blockedRecomputeAnyChildrenGate)
		s.Run("WispIsBlockedByIssue", s.blockedRecomputeWispBlockedByIssue)
		s.Run("IssueIsBlockedByWisp", s.blockedRecomputeIssueBlockedByWisp)
		s.Run("EmptyAffectedNoOp", s.blockedRecomputeEmptyNoOp)
		s.Run("ClosedIssueIsNeverBlocked", s.blockedRecomputeClosedNeverBlocked)
		s.Run("PinnedIssueIsNeverBlocked", s.blockedRecomputePinnedNeverBlocked)
		s.Run("ConditionalBlocksCountsAsBlocker", s.blockedRecomputeConditionalBlocks)
	})
	s.Run("MarkOnly", func() {
		s.Run("MarksNewBlocker", s.blockedMarkOnlyMarks)
		s.Run("DoesNotUnmarkWhenBlockerClosed", s.blockedMarkOnlyNeverUnmarks)
		s.Run("EmptyAffectedNoOp", s.blockedMarkOnlyEmptyNoOp)
	})
	s.Run("AffectedByStatusChange", func() {
		s.Run("SelfAlwaysIncluded", s.affectedStatusSelf)
		s.Run("BlockingDependersSeeded", s.affectedStatusBlockingDependers)
		s.Run("WaitsForViaParentSeeded", s.affectedStatusWaitsForViaParent)
		s.Run("ParentChildDescendantsExpanded", s.affectedStatusDescendantsExpanded)
		s.Run("WispSourceVariant", s.affectedStatusWispSource)
		s.Run("LoneRowReturnsOnlySelf", s.affectedStatusLoneRow)
		s.Run("EmptyIDRejected", s.affectedStatusEmptyID)
	})
	s.Run("AffectedByDepChange", func() {
		s.Run("BlocksSourceSeeded", s.affectedDepBlocksSource)
		s.Run("ConditionalBlocksSourceSeeded", s.affectedDepConditionalBlocksSource)
		s.Run("WaitsForSourceSeeded", s.affectedDepWaitsForSource)
		s.Run("ParentChildSourceSeeded", s.affectedDepParentChildSource)
		s.Run("ParentChildLoadsWaitersOnTarget", s.affectedDepParentChildWaitersOnTarget)
		s.Run("UnrelatedTypeReturnsEmpty", s.affectedDepUnrelatedTypeEmpty)
		s.Run("WispSourceVariant", s.affectedDepWispSource)
		s.Run("EmptySourceRejected", s.affectedDepEmptySource)
	})
	s.Run("AffectedByDeletion", func() {
		s.Run("EmptyInputNoOp", s.affectedDeletionEmpty)
		s.Run("BlockingDependersIncluded", s.affectedDeletionBlockingDependers)
		s.Run("WaitsForWaitersIncluded", s.affectedDeletionWaitsForWaiters)
		s.Run("WaitersOnParentOfDeletedIncluded", s.affectedDeletionWaitersOnParent)
		s.Run("ImmediateChildrenIncluded", s.affectedDeletionChildren)
		s.Run("DescendantsExpanded", s.affectedDeletionDescendants)
		s.Run("MixedIssueAndWispSeeds", s.affectedDeletionMixed)
	})
}

func (s *testSuite) blockedStateRepo() domain.BlockedStateSQLRepository {
	return NewBlockedStateSQLRepository(s.Runner())
}

func (s *testSuite) seedIssueWithStatus(id string, status types.Status) {
	_, err := s.Runner().ExecContext(s.Ctx(), `
		INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status)
		VALUES (?, ?, '', '', '', '', ?)
	`, id, "seed", string(status))
	s.Require().NoError(err)
}

func (s *testSuite) seedWispWithStatus(id string, status types.Status) {
	_, err := s.Runner().ExecContext(s.Ctx(),
		"INSERT INTO wisps (id, title, status) VALUES (?, ?, ?)",
		id, "seed-wisp", string(status))
	s.Require().NoError(err)
}

func (s *testSuite) addDep(issueID, dependsOnID string, depType types.DependencyType, useWisps bool) {
	s.addDepWithMetadata(issueID, dependsOnID, depType, "", useWisps)
}

func (s *testSuite) addDepWithMetadata(issueID, dependsOnID string, depType types.DependencyType, metadata string, useWisps bool) {
	if metadata == "" {
		metadata = "{}"
	}
	dep := &types.Dependency{
		IssueID:     issueID,
		DependsOnID: dependsOnID,
		Type:        depType,
		Metadata:    metadata,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: useWisps}))
}

func (s *testSuite) isBlockedFlag(id string, useWisps bool) int {
	table := "issues"
	if useWisps {
		table = "wisps"
	}
	var v int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT is_blocked FROM "+table+" WHERE id = ?", id,
	).Scan(&v))
	return v
}

func (s *testSuite) setIsBlocked(id string, useWisps bool, value int) {
	table := "issues"
	if useWisps {
		table = "wisps"
	}
	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE "+table+" SET is_blocked = ? WHERE id = ?", value, id,
	)
	s.Require().NoError(err)
}

func (s *testSuite) setStatus(id string, useWisps bool, status types.Status) {
	table := "issues"
	if useWisps {
		table = "wisps"
	}
	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE "+table+" SET status = ? WHERE id = ?", string(status), id,
	)
	s.Require().NoError(err)
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func (s *testSuite) blockedRecomputeMarkBlocked() {
	s.seedIssueWithStatus("bd-bs-mb-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-mb-blocked", types.StatusOpen)
	s.addDep("bd-bs-mb-blocked", "bd-bs-mb-blocker", types.DepBlocks, false)

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-mb-blocked", "bd-bs-mb-blocker"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-mb-blocked", false))
	s.Equal(0, s.isBlockedFlag("bd-bs-mb-blocker", false))
}

func (s *testSuite) blockedRecomputeUnmark() {
	s.seedIssueWithStatus("bd-bs-un-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-un-blocked", types.StatusOpen)
	s.addDep("bd-bs-un-blocked", "bd-bs-un-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-bs-un-blocked", false, 1)

	s.setStatus("bd-bs-un-blocker", false, types.StatusClosed)
	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-un-blocked", "bd-bs-un-blocker"},
	}))
	s.Equal(0, s.isBlockedFlag("bd-bs-un-blocked", false))
}

func (s *testSuite) blockedRecomputeParentChildCascade() {
	s.seedIssueWithStatus("bd-bs-pc-h", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-pc-g", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-pc-p", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-pc-c", types.StatusOpen)
	s.addDep("bd-bs-pc-g", "bd-bs-pc-h", types.DepBlocks, false)
	s.addDep("bd-bs-pc-p", "bd-bs-pc-g", types.DepParentChild, false)
	s.addDep("bd-bs-pc-c", "bd-bs-pc-p", types.DepParentChild, false)

	ids := []string{"bd-bs-pc-h", "bd-bs-pc-g", "bd-bs-pc-p", "bd-bs-pc-c"}
	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(0, s.isBlockedFlag("bd-bs-pc-h", false))
	s.Equal(1, s.isBlockedFlag("bd-bs-pc-g", false))
	s.Equal(1, s.isBlockedFlag("bd-bs-pc-p", false))
	s.Equal(1, s.isBlockedFlag("bd-bs-pc-c", false))

	s.setStatus("bd-bs-pc-h", false, types.StatusClosed)
	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(0, s.isBlockedFlag("bd-bs-pc-g", false))
	s.Equal(0, s.isBlockedFlag("bd-bs-pc-p", false))
	s.Equal(0, s.isBlockedFlag("bd-bs-pc-c", false))
}

func (s *testSuite) blockedRecomputeAllChildrenGate() {
	s.seedIssueWithStatus("bd-bs-ac-w", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-ac-s", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-ac-c1", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-ac-c2", types.StatusOpen)
	s.addDepWithMetadata("bd-bs-ac-w", "bd-bs-ac-s", types.DepWaitsFor, `{"gate":"all-children"}`, false)
	s.addDep("bd-bs-ac-c1", "bd-bs-ac-s", types.DepParentChild, false)
	s.addDep("bd-bs-ac-c2", "bd-bs-ac-s", types.DepParentChild, false)

	r := s.blockedStateRepo()
	ids := []string{"bd-bs-ac-w", "bd-bs-ac-s", "bd-bs-ac-c1", "bd-bs-ac-c2"}
	s.Require().NoError(r.Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(1, s.isBlockedFlag("bd-bs-ac-w", false), "waiter blocked while any child open")

	s.setStatus("bd-bs-ac-c1", false, types.StatusClosed)
	s.Require().NoError(r.Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(1, s.isBlockedFlag("bd-bs-ac-w", false), "waiter still blocked: c2 still open")

	s.setStatus("bd-bs-ac-c2", false, types.StatusClosed)
	s.Require().NoError(r.Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(0, s.isBlockedFlag("bd-bs-ac-w", false), "all children closed unblocks all-children gate")
}

func (s *testSuite) blockedRecomputeAnyChildrenGate() {
	s.seedIssueWithStatus("bd-bs-an-w", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-an-s", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-an-c1", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-an-c2", types.StatusOpen)
	s.addDepWithMetadata("bd-bs-an-w", "bd-bs-an-s", types.DepWaitsFor, `{"gate":"any-children"}`, false)
	s.addDep("bd-bs-an-c1", "bd-bs-an-s", types.DepParentChild, false)
	s.addDep("bd-bs-an-c2", "bd-bs-an-s", types.DepParentChild, false)

	r := s.blockedStateRepo()
	ids := []string{"bd-bs-an-w", "bd-bs-an-s", "bd-bs-an-c1", "bd-bs-an-c2"}
	s.Require().NoError(r.Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(1, s.isBlockedFlag("bd-bs-an-w", false), "waiter blocked: no children closed yet")

	s.setStatus("bd-bs-an-c1", false, types.StatusClosed)
	s.Require().NoError(r.Recompute(s.Ctx(), domain.AffectedIDs{IssueIDs: ids}))
	s.Equal(0, s.isBlockedFlag("bd-bs-an-w", false), "one closed child unblocks any-children gate")
}

func (s *testSuite) blockedRecomputeWispBlockedByIssue() {
	s.seedIssueWithStatus("bd-bs-wbi-blocker", types.StatusOpen)
	s.seedWispWithStatus("bd-bs-wbi-wisp", types.StatusOpen)
	s.addDep("bd-bs-wbi-wisp", "bd-bs-wbi-blocker", types.DepBlocks, true)

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		WispIDs: []string{"bd-bs-wbi-wisp"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-wbi-wisp", true))
}

func (s *testSuite) blockedRecomputeIssueBlockedByWisp() {
	s.seedIssueWithStatus("bd-bs-ibw-issue", types.StatusOpen)
	s.seedWispWithStatus("bd-bs-ibw-wisp-blocker", types.StatusOpen)
	dep := &types.Dependency{
		IssueID:     "bd-bs-ibw-issue",
		DependsOnID: "bd-bs-ibw-wisp-blocker",
		Type:        types.DepBlocks,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-ibw-issue"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-ibw-issue", false), "issue blocked when its blocker lives in wisps")
}

func (s *testSuite) blockedRecomputeEmptyNoOp() {
	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{}))
}

func (s *testSuite) blockedRecomputeClosedNeverBlocked() {
	s.seedIssueWithStatus("bd-bs-cn-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-cn-closed", types.StatusClosed)
	s.addDep("bd-bs-cn-closed", "bd-bs-cn-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-bs-cn-closed", false, 1)

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-cn-closed"},
	}))
	s.Equal(0, s.isBlockedFlag("bd-bs-cn-closed", false), "closed rows are never is_blocked=1")
}

func (s *testSuite) blockedRecomputePinnedNeverBlocked() {
	s.seedIssueWithStatus("bd-bs-pin-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-pin-pinned", types.StatusPinned)
	s.addDep("bd-bs-pin-pinned", "bd-bs-pin-blocker", types.DepBlocks, false)

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-pin-pinned"},
	}))
	s.Equal(0, s.isBlockedFlag("bd-bs-pin-pinned", false), "pinned rows are never is_blocked=1")
}

func (s *testSuite) blockedRecomputeConditionalBlocks() {
	s.seedIssueWithStatus("bd-bs-cb-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-cb-blocked", types.StatusOpen)
	s.addDep("bd-bs-cb-blocked", "bd-bs-cb-blocker", types.DepConditionalBlocks, false)

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-cb-blocked"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-cb-blocked", false))
}

func (s *testSuite) blockedMarkOnlyMarks() {
	s.seedIssueWithStatus("bd-bs-mom-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-mom-blocked", types.StatusOpen)
	s.addDep("bd-bs-mom-blocked", "bd-bs-mom-blocker", types.DepBlocks, false)

	s.Require().NoError(s.blockedStateRepo().MarkOnly(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-mom-blocked"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-mom-blocked", false))
}

func (s *testSuite) blockedMarkOnlyNeverUnmarks() {
	s.seedIssueWithStatus("bd-bs-mnu-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-bs-mnu-blocked", types.StatusOpen)
	s.addDep("bd-bs-mnu-blocked", "bd-bs-mnu-blocker", types.DepBlocks, false)
	s.setIsBlocked("bd-bs-mnu-blocked", false, 1)

	s.setStatus("bd-bs-mnu-blocker", false, types.StatusClosed)
	s.Require().NoError(s.blockedStateRepo().MarkOnly(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-mnu-blocked"},
	}))
	s.Equal(1, s.isBlockedFlag("bd-bs-mnu-blocked", false), "MarkOnly must not unmark stale is_blocked flags")

	s.Require().NoError(s.blockedStateRepo().Recompute(s.Ctx(), domain.AffectedIDs{
		IssueIDs: []string{"bd-bs-mnu-blocked"},
	}))
	s.Equal(0, s.isBlockedFlag("bd-bs-mnu-blocked", false), "Recompute then clears the stale flag")
}

func (s *testSuite) blockedMarkOnlyEmptyNoOp() {
	s.Require().NoError(s.blockedStateRepo().MarkOnly(s.Ctx(), domain.AffectedIDs{}))
}

func (s *testSuite) affectedStatusSelf() {
	s.seedIssueWithStatus("bd-as-self", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-self", false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-as-self"}, got.IssueIDs)
	s.Empty(got.WispIDs)
}

func (s *testSuite) affectedStatusBlockingDependers() {
	s.seedIssueWithStatus("bd-as-bd-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-bd-i", types.StatusOpen)
	s.seedWispWithStatus("bd-as-bd-w", types.StatusOpen)
	s.addDep("bd-as-bd-i", "bd-as-bd-target", types.DepBlocks, false)
	s.addDep("bd-as-bd-w", "bd-as-bd-target", types.DepConditionalBlocks, true)

	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-bd-target", false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-as-bd-i", "bd-as-bd-target"}, sortedCopy(got.IssueIDs))
	s.Equal([]string{"bd-as-bd-w"}, got.WispIDs)
}

func (s *testSuite) affectedStatusWaitsForViaParent() {
	s.seedIssueWithStatus("bd-as-wp-parent", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-wp-child", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-wp-waiter", types.StatusOpen)
	s.addDep("bd-as-wp-child", "bd-as-wp-parent", types.DepParentChild, false)
	s.addDepWithMetadata("bd-as-wp-waiter", "bd-as-wp-parent", types.DepWaitsFor, `{"gate":"all-children"}`, false)

	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-wp-child", false)
	s.Require().NoError(err)
	s.Contains(got.IssueIDs, "bd-as-wp-child")
	s.Contains(got.IssueIDs, "bd-as-wp-waiter")
}

func (s *testSuite) affectedStatusDescendantsExpanded() {
	s.seedIssueWithStatus("bd-as-d-root", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-d-mid", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-d-leaf", types.StatusOpen)
	s.seedWispWithStatus("bd-as-d-wisp-leaf", types.StatusOpen)
	s.addDep("bd-as-d-mid", "bd-as-d-root", types.DepParentChild, false)
	s.addDep("bd-as-d-leaf", "bd-as-d-mid", types.DepParentChild, false)
	s.addDep("bd-as-d-wisp-leaf", "bd-as-d-mid", types.DepParentChild, true)

	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-d-root", false)
	s.Require().NoError(err)
	s.Equal(
		[]string{"bd-as-d-leaf", "bd-as-d-mid", "bd-as-d-root"},
		sortedCopy(got.IssueIDs),
	)
	s.Equal([]string{"bd-as-d-wisp-leaf"}, got.WispIDs)
}

func (s *testSuite) affectedStatusWispSource() {
	s.seedWispWithStatus("bd-as-ws-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-as-ws-blocker-issue", types.StatusOpen)
	s.seedWispWithStatus("bd-as-ws-blocker-wisp", types.StatusOpen)
	dep := &types.Dependency{
		IssueID:     "bd-as-ws-blocker-issue",
		DependsOnID: "bd-as-ws-target",
		Type:        types.DepBlocks,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))
	dep2 := &types.Dependency{
		IssueID:     "bd-as-ws-blocker-wisp",
		DependsOnID: "bd-as-ws-target",
		Type:        types.DepBlocks,
	}
	s.Require().NoError(r.Insert(s.Ctx(), dep2, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-ws-target", true)
	s.Require().NoError(err)
	s.Equal([]string{"bd-as-ws-blocker-issue"}, got.IssueIDs)
	s.Equal([]string{"bd-as-ws-blocker-wisp", "bd-as-ws-target"}, sortedCopy(got.WispIDs))
}

func (s *testSuite) affectedStatusLoneRow() {
	s.seedIssueWithStatus("bd-as-lone", types.StatusOpen)
	got, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "bd-as-lone", false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-as-lone"}, got.IssueIDs)
	s.Empty(got.WispIDs)
}

func (s *testSuite) affectedStatusEmptyID() {
	_, err := s.blockedStateRepo().AffectedByStatusChange(s.Ctx(), "", false)
	s.Require().Error(err)
}

func (s *testSuite) affectedDepBlocksSource() {
	s.seedIssueWithStatus("bd-ad-b-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-b-tgt", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-b-src", "bd-ad-b-tgt", types.DepBlocks, false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-ad-b-src"}, got.IssueIDs)
	s.Empty(got.WispIDs)
}

func (s *testSuite) affectedDepConditionalBlocksSource() {
	s.seedIssueWithStatus("bd-ad-cb-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-cb-tgt", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-cb-src", "bd-ad-cb-tgt", types.DepConditionalBlocks, false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-ad-cb-src"}, got.IssueIDs)
}

func (s *testSuite) affectedDepWaitsForSource() {
	s.seedIssueWithStatus("bd-ad-wf-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-wf-tgt", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-wf-src", "bd-ad-wf-tgt", types.DepWaitsFor, false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-ad-wf-src"}, got.IssueIDs)
}

func (s *testSuite) affectedDepParentChildSource() {
	s.seedIssueWithStatus("bd-ad-pc-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-pc-tgt", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-pc-grandchild", types.StatusOpen)
	s.addDep("bd-ad-pc-grandchild", "bd-ad-pc-src", types.DepParentChild, false)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-pc-src", "bd-ad-pc-tgt", types.DepParentChild, false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-ad-pc-grandchild", "bd-ad-pc-src"}, sortedCopy(got.IssueIDs))
}

func (s *testSuite) affectedDepParentChildWaitersOnTarget() {
	s.seedIssueWithStatus("bd-ad-pcw-child", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-pcw-parent", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-pcw-waiter-issue", types.StatusOpen)
	s.seedWispWithStatus("bd-ad-pcw-waiter-wisp", types.StatusOpen)
	s.addDepWithMetadata("bd-ad-pcw-waiter-issue", "bd-ad-pcw-parent", types.DepWaitsFor, `{"gate":"any-children"}`, false)
	dep := &types.Dependency{
		IssueID:     "bd-ad-pcw-waiter-wisp",
		DependsOnID: "bd-ad-pcw-parent",
		Type:        types.DepWaitsFor,
		Metadata:    `{"gate":"any-children"}`,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-pcw-child", "bd-ad-pcw-parent", types.DepParentChild, false)
	s.Require().NoError(err)
	s.Equal([]string{"bd-ad-pcw-child", "bd-ad-pcw-waiter-issue"}, sortedCopy(got.IssueIDs))
	s.Equal([]string{"bd-ad-pcw-waiter-wisp"}, got.WispIDs)
}

func (s *testSuite) affectedDepUnrelatedTypeEmpty() {
	s.seedIssueWithStatus("bd-ad-rel-a", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-rel-b", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-rel-a", "bd-ad-rel-b", types.DepRelated, false)
	s.Require().NoError(err)
	s.Empty(got.IssueIDs)
	s.Empty(got.WispIDs)
}

func (s *testSuite) affectedDepWispSource() {
	s.seedWispWithStatus("bd-ad-ws-src", types.StatusOpen)
	s.seedIssueWithStatus("bd-ad-ws-tgt", types.StatusOpen)

	got, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "bd-ad-ws-src", "bd-ad-ws-tgt", types.DepBlocks, true)
	s.Require().NoError(err)
	s.Empty(got.IssueIDs)
	s.Equal([]string{"bd-ad-ws-src"}, got.WispIDs)
}

func (s *testSuite) affectedDepEmptySource() {
	_, err := s.blockedStateRepo().AffectedByDepChange(s.Ctx(), "", "bd-x", types.DepBlocks, false)
	s.Require().Error(err)
}

func (s *testSuite) affectedDeletionEmpty() {
	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(), nil, nil)
	s.Require().NoError(err)
	s.Empty(got.IssueIDs)
	s.Empty(got.WispIDs)
}

func (s *testSuite) affectedDeletionBlockingDependers() {
	s.seedIssueWithStatus("bd-adel-bd-deleted-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-bd-deleted-w", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-bd-i-blocker", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-bd-w-blocker", types.StatusOpen)
	s.addDep("bd-adel-bd-i-blocker", "bd-adel-bd-deleted-i", types.DepBlocks, false)
	dep := &types.Dependency{
		IssueID:     "bd-adel-bd-w-blocker",
		DependsOnID: "bd-adel-bd-deleted-w",
		Type:        types.DepBlocks,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(),
		[]string{"bd-adel-bd-deleted-i"},
		[]string{"bd-adel-bd-deleted-w"},
	)
	s.Require().NoError(err)
	s.Equal([]string{"bd-adel-bd-i-blocker"}, got.IssueIDs)
	s.Equal([]string{"bd-adel-bd-w-blocker"}, got.WispIDs)
}

func (s *testSuite) affectedDeletionWaitsForWaiters() {
	s.seedIssueWithStatus("bd-adel-wf-spawner", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-wf-waiter-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-wf-waiter-w", types.StatusOpen)
	s.addDepWithMetadata("bd-adel-wf-waiter-i", "bd-adel-wf-spawner", types.DepWaitsFor, `{"gate":"all-children"}`, false)
	dep := &types.Dependency{
		IssueID:     "bd-adel-wf-waiter-w",
		DependsOnID: "bd-adel-wf-spawner",
		Type:        types.DepWaitsFor,
		Metadata:    `{"gate":"all-children"}`,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(), []string{"bd-adel-wf-spawner"}, nil)
	s.Require().NoError(err)
	s.Equal([]string{"bd-adel-wf-waiter-i"}, got.IssueIDs)
	s.Equal([]string{"bd-adel-wf-waiter-w"}, got.WispIDs)
}

func (s *testSuite) affectedDeletionWaitersOnParent() {
	s.seedIssueWithStatus("bd-adel-wp-parent", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-wp-deleted-child", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-wp-waiter", types.StatusOpen)
	s.addDep("bd-adel-wp-deleted-child", "bd-adel-wp-parent", types.DepParentChild, false)
	s.addDepWithMetadata("bd-adel-wp-waiter", "bd-adel-wp-parent", types.DepWaitsFor, `{"gate":"all-children"}`, false)

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(), []string{"bd-adel-wp-deleted-child"}, nil)
	s.Require().NoError(err)
	s.Contains(got.IssueIDs, "bd-adel-wp-waiter")
}

func (s *testSuite) affectedDeletionChildren() {
	s.seedIssueWithStatus("bd-adel-ch-deleted-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-ch-deleted-w", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-ch-i-child-of-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-ch-w-child-of-i", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-ch-i-child-of-w", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-ch-w-child-of-w", types.StatusOpen)

	s.addDep("bd-adel-ch-i-child-of-i", "bd-adel-ch-deleted-i", types.DepParentChild, false)
	s.addDep("bd-adel-ch-w-child-of-i", "bd-adel-ch-deleted-i", types.DepParentChild, true)
	dep := &types.Dependency{
		IssueID:     "bd-adel-ch-i-child-of-w",
		DependsOnID: "bd-adel-ch-deleted-w",
		Type:        types.DepParentChild,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))
	dep2 := &types.Dependency{
		IssueID:     "bd-adel-ch-w-child-of-w",
		DependsOnID: "bd-adel-ch-deleted-w",
		Type:        types.DepParentChild,
	}
	s.Require().NoError(r.Insert(s.Ctx(), dep2, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(),
		[]string{"bd-adel-ch-deleted-i"},
		[]string{"bd-adel-ch-deleted-w"},
	)
	s.Require().NoError(err)
	s.Equal(
		[]string{"bd-adel-ch-i-child-of-i", "bd-adel-ch-i-child-of-w"},
		sortedCopy(got.IssueIDs),
	)
	s.Equal(
		[]string{"bd-adel-ch-w-child-of-i", "bd-adel-ch-w-child-of-w"},
		sortedCopy(got.WispIDs),
	)
}

func (s *testSuite) affectedDeletionDescendants() {
	s.seedIssueWithStatus("bd-adel-d-deleted", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-d-grandchild-via-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-d-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-d-child-of-blocker", types.StatusOpen)

	s.addDep("bd-adel-d-blocker", "bd-adel-d-deleted", types.DepBlocks, false)
	s.addDep("bd-adel-d-child-of-blocker", "bd-adel-d-blocker", types.DepParentChild, false)
	s.addDep("bd-adel-d-grandchild-via-blocker", "bd-adel-d-child-of-blocker", types.DepParentChild, false)

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(), []string{"bd-adel-d-deleted"}, nil)
	s.Require().NoError(err)
	s.Equal([]string{
		"bd-adel-d-blocker",
		"bd-adel-d-child-of-blocker",
		"bd-adel-d-grandchild-via-blocker",
	}, sortedCopy(got.IssueIDs))
}

func (s *testSuite) affectedDeletionMixed() {
	s.seedIssueWithStatus("bd-adel-m-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-m-w", types.StatusOpen)
	s.seedIssueWithStatus("bd-adel-m-blocker-of-i", types.StatusOpen)
	s.seedWispWithStatus("bd-adel-m-w-child-of-w", types.StatusOpen)
	s.addDep("bd-adel-m-blocker-of-i", "bd-adel-m-i", types.DepBlocks, false)
	dep := &types.Dependency{
		IssueID:     "bd-adel-m-w-child-of-w",
		DependsOnID: "bd-adel-m-w",
		Type:        types.DepParentChild,
	}
	r := NewDependencySQLRepository(s.Runner())
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	got, err := s.blockedStateRepo().AffectedByDeletion(s.Ctx(),
		[]string{"bd-adel-m-i"},
		[]string{"bd-adel-m-w"},
	)
	s.Require().NoError(err)
	s.Equal([]string{"bd-adel-m-blocker-of-i"}, got.IssueIDs)
	s.Equal([]string{"bd-adel-m-w-child-of-w"}, got.WispIDs)
}
