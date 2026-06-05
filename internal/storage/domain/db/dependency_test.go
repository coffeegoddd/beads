package db

import (
	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/types"
)

func (s *testSuite) TestDependencySQLRepository() {
	s.Run("Insert", func() {
		s.Run("RoundTripVisibleViaList", s.depInsertRoundTrip)
		s.Run("RejectsSelfDependency", s.depInsertSelfDep)
		s.Run("RejectsEmptyIDs", s.depInsertEmptyIDs)
		s.Run("SameTypeIsIdempotentMetadataRefresh", s.depInsertIdempotentSameType)
		s.Run("DifferentTypeIsRejected", s.depInsertConflictingType)
		s.Run("MissingTargetIssueFailsFK", s.depInsertFKViolation)
		s.Run("ThreadIDPersists", s.depInsertThreadID)
	})
	s.Run("HasCycle", func() {
		s.Run("StraightLineIsAcyclic", s.depCycleAcyclic)
		s.Run("DirectBackEdgeDetected", s.depCycleDirectBackEdge)
		s.Run("BackEdgeDetected", s.depCycleBackEdge)
		s.Run("NonBlockingEdgesIgnored", s.depCycleIgnoresNonBlocking)
	})
	s.Run("ListByIssueIDs", func() {
		s.Run("EmptySliceReturnsEmptyMaps", s.depListEmpty)
		s.Run("OutgoingOnly", s.depListOutgoing)
		s.Run("IncomingOnly", s.depListIncoming)
		s.Run("BothDirections", s.depListBoth)
		s.Run("TypeFilterApplied", s.depListTypeFilter)
	})
	s.Run("CountsByIssueIDs", func() {
		s.Run("EmptySliceReturnsEmptyMap", s.depCountsEmpty)
		s.Run("CountsBlockingEdgesOnly", s.depCountsBlocksOnly)
		s.Run("ZeroCountsPresentInMap", s.depCountsZeroPresent)
	})
	s.Run("GetBlockingInfo", func() {
		s.Run("EmptyInputReturnsEmptyMaps", s.depBlockingInfoEmpty)
		s.Run("PopulatesBlockedByAndBlocks", s.depBlockingInfoBlockedByAndBlocks)
		s.Run("ParentChildPopulatesParent", s.depBlockingInfoParent)
		s.Run("ClosedBlockerFiltered", s.depBlockingInfoSkipsClosed)
	})
	s.Run("GetBlockingInfoAcrossIssuesAndWisps", func() {
		s.Run("UnionsBothTables", s.depBlockingInfoAcrossUnions)
	})
	s.Run("Wisp", func() {
		s.Run("InsertRoutesToWispDependencies", s.depWispInsertRouting)
		s.Run("ListReadsFromWispDependencies", s.depWispListRouting)
		s.Run("CountsReadFromWispDependencies", s.depWispCountsRouting)
		s.Run("HasCycleSpansBothTables", s.depWispHasCycleCrossTable)
		s.Run("WispDirectBackEdgeDetected", s.depWispDirectBackEdge)
	})
	s.Run("Remove", func() {
		s.Run("ExistingDepRemoved", s.depRemoveExisting)
		s.Run("MissingDepIsNoOp", s.depRemoveMissingNoOp)
		s.Run("EmptyIDsRejected", s.depRemoveEmptyIDs)
		s.Run("RoutesToWispDependencies", s.depRemoveWispRouting)
		s.Run("ExternalTargetVariant", s.depRemoveExternalTarget)
		s.Run("UnblocksDependentAfterBlockingDepRemoved", s.depRemoveUnblocksDependent)
		s.Run("OnlyRemovesNamedPairLeavesOtherEdgesIntact", s.depRemoveLeavesOtherEdges)
	})
	s.Run("ListRecordsForIssue", func() {
		s.Run("EmptyWhenNoDeps", s.depRecordsEmpty)
		s.Run("ReturnsOutgoingWithFields", s.depRecordsOutgoingFields)
		s.Run("TypesFilterHonored", s.depRecordsTypesFilter)
		s.Run("RoutesToWispDependencies", s.depRecordsWispRouting)
		s.Run("EmptyIDRejected", s.depRecordsEmptyID)
	})
	s.Run("ListIssuesDependedOn", func() {
		s.Run("EmptyWhenNoDeps", s.depDependedOnEmpty)
		s.Run("ReturnsIssueTargets", s.depDependedOnIssueTargets)
		s.Run("ReturnsWispTargetsToo", s.depDependedOnWispTargets)
		s.Run("SkipsExternalTargets", s.depDependedOnSkipsExternal)
		s.Run("WispSourceQueriesWispDepsTable", s.depDependedOnWispSource)
		s.Run("EmptyIDRejected", s.depDependedOnEmptyID)
	})
	s.Run("ListIssueDependents", func() {
		s.Run("EmptyWhenNone", s.depDependentsEmpty)
		s.Run("ReturnsIssueDependents", s.depDependentsIssues)
		s.Run("ReturnsMultipleDependents", s.depDependentsMultiple)
		s.Run("WispDependentsIncluded", s.depDependentsWispsIncluded)
		s.Run("WispTargetCollectsAllSourceTables", s.depDependentsWispTarget)
		s.Run("EmptyIDRejected", s.depDependentsEmptyID)
	})
	s.Run("ListDependentsWithMetadata", func() {
		s.Run("EmptyWhenNone", s.depDepMetaEmpty)
		s.Run("AttachesDepTypePerDependent", s.depDepMetaAttachesType)
		s.Run("MultipleTypesAcrossDependents", s.depDepMetaMultipleTypes)
		s.Run("WispDependentsIncluded", s.depDepMetaWispsIncluded)
		s.Run("HydratesFullIssueFields", s.depDepMetaHydratesFields)
		s.Run("EmptyIDRejected", s.depDepMetaEmptyID)
	})
	s.Run("IsBlocked", func() {
		s.Run("NotBlockedWhenNoDeps", s.depIsBlockedNoDeps)
		s.Run("NotBlockedWhenFlagZeroFastPath", s.depIsBlockedFastPath)
		s.Run("BlockedByOpenBlocker", s.depIsBlockedByOpen)
		s.Run("ClosedBlockerFilteredOut", s.depIsBlockedClosedFiltered)
		s.Run("PinnedBlockerFilteredOut", s.depIsBlockedPinnedFiltered)
		s.Run("ConditionalBlocksReportedWithTypeSuffix", s.depIsBlockedConditionalBlocks)
		s.Run("WaitsForReportedWithTypeSuffix", s.depIsBlockedWaitsFor)
		s.Run("MultipleBlockersReturned", s.depIsBlockedMultiple)
		s.Run("WispSourceAutoDetected", s.depIsBlockedWispSource)
		s.Run("MissingIssueReturnsFalse", s.depIsBlockedMissingIssue)
		s.Run("EmptyIDRejected", s.depIsBlockedEmptyID)
	})
	s.Run("ListNewlyUnblockedByClose", func() {
		s.Run("EmptyWhenNoDependents", s.depUnblockedEmpty)
		s.Run("SoleBlockerClosedReturnsDependent", s.depUnblockedSoleBlocker)
		s.Run("OmitsCandidateStillBlockedByOther", s.depUnblockedStillBlocked)
		s.Run("ClosedCandidateExcluded", s.depUnblockedClosedCandidate)
		s.Run("PinnedCandidateExcluded", s.depUnblockedPinnedCandidate)
		s.Run("OnlyBlocksTypeCounted", s.depUnblockedOnlyBlocksType)
		s.Run("WispDependentIncluded", s.depUnblockedWispDependent)
		s.Run("HydratesFullIssueFields", s.depUnblockedHydrates)
		s.Run("MultipleDependentsReturned", s.depUnblockedMultiple)
		s.Run("ClosedBlockerNotCountedAsRemaining", s.depUnblockedClosedNotCountedAsRemaining)
		s.Run("EmptyIDRejected", s.depUnblockedEmptyID)
	})
}

func (s *testSuite) depRepo() domain.DependencySQLRepository {
	return NewDependencySQLRepository(s.Runner())
}

func newDep(issueID, dependsOnID string, t types.DependencyType) *types.Dependency {
	return &types.Dependency{
		IssueID:     issueID,
		DependsOnID: dependsOnID,
		Type:        t,
	}
}

func (s *testSuite) depInsertRoundTrip() {
	s.seedIssueRow("bd-dep-a")
	s.seedIssueRow("bd-dep-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dep-a", "bd-dep-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dep-a"}, domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-dep-a"], 1)
	s.Equal("bd-dep-b", out.Outgoing["bd-dep-a"][0].DependsOnID)
	s.Equal(types.DepBlocks, out.Outgoing["bd-dep-a"][0].Type)
}

func (s *testSuite) depInsertSelfDep() {
	s.seedIssueRow("bd-dep-self")
	err := s.depRepo().Insert(s.Ctx(), newDep("bd-dep-self", "bd-dep-self", types.DepBlocks), "tester", domain.DepInsertOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "cannot depend on itself")
}

func (s *testSuite) depInsertEmptyIDs() {
	r := s.depRepo()
	s.Require().Error(r.Insert(s.Ctx(), newDep("", "bd-x", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().Error(r.Insert(s.Ctx(), newDep("bd-x", "", types.DepBlocks), "tester", domain.DepInsertOpts{}))
}

func (s *testSuite) depInsertIdempotentSameType() {
	s.seedIssueRow("bd-dep-idem-1")
	s.seedIssueRow("bd-dep-idem-2")
	r := s.depRepo()

	dep := newDep("bd-dep-idem-1", "bd-dep-idem-2", types.DepBlocks)
	dep.Metadata = `{"v":1}`
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	// Re-add same edge, new metadata. Should refresh, not error.
	dep.Metadata = `{"v":2}`
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dep-idem-1"}, domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-dep-idem-1"], 1, "duplicate insert should still result in exactly one row")
	s.Equal(`{"v":2}`, out.Outgoing["bd-dep-idem-1"][0].Metadata)
}

func (s *testSuite) depInsertConflictingType() {
	s.seedIssueRow("bd-dep-conf-1")
	s.seedIssueRow("bd-dep-conf-2")
	r := s.depRepo()

	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dep-conf-1", "bd-dep-conf-2", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	err := r.Insert(s.Ctx(), newDep("bd-dep-conf-1", "bd-dep-conf-2", types.DepRelated), "tester", domain.DepInsertOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "already exists with type")
}

func (s *testSuite) depInsertFKViolation() {
	s.seedIssueRow("bd-dep-src")
	err := s.depRepo().Insert(s.Ctx(), newDep("bd-dep-src", "bd-dep-no-such-target", types.DepBlocks), "tester", domain.DepInsertOpts{})
	s.Require().Error(err, "missing target should fail fk_dep_issue_target")
}

func (s *testSuite) depInsertThreadID() {
	s.seedIssueRow("bd-dep-th-1")
	s.seedIssueRow("bd-dep-th-2")
	r := s.depRepo()

	dep := newDep("bd-dep-th-1", "bd-dep-th-2", types.DepRepliesTo)
	dep.ThreadID = "thread-xyz"
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dep-th-1"}, domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-dep-th-1"], 1)
	s.Equal("thread-xyz", out.Outgoing["bd-dep-th-1"][0].ThreadID)
}

func (s *testSuite) depCycleAcyclic() {
	s.seedIssueRow("bd-cy-a")
	s.seedIssueRow("bd-cy-b")
	s.seedIssueRow("bd-cy-c")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-a", "bd-cy-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-b", "bd-cy-c", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	// Adding a -> c is fine.
	cycle, err := r.HasCycle(s.Ctx(), "bd-cy-a", "bd-cy-c")
	s.Require().NoError(err)
	s.False(cycle)
}

func (s *testSuite) depCycleDirectBackEdge() {
	// Direct (one-hop) back-edge: a blocks b already; adding b -> a closes
	// a 2-cycle. Exercises the indexed point-lookup fast path before the CTE.
	s.seedIssueRow("bd-cy-dir-a")
	s.seedIssueRow("bd-cy-dir-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-dir-a", "bd-cy-dir-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	cycle, err := r.HasCycle(s.Ctx(), "bd-cy-dir-b", "bd-cy-dir-a")
	s.Require().NoError(err)
	s.True(cycle, "direct back-edge should detect cycle via fast path")
}

func (s *testSuite) depCycleBackEdge() {
	s.seedIssueRow("bd-cy-back-a")
	s.seedIssueRow("bd-cy-back-b")
	s.seedIssueRow("bd-cy-back-c")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-back-a", "bd-cy-back-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-back-b", "bd-cy-back-c", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	// Adding c -> a would close the cycle a -> b -> c -> a.
	cycle, err := r.HasCycle(s.Ctx(), "bd-cy-back-c", "bd-cy-back-a")
	s.Require().NoError(err)
	s.True(cycle, "expected back-edge to close a cycle")
}

func (s *testSuite) depCycleIgnoresNonBlocking() {
	s.seedIssueRow("bd-cy-rel-a")
	s.seedIssueRow("bd-cy-rel-b")
	r := s.depRepo()
	// related-only edge — not a blocking type, must not contribute to cycle search.
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cy-rel-a", "bd-cy-rel-b", types.DepRelated), "tester", domain.DepInsertOpts{}))

	cycle, err := r.HasCycle(s.Ctx(), "bd-cy-rel-b", "bd-cy-rel-a")
	s.Require().NoError(err)
	s.False(cycle)
}

func (s *testSuite) depListEmpty() {
	out, err := s.depRepo().ListByIssueIDs(s.Ctx(), nil, domain.DepListOpts{})
	s.Require().NoError(err)
	s.NotNil(out.Outgoing)
	s.NotNil(out.Incoming)
	s.Empty(out.Outgoing)
	s.Empty(out.Incoming)
}

func (s *testSuite) depListOutgoing() {
	s.seedIssueRow("bd-lst-out-1")
	s.seedIssueRow("bd-lst-out-2")
	s.seedIssueRow("bd-lst-out-3")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-out-1", "bd-lst-out-2", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-out-1", "bd-lst-out-3", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-lst-out-1"}, domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-lst-out-1"], 2)
	s.Empty(out.Incoming, "outgoing-only request should leave Incoming empty")
}

func (s *testSuite) depListIncoming() {
	s.seedIssueRow("bd-lst-in-1")
	s.seedIssueRow("bd-lst-in-2")
	s.seedIssueRow("bd-lst-in-3")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-in-2", "bd-lst-in-1", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-in-3", "bd-lst-in-1", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-lst-in-1"}, domain.DepListOpts{Direction: domain.DepDirectionIn})
	s.Require().NoError(err)
	s.Require().Len(out.Incoming["bd-lst-in-1"], 2)
	s.Empty(out.Outgoing)
}

func (s *testSuite) depListBoth() {
	s.seedIssueRow("bd-lst-bo-mid")
	s.seedIssueRow("bd-lst-bo-up")
	s.seedIssueRow("bd-lst-bo-down")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-bo-up", "bd-lst-bo-mid", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-bo-mid", "bd-lst-bo-down", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-lst-bo-mid"}, domain.DepListOpts{Direction: domain.DepDirectionBoth})
	s.Require().NoError(err)
	s.Len(out.Outgoing["bd-lst-bo-mid"], 1, "mid -> down should be outgoing")
	s.Equal("bd-lst-bo-down", out.Outgoing["bd-lst-bo-mid"][0].DependsOnID)
	s.Len(out.Incoming["bd-lst-bo-mid"], 1, "up -> mid should be incoming")
	s.Equal("bd-lst-bo-up", out.Incoming["bd-lst-bo-mid"][0].IssueID)
}

func (s *testSuite) depListTypeFilter() {
	s.seedIssueRow("bd-lst-typ-a")
	s.seedIssueRow("bd-lst-typ-b")
	s.seedIssueRow("bd-lst-typ-c")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-typ-a", "bd-lst-typ-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-lst-typ-a", "bd-lst-typ-c", types.DepRelated), "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-lst-typ-a"}, domain.DepListOpts{
		Direction: domain.DepDirectionOut,
		Types:     []types.DependencyType{types.DepBlocks},
	})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-lst-typ-a"], 1)
	s.Equal("bd-lst-typ-b", out.Outgoing["bd-lst-typ-a"][0].DependsOnID)
}

func (s *testSuite) depCountsEmpty() {
	out, err := s.depRepo().CountsByIssueIDs(s.Ctx(), nil, domain.DepCountsOpts{})
	s.Require().NoError(err)
	s.NotNil(out)
	s.Empty(out)
}

func (s *testSuite) depCountsBlocksOnly() {
	s.seedIssueRow("bd-cnt-mid")
	s.seedIssueRow("bd-cnt-out-1")
	s.seedIssueRow("bd-cnt-out-2")
	s.seedIssueRow("bd-cnt-in-1")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cnt-mid", "bd-cnt-out-1", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cnt-mid", "bd-cnt-out-2", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	// Non-blocking outgoing — must not be counted.
	s.seedIssueRow("bd-cnt-rel-tgt")
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cnt-mid", "bd-cnt-rel-tgt", types.DepRelated), "tester", domain.DepInsertOpts{}))
	// Incoming blocking edge.
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-cnt-in-1", "bd-cnt-mid", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.CountsByIssueIDs(s.Ctx(), []string{"bd-cnt-mid"}, domain.DepCountsOpts{})
	s.Require().NoError(err)
	s.Require().NotNil(out["bd-cnt-mid"])
	s.Equal(2, out["bd-cnt-mid"].DependencyCount, "outgoing blocks only")
	s.Equal(1, out["bd-cnt-mid"].DependentCount, "incoming blocks only")
}

func (s *testSuite) depCountsZeroPresent() {
	s.seedIssueRow("bd-cnt-zero")
	out, err := s.depRepo().CountsByIssueIDs(s.Ctx(), []string{"bd-cnt-zero"}, domain.DepCountsOpts{})
	s.Require().NoError(err)
	s.Require().NotNil(out["bd-cnt-zero"], "issues with zero deps should still appear with zero counts")
	s.Equal(0, out["bd-cnt-zero"].DependencyCount)
	s.Equal(0, out["bd-cnt-zero"].DependentCount)
}

func (s *testSuite) depWispInsertRouting() {
	// Source is a wisp; target is a permanent issue. wisp_dependencies has
	// fk_wisp_dep_issue (issue_id -> wisps) and fk_wisp_dep_issue_target
	// (depends_on_issue_id -> issues).
	s.seedWispRow("bd-dep-wisp-src")
	s.seedIssueRow("bd-dep-wisp-tgt")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wisp-src", "bd-dep-wisp-tgt", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true},
	))

	var wispCount, permCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_dependencies WHERE issue_id = ?", "bd-dep-wisp-src").Scan(&wispCount))
	s.Equal(1, wispCount)
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE issue_id = ?", "bd-dep-wisp-src").Scan(&permCount))
	s.Equal(0, permCount, "wisp-routed insert must not write to dependencies")
}

func (s *testSuite) depWispListRouting() {
	s.seedWispRow("bd-dep-wlist-src")
	s.seedIssueRow("bd-dep-wlist-a")
	s.seedIssueRow("bd-dep-wlist-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wlist-src", "bd-dep-wlist-a", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wlist-src", "bd-dep-wlist-b", types.DepRelated), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dep-wlist-src"},
		domain.DepListOpts{Direction: domain.DepDirectionOut, UseWispsTable: true})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-dep-wlist-src"], 2)

	// Same query against the permanent table returns nothing.
	empty, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dep-wlist-src"},
		domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Empty(empty.Outgoing)
}

func (s *testSuite) depWispCountsRouting() {
	s.seedWispRow("bd-dep-wcnt-src")
	s.seedIssueRow("bd-dep-wcnt-a")
	s.seedIssueRow("bd-dep-wcnt-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wcnt-src", "bd-dep-wcnt-a", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wcnt-src", "bd-dep-wcnt-b", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.CountsByIssueIDs(s.Ctx(), []string{"bd-dep-wcnt-src"}, domain.DepCountsOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Require().NotNil(out["bd-dep-wcnt-src"])
	s.Equal(2, out["bd-dep-wcnt-src"].DependencyCount)

	// Permanent-table count is zero for the same source.
	permOut, err := r.CountsByIssueIDs(s.Ctx(), []string{"bd-dep-wcnt-src"}, domain.DepCountsOpts{})
	s.Require().NoError(err)
	s.Require().NotNil(permOut["bd-dep-wcnt-src"])
	s.Equal(0, permOut["bd-dep-wcnt-src"].DependencyCount)
}

func (s *testSuite) depWispHasCycleCrossTable() {
	// Cross-table closure: issue a (perm) blocks wisp s (wisp_dependencies),
	// then wisp s blocks issue b (wisp_dependencies). Adding b -> a (perm)
	// would close a cycle through both tables.
	s.seedIssueRow("bd-dep-cx-a")
	s.seedIssueRow("bd-dep-cx-b")
	s.seedWispRow("bd-dep-cx-s")

	r := s.depRepo()
	// a -> s: source a is permanent, target is a wisp. Stored in dependencies
	// with depends_on_wisp_id set. We need to insert via raw SQL because our
	// Insert path writes to depends_on_issue_id only.
	_, err := s.Runner().ExecContext(s.Ctx(), `
		INSERT INTO dependencies (issue_id, depends_on_wisp_id, type, created_at, created_by, metadata)
		VALUES (?, ?, 'blocks', NOW(), 'tester', '{}')
	`, "bd-dep-cx-a", "bd-dep-cx-s")
	s.Require().NoError(err)
	// s -> b: source s is wisp, target is permanent. Stored in wisp_dependencies.
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-cx-s", "bd-dep-cx-b", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))

	// HasCycle traverses both tables, but only follows depends_on_issue_id
	// edges. a -> s (via depends_on_wisp_id) is NOT followed, so the closure
	// from b stops at b. This is documented behavior — wisp-target closure is
	// intentionally excluded; revisit if needed.
	cycle, err := r.HasCycle(s.Ctx(), "bd-dep-cx-b", "bd-dep-cx-a")
	s.Require().NoError(err)
	s.False(cycle, "wisp-target edges are intentionally not followed in cycle detection")
}

func (s *testSuite) depBlockingInfoEmpty() {
	info, err := s.depRepo().GetBlockingInfo(s.Ctx(), nil, domain.DepListOpts{})
	s.Require().NoError(err)
	s.NotNil(info.BlockedBy)
	s.NotNil(info.Blocks)
	s.NotNil(info.Parent)
	s.Empty(info.BlockedBy)
	s.Empty(info.Blocks)
	s.Empty(info.Parent)
}

func (s *testSuite) depBlockingInfoBlockedByAndBlocks() {
	s.seedIssueRow("bd-bi-mid")
	s.seedIssueRow("bd-bi-up")
	s.seedIssueRow("bd-bi-down")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-bi-mid", "bd-bi-up", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-bi-down", "bd-bi-mid", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	info, err := r.GetBlockingInfo(s.Ctx(), []string{"bd-bi-mid"}, domain.DepListOpts{})
	s.Require().NoError(err)
	s.Equal([]string{"bd-bi-up"}, info.BlockedBy["bd-bi-mid"])
	s.Equal([]string{"bd-bi-down"}, info.Blocks["bd-bi-mid"])
	s.Empty(info.Parent)
}

func (s *testSuite) depBlockingInfoParent() {
	s.seedIssueRow("bd-bi-child")
	s.seedIssueRow("bd-bi-parent")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-bi-child", "bd-bi-parent", types.DepParentChild), "tester", domain.DepInsertOpts{}))

	info, err := r.GetBlockingInfo(s.Ctx(), []string{"bd-bi-child"}, domain.DepListOpts{})
	s.Require().NoError(err)
	s.Equal("bd-bi-parent", info.Parent["bd-bi-child"])
	s.Empty(info.BlockedBy, "parent-child must not appear in BlockedBy")
}

func (s *testSuite) depBlockingInfoSkipsClosed() {
	s.seedIssueRow("bd-bi-cls-mid")
	s.seedIssueRow("bd-bi-cls-blocker")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-bi-cls-mid", "bd-bi-cls-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	_, err := s.Runner().ExecContext(s.Ctx(),
		"UPDATE issues SET status = ? WHERE id = ?", string(types.StatusClosed), "bd-bi-cls-blocker")
	s.Require().NoError(err)

	info, err := r.GetBlockingInfo(s.Ctx(), []string{"bd-bi-cls-mid"}, domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(info.BlockedBy["bd-bi-cls-mid"], "closed blockers should be filtered out")
}

func (s *testSuite) depBlockingInfoAcrossUnions() {
	s.seedIssueRow("bd-bi-x-target")
	s.seedIssueRow("bd-bi-x-permblocker")
	s.seedWispRow("bd-bi-x-wispblocker")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-bi-x-target", "bd-bi-x-permblocker", types.DepBlocks), "tester",
		domain.DepInsertOpts{}))
	_, err := s.Runner().ExecContext(s.Ctx(), `
		INSERT INTO wisp_dependencies (issue_id, depends_on_wisp_id, type, created_at, created_by, metadata)
		VALUES (?, ?, 'blocks', NOW(), 'tester', '{}')
	`, "bd-bi-x-target", "bd-bi-x-wispblocker")
	s.Require().NoError(err)

	info, err := r.GetBlockingInfoAcrossIssuesAndWisps(s.Ctx(), []string{"bd-bi-x-target"})
	s.Require().NoError(err)
	s.ElementsMatch([]string{"bd-bi-x-permblocker", "bd-bi-x-wispblocker"}, info.BlockedBy["bd-bi-x-target"])
}

func (s *testSuite) depWispDirectBackEdge() {
	s.seedWispRow("bd-dep-wd-s")
	s.seedIssueRow("bd-dep-wd-t")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-dep-wd-s", "bd-dep-wd-t", types.DepBlocks), "tester",
		domain.DepInsertOpts{UseWispsTable: true}))

	cycle, err := r.HasCycle(s.Ctx(), "bd-dep-wd-t", "bd-dep-wd-s")
	s.Require().NoError(err)
	s.True(cycle, "fast path must probe wisp_dependencies too")
}

func (s *testSuite) depRemoveExisting() {
	s.seedIssueRow("bd-dr-ex-a")
	s.seedIssueRow("bd-dr-ex-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dr-ex-a", "bd-dr-ex-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-ex-a", "bd-dr-ex-b", "tester", domain.DepInsertOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE issue_id = ? AND depends_on_issue_id = ?",
		"bd-dr-ex-a", "bd-dr-ex-b",
	).Scan(&count))
	s.Equal(0, count)
}

func (s *testSuite) depRemoveMissingNoOp() {
	s.seedIssueRow("bd-dr-mn-a")
	s.seedIssueRow("bd-dr-mn-b")
	r := s.depRepo()
	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-mn-a", "bd-dr-mn-b", "tester", domain.DepInsertOpts{}),
		"removing a non-existent dep edge must be a no-op success")
}

func (s *testSuite) depRemoveEmptyIDs() {
	r := s.depRepo()
	s.Require().Error(r.Remove(s.Ctx(), "", "bd-x", "tester", domain.DepInsertOpts{}))
	s.Require().Error(r.Remove(s.Ctx(), "bd-x", "", "tester", domain.DepInsertOpts{}))
}

func (s *testSuite) depRemoveWispRouting() {
	s.seedWispRow("bd-dr-wp-a")
	s.seedWispRow("bd-dr-wp-b")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dr-wp-a", "bd-dr-wp-b", types.DepBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-wp-a", "bd-dr-wp-b", "tester", domain.DepInsertOpts{UseWispsTable: true}))

	var wispCount int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM wisp_dependencies WHERE issue_id = ? AND depends_on_wisp_id = ?",
		"bd-dr-wp-a", "bd-dr-wp-b",
	).Scan(&wispCount))
	s.Equal(0, wispCount, "wisp dep should be removed from wisp_dependencies")
}

func (s *testSuite) depRemoveExternalTarget() {
	s.seedIssueRow("bd-dr-ext-src")
	r := s.depRepo()
	dep := newDep("bd-dr-ext-src", "external:gh-123", types.DepBlocks)
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{}))

	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-ext-src", "external:gh-123", "tester", domain.DepInsertOpts{}))

	var count int
	s.Require().NoError(s.Runner().QueryRowContext(s.Ctx(),
		"SELECT COUNT(*) FROM dependencies WHERE issue_id = ? AND depends_on_external = ?",
		"bd-dr-ext-src", "external:gh-123",
	).Scan(&count))
	s.Equal(0, count, "depTargetExpr COALESCE must match external targets too")
}

func (s *testSuite) depRemoveUnblocksDependent() {
	s.seedIssueWithStatus("bd-dr-ub-blocker", types.StatusOpen)
	s.seedIssueWithStatus("bd-dr-ub-blocked", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dr-ub-blocked", "bd-dr-ub-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-dr-ub-blocked", false, 1)

	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-ub-blocked", "bd-dr-ub-blocker", "tester", domain.DepInsertOpts{}))
	s.Equal(0, s.isBlockedFlag("bd-dr-ub-blocked", false),
		"Remove should drive AffectedByDepChange + Recompute so the source's is_blocked drops")
}

func (s *testSuite) depRemoveLeavesOtherEdges() {
	s.seedIssueRow("bd-dr-le-a")
	s.seedIssueRow("bd-dr-le-b")
	s.seedIssueRow("bd-dr-le-c")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dr-le-a", "bd-dr-le-b", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dr-le-a", "bd-dr-le-c", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	s.Require().NoError(r.Remove(s.Ctx(), "bd-dr-le-a", "bd-dr-le-b", "tester", domain.DepInsertOpts{}))

	out, err := r.ListByIssueIDs(s.Ctx(), []string{"bd-dr-le-a"}, domain.DepListOpts{Direction: domain.DepDirectionOut})
	s.Require().NoError(err)
	s.Require().Len(out.Outgoing["bd-dr-le-a"], 1, "the other outgoing edge must survive")
	s.Equal("bd-dr-le-c", out.Outgoing["bd-dr-le-a"][0].DependsOnID)
}

func (s *testSuite) depRecordsEmpty() {
	s.seedIssueRow("bd-recs-empty")
	out, err := s.depRepo().ListRecordsForIssue(s.Ctx(), "bd-recs-empty", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out)
}

func (s *testSuite) depRecordsOutgoingFields() {
	s.seedIssueRow("bd-recs-src")
	s.seedIssueRow("bd-recs-tgt")
	r := s.depRepo()
	dep := &types.Dependency{
		IssueID:     "bd-recs-src",
		DependsOnID: "bd-recs-tgt",
		Type:        types.DepBlocks,
		Metadata:    `{"reason":"audit"}`,
		ThreadID:    "thread-42",
	}
	s.Require().NoError(r.Insert(s.Ctx(), dep, "alice", domain.DepInsertOpts{}))

	out, err := r.ListRecordsForIssue(s.Ctx(), "bd-recs-src", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	got := out[0]
	s.Equal("bd-recs-src", got.IssueID)
	s.Equal("bd-recs-tgt", got.DependsOnID)
	s.Equal(types.DepBlocks, got.Type)
	s.Equal(`{"reason":"audit"}`, got.Metadata)
	s.Equal("thread-42", got.ThreadID)
	s.Equal("alice", got.CreatedBy)
	s.False(got.CreatedAt.IsZero(), "created_at should be populated")
}

func (s *testSuite) depRecordsTypesFilter() {
	s.seedIssueRow("bd-recs-tf-src")
	s.seedIssueRow("bd-recs-tf-parent")
	s.seedIssueRow("bd-recs-tf-blocker")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-recs-tf-src", "bd-recs-tf-parent", types.DepParentChild), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-recs-tf-src", "bd-recs-tf-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListRecordsForIssue(s.Ctx(), "bd-recs-tf-src", domain.DepListOpts{
		Types: []types.DependencyType{types.DepParentChild},
	})
	s.Require().NoError(err)
	s.Require().Len(out, 1, "only parent-child should pass the filter")
	s.Equal(types.DepParentChild, out[0].Type)
	s.Equal("bd-recs-tf-parent", out[0].DependsOnID)
}

func (s *testSuite) depRecordsWispRouting() {
	s.seedWispRow("bd-recs-wp-src")
	s.seedWispRow("bd-recs-wp-tgt")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-recs-wp-src", "bd-recs-wp-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	wispOut, err := r.ListRecordsForIssue(s.Ctx(), "bd-recs-wp-src", domain.DepListOpts{UseWispsTable: true})
	s.Require().NoError(err)
	s.Require().Len(wispOut, 1)
	s.Equal("bd-recs-wp-tgt", wispOut[0].DependsOnID)

	permOut, err := r.ListRecordsForIssue(s.Ctx(), "bd-recs-wp-src", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(permOut, "non-wisp lookup must not see wisp deps")
}

func (s *testSuite) depRecordsEmptyID() {
	_, err := s.depRepo().ListRecordsForIssue(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "issueID must not be empty")
}

func depIssueIDSet(issues []*types.Issue) map[string]bool {
	out := make(map[string]bool, len(issues))
	for _, iss := range issues {
		out[iss.ID] = true
	}
	return out
}

func (s *testSuite) depDependedOnEmpty() {
	s.seedIssueRow("bd-do-empty")
	out, err := s.depRepo().ListIssuesDependedOn(s.Ctx(), "bd-do-empty", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out)
}

func (s *testSuite) depDependedOnIssueTargets() {
	s.seedIssueRow("bd-do-it-src")
	s.seedIssueRow("bd-do-it-blocker")
	s.seedIssueRow("bd-do-it-parent")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-do-it-src", "bd-do-it-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-do-it-src", "bd-do-it-parent", types.DepParentChild), "tester", domain.DepInsertOpts{}))

	out, err := r.ListIssuesDependedOn(s.Ctx(), "bd-do-it-src", domain.DepListOpts{})
	s.Require().NoError(err)
	ids := depIssueIDSet(out)
	s.True(ids["bd-do-it-blocker"])
	s.True(ids["bd-do-it-parent"])
	s.Len(ids, 2)
	for _, iss := range out {
		s.NotEmpty(iss.Title, "full row should be hydrated, not just ID")
	}
}

func (s *testSuite) depDependedOnWispTargets() {
	s.seedIssueRow("bd-do-wt-src")
	s.seedWispRow("bd-do-wt-wisp-target")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-do-wt-src", "bd-do-wt-wisp-target", types.DepBlocks),
		"tester", domain.DepInsertOpts{}))

	out, err := r.ListIssuesDependedOn(s.Ctx(), "bd-do-wt-src", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("bd-do-wt-wisp-target", out[0].ID, "wisp targets are returned alongside issue targets")
}

func (s *testSuite) depDependedOnSkipsExternal() {
	s.seedIssueRow("bd-do-ext-src")
	s.seedIssueRow("bd-do-ext-issue")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-do-ext-src", "external:gh-99", types.DepBlocks),
		"tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-do-ext-src", "bd-do-ext-issue", types.DepBlocks),
		"tester", domain.DepInsertOpts{}))

	out, err := r.ListIssuesDependedOn(s.Ctx(), "bd-do-ext-src", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1, "external targets are not hydratable issues; should be skipped")
	s.Equal("bd-do-ext-issue", out[0].ID)
}

func (s *testSuite) depDependedOnWispSource() {
	s.seedWispRow("bd-do-ws-src")
	s.seedIssueRow("bd-do-ws-tgt")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(),
		newDep("bd-do-ws-src", "bd-do-ws-tgt", types.DepBlocks),
		"tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListIssuesDependedOn(s.Ctx(), "bd-do-ws-src", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1, "outgoing deps from a wisp live in wisp_dependencies; method must query both tables")
	s.Equal("bd-do-ws-tgt", out[0].ID)
}

func (s *testSuite) depDependedOnEmptyID() {
	_, err := s.depRepo().ListIssuesDependedOn(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "issueID must not be empty")
}

func (s *testSuite) depDependentsEmpty() {
	s.seedIssueRow("bd-dt-empty")
	out, err := s.depRepo().ListIssueDependents(s.Ctx(), "bd-dt-empty", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out)
}

func (s *testSuite) depDependentsIssues() {
	s.seedIssueRow("bd-dt-tgt")
	s.seedIssueRow("bd-dt-dep")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-dep", "bd-dt-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListIssueDependents(s.Ctx(), "bd-dt-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("bd-dt-dep", out[0].ID)
	s.NotEmpty(out[0].Title, "full row should be hydrated")
}

func (s *testSuite) depDependentsMultiple() {
	s.seedIssueRow("bd-dt-mt-tgt")
	s.seedIssueRow("bd-dt-mt-dep1")
	s.seedIssueRow("bd-dt-mt-dep2")
	s.seedIssueRow("bd-dt-mt-dep3")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-mt-dep1", "bd-dt-mt-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-mt-dep2", "bd-dt-mt-tgt", types.DepParentChild), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-mt-dep3", "bd-dt-mt-tgt", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListIssueDependents(s.Ctx(), "bd-dt-mt-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	ids := depIssueIDSet(out)
	s.True(ids["bd-dt-mt-dep1"])
	s.True(ids["bd-dt-mt-dep2"])
	s.True(ids["bd-dt-mt-dep3"])
	s.Len(ids, 3)
}

func (s *testSuite) depDependentsWispsIncluded() {
	s.seedIssueRow("bd-dt-wi-tgt")
	s.seedIssueRow("bd-dt-wi-issue-dep")
	s.seedWispRow("bd-dt-wi-wisp-dep")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-wi-issue-dep", "bd-dt-wi-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-wi-wisp-dep", "bd-dt-wi-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListIssueDependents(s.Ctx(), "bd-dt-wi-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	ids := depIssueIDSet(out)
	s.True(ids["bd-dt-wi-issue-dep"])
	s.True(ids["bd-dt-wi-wisp-dep"], "wisp-side dep rows must also surface as dependents")
	s.Len(ids, 2)
}

func (s *testSuite) depDependentsWispTarget() {
	s.seedWispRow("bd-dt-wt-tgt")
	s.seedIssueRow("bd-dt-wt-issue-dep")
	s.seedWispRow("bd-dt-wt-wisp-dep")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-wt-issue-dep", "bd-dt-wt-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dt-wt-wisp-dep", "bd-dt-wt-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListIssueDependents(s.Ctx(), "bd-dt-wt-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	ids := depIssueIDSet(out)
	s.True(ids["bd-dt-wt-issue-dep"])
	s.True(ids["bd-dt-wt-wisp-dep"])
	s.Len(ids, 2, "dependents on a wisp target must come from both dep tables")
}

func (s *testSuite) depDependentsEmptyID() {
	_, err := s.depRepo().ListIssueDependents(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "issueID must not be empty")
}

func (s *testSuite) depDepMetaEmpty() {
	s.seedIssueRow("bd-dm-empty")
	out, err := s.depRepo().ListDependentsWithMetadata(s.Ctx(), "bd-dm-empty", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out)
}

func (s *testSuite) depDepMetaAttachesType() {
	s.seedIssueRow("bd-dm-at-tgt")
	s.seedIssueRow("bd-dm-at-dep")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-at-dep", "bd-dm-at-tgt", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListDependentsWithMetadata(s.Ctx(), "bd-dm-at-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("bd-dm-at-dep", out[0].ID)
	s.Equal(types.DepConditionalBlocks, out[0].DependencyType, "dep type must be attached to each dependent")
}

func (s *testSuite) depDepMetaMultipleTypes() {
	s.seedIssueRow("bd-dm-mt-tgt")
	s.seedIssueRow("bd-dm-mt-blocks")
	s.seedIssueRow("bd-dm-mt-child")
	s.seedIssueRow("bd-dm-mt-waits")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-mt-blocks", "bd-dm-mt-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-mt-child", "bd-dm-mt-tgt", types.DepParentChild), "tester", domain.DepInsertOpts{}))
	waitsDep := newDep("bd-dm-mt-waits", "bd-dm-mt-tgt", types.DepWaitsFor)
	waitsDep.Metadata = `{"gate":"all-children"}`
	s.Require().NoError(r.Insert(s.Ctx(), waitsDep, "tester", domain.DepInsertOpts{}))

	out, err := r.ListDependentsWithMetadata(s.Ctx(), "bd-dm-mt-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 3)

	typeByID := make(map[string]types.DependencyType, len(out))
	for _, d := range out {
		typeByID[d.ID] = d.DependencyType
	}
	s.Equal(types.DepBlocks, typeByID["bd-dm-mt-blocks"])
	s.Equal(types.DepParentChild, typeByID["bd-dm-mt-child"])
	s.Equal(types.DepWaitsFor, typeByID["bd-dm-mt-waits"])
}

func (s *testSuite) depDepMetaWispsIncluded() {
	s.seedIssueRow("bd-dm-wi-tgt")
	s.seedIssueRow("bd-dm-wi-issue-dep")
	s.seedWispRow("bd-dm-wi-wisp-dep")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-wi-issue-dep", "bd-dm-wi-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-wi-wisp-dep", "bd-dm-wi-tgt", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListDependentsWithMetadata(s.Ctx(), "bd-dm-wi-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 2)

	typeByID := make(map[string]types.DependencyType, len(out))
	for _, d := range out {
		typeByID[d.ID] = d.DependencyType
	}
	s.Equal(types.DepBlocks, typeByID["bd-dm-wi-issue-dep"])
	s.Equal(types.DepConditionalBlocks, typeByID["bd-dm-wi-wisp-dep"],
		"wisp dependents must surface with their dep type from wisp_dependencies")
}

func (s *testSuite) depDepMetaHydratesFields() {
	_, err := s.Runner().ExecContext(s.Ctx(), `
		INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, priority, issue_type, assignee)
		VALUES (?, ?, '', '', '', '', 1, 'feature', 'alice')
	`, "bd-dm-hf-dep", "rich dependent")
	s.Require().NoError(err)
	s.seedIssueRow("bd-dm-hf-tgt")
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-dm-hf-dep", "bd-dm-hf-tgt", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListDependentsWithMetadata(s.Ctx(), "bd-dm-hf-tgt", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("rich dependent", out[0].Title)
	s.Equal("alice", out[0].Assignee)
	s.Equal(1, out[0].Priority)
	s.Equal(types.TypeFeature, out[0].IssueType)
}

func (s *testSuite) depDepMetaEmptyID() {
	_, err := s.depRepo().ListDependentsWithMetadata(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "issueID must not be empty")
}

func (s *testSuite) depIsBlockedNoDeps() {
	s.seedIssueWithStatus("bd-ib-none", types.StatusOpen)
	blocked, blockers, err := s.depRepo().IsBlocked(s.Ctx(), "bd-ib-none", domain.DepListOpts{})
	s.Require().NoError(err)
	s.False(blocked)
	s.Empty(blockers)
}

func (s *testSuite) depIsBlockedFastPath() {
	s.seedIssueWithStatus("bd-ib-fast-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-fast-blocker", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-fast-target", "bd-ib-fast-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-fast-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.False(blocked, "is_blocked column is still 0 until Recompute runs; IsBlocked returns false on the fast path")
	s.Empty(blockers)
}

func (s *testSuite) depIsBlockedByOpen() {
	s.seedIssueWithStatus("bd-ib-op-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-op-blocker", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-op-target", "bd-ib-op-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-op-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-op-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked)
	s.Equal([]string{"bd-ib-op-blocker"}, blockers)
}

func (s *testSuite) depIsBlockedClosedFiltered() {
	s.seedIssueWithStatus("bd-ib-cl-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-cl-closed", types.StatusClosed)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-cl-target", "bd-ib-cl-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-cl-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-cl-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked, "flag is set so impl reports blocked, even if the only edge is to a closed issue")
	s.Empty(blockers, "closed blockers are filtered from the human-readable list")
}

func (s *testSuite) depIsBlockedPinnedFiltered() {
	s.seedIssueWithStatus("bd-ib-pin-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-pin-blocker", types.StatusPinned)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-pin-target", "bd-ib-pin-blocker", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-pin-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-pin-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked)
	s.Empty(blockers, "pinned blockers are filtered from the human-readable list")
}

func (s *testSuite) depIsBlockedConditionalBlocks() {
	s.seedIssueWithStatus("bd-ib-cb-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-cb-blocker", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-cb-target", "bd-ib-cb-blocker", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-cb-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-cb-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked)
	s.Equal([]string{"bd-ib-cb-blocker (conditional-blocks)"}, blockers)
}

func (s *testSuite) depIsBlockedWaitsFor() {
	s.seedIssueWithStatus("bd-ib-wf-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-wf-spawner", types.StatusOpen)
	r := s.depRepo()
	wf := newDep("bd-ib-wf-target", "bd-ib-wf-spawner", types.DepWaitsFor)
	wf.Metadata = `{"gate":"all-children"}`
	s.Require().NoError(r.Insert(s.Ctx(), wf, "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-wf-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-wf-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked)
	s.Equal([]string{"bd-ib-wf-spawner (waits-for)"}, blockers)
}

func (s *testSuite) depIsBlockedMultiple() {
	s.seedIssueWithStatus("bd-ib-mt-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-mt-b1", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-mt-b2", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-mt-b3-closed", types.StatusClosed)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-mt-target", "bd-ib-mt-b1", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-mt-target", "bd-ib-mt-b2", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ib-mt-target", "bd-ib-mt-b3-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.setIsBlocked("bd-ib-mt-target", false, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-mt-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked)
	s.ElementsMatch([]string{"bd-ib-mt-b1", "bd-ib-mt-b2 (conditional-blocks)"}, blockers,
		"closed blocker excluded; remaining two listed with type suffix where applicable")
}

func (s *testSuite) depIsBlockedWispSource() {
	s.seedWispWithStatus("bd-ib-ws-target", types.StatusOpen)
	s.seedIssueWithStatus("bd-ib-ws-blocker", types.StatusOpen)
	r := s.depRepo()
	dep := &types.Dependency{
		IssueID:     "bd-ib-ws-target",
		DependsOnID: "bd-ib-ws-blocker",
		Type:        types.DepBlocks,
	}
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))
	s.setIsBlocked("bd-ib-ws-target", true, 1)

	blocked, blockers, err := r.IsBlocked(s.Ctx(), "bd-ib-ws-target", domain.DepListOpts{})
	s.Require().NoError(err)
	s.True(blocked, "should auto-detect that the source is a wisp")
	s.Equal([]string{"bd-ib-ws-blocker"}, blockers)
}

func (s *testSuite) depIsBlockedMissingIssue() {
	blocked, blockers, err := s.depRepo().IsBlocked(s.Ctx(), "bd-ib-nope", domain.DepListOpts{})
	s.Require().NoError(err)
	s.False(blocked)
	s.Empty(blockers)
}

func (s *testSuite) depIsBlockedEmptyID() {
	_, _, err := s.depRepo().IsBlocked(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "issueID must not be empty")
}

func (s *testSuite) depUnblockedEmpty() {
	s.seedIssueWithStatus("bd-ub-none", types.StatusClosed)
	out, err := s.depRepo().ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-none", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out)
}

func (s *testSuite) depUnblockedSoleBlocker() {
	s.seedIssueWithStatus("bd-ub-sb-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-sb-dep", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-sb-dep", "bd-ub-sb-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-sb-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("bd-ub-sb-dep", out[0].ID)
}

func (s *testSuite) depUnblockedStillBlocked() {
	s.seedIssueWithStatus("bd-ub-st-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-st-other-open", types.StatusOpen)
	s.seedIssueWithStatus("bd-ub-st-dep", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-st-dep", "bd-ub-st-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-st-dep", "bd-ub-st-other-open", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-st-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out, "dependent still blocked by another open issue must not appear in the unblocked set")
}

func (s *testSuite) depUnblockedClosedCandidate() {
	s.seedIssueWithStatus("bd-ub-cl-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-cl-dep", types.StatusClosed)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-cl-dep", "bd-ub-cl-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-cl-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out, "already-closed candidates aren't 'unblocked'")
}

func (s *testSuite) depUnblockedPinnedCandidate() {
	s.seedIssueWithStatus("bd-ub-pin-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-pin-dep", types.StatusPinned)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-pin-dep", "bd-ub-pin-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-pin-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out, "pinned candidates also excluded")
}

func (s *testSuite) depUnblockedOnlyBlocksType() {
	s.seedIssueWithStatus("bd-ub-ot-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-ot-condep", types.StatusOpen)
	s.seedIssueWithStatus("bd-ub-ot-pcdep", types.StatusOpen)
	s.seedIssueWithStatus("bd-ub-ot-waiter", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-ot-condep", "bd-ub-ot-closed", types.DepConditionalBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-ot-pcdep", "bd-ub-ot-closed", types.DepParentChild), "tester", domain.DepInsertOpts{}))
	wf := newDep("bd-ub-ot-waiter", "bd-ub-ot-closed", types.DepWaitsFor)
	wf.Metadata = `{"gate":"all-children"}`
	s.Require().NoError(r.Insert(s.Ctx(), wf, "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-ot-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Empty(out, "only the 'blocks' edge type is considered for newly-unblocked discovery")
}

func (s *testSuite) depUnblockedWispDependent() {
	s.seedIssueWithStatus("bd-ub-wp-closed", types.StatusClosed)
	s.seedWispWithStatus("bd-ub-wp-dep", types.StatusOpen)
	r := s.depRepo()
	dep := newDep("bd-ub-wp-dep", "bd-ub-wp-closed", types.DepBlocks)
	s.Require().NoError(r.Insert(s.Ctx(), dep, "tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-wp-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("bd-ub-wp-dep", out[0].ID, "wisp dependents are hydrated from wisps table")
}

func (s *testSuite) depUnblockedHydrates() {
	s.seedIssueWithStatus("bd-ub-hy-closed", types.StatusClosed)
	_, err := s.Runner().ExecContext(s.Ctx(), `
		INSERT INTO issues (id, title, description, design, acceptance_criteria, notes, status, priority, issue_type, assignee)
		VALUES (?, ?, '', '', '', '', 'open', 1, 'bug', 'alice')
	`, "bd-ub-hy-dep", "rich dependent")
	s.Require().NoError(err)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-hy-dep", "bd-ub-hy-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-hy-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1)
	s.Equal("rich dependent", out[0].Title)
	s.Equal("alice", out[0].Assignee)
	s.Equal(1, out[0].Priority)
	s.Equal(types.TypeBug, out[0].IssueType)
}

func (s *testSuite) depUnblockedMultiple() {
	s.seedIssueWithStatus("bd-ub-mt-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-mt-d1", types.StatusOpen)
	s.seedIssueWithStatus("bd-ub-mt-d2", types.StatusOpen)
	s.seedWispWithStatus("bd-ub-mt-wd", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-mt-d1", "bd-ub-mt-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-mt-d2", "bd-ub-mt-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-mt-wd", "bd-ub-mt-closed", types.DepBlocks), "tester", domain.DepInsertOpts{UseWispsTable: true}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-mt-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 3)
	got := depIssueIDSet(out)
	s.True(got["bd-ub-mt-d1"])
	s.True(got["bd-ub-mt-d2"])
	s.True(got["bd-ub-mt-wd"])
}

func (s *testSuite) depUnblockedClosedNotCountedAsRemaining() {
	s.seedIssueWithStatus("bd-ub-cb-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-cb-also-closed", types.StatusClosed)
	s.seedIssueWithStatus("bd-ub-cb-dep", types.StatusOpen)
	r := s.depRepo()
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-cb-dep", "bd-ub-cb-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))
	s.Require().NoError(r.Insert(s.Ctx(), newDep("bd-ub-cb-dep", "bd-ub-cb-also-closed", types.DepBlocks), "tester", domain.DepInsertOpts{}))

	out, err := r.ListNewlyUnblockedByClose(s.Ctx(), "bd-ub-cb-closed", domain.DepListOpts{})
	s.Require().NoError(err)
	s.Require().Len(out, 1, "the other blocker is also closed, so the candidate is fully unblocked")
	s.Equal("bd-ub-cb-dep", out[0].ID)
}

func (s *testSuite) depUnblockedEmptyID() {
	_, err := s.depRepo().ListNewlyUnblockedByClose(s.Ctx(), "", domain.DepListOpts{})
	s.Require().Error(err)
	s.Contains(err.Error(), "closedIssueID must not be empty")
}
