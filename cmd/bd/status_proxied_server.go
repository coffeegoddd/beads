package main

import (
	"context"

	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
)

func runStatusProxiedServer(ctx context.Context, showAssigned, noActivity bool) error {
	uw, err := openProxiedListUOW(ctx)
	if err != nil {
		return HandleError("%v", err)
	}
	defer uw.Close(ctx)

	statsDone := latSpan("uow:GetStatistics")
	stats, err := uw.IssueUseCase().GetStatistics(ctx)
	statsDone()
	if err != nil {
		return HandleErrorRespectJSON("%v", err)
	}

	if showAssigned {
		assignedDone := latSpan("status:proxiedAssignedStatistics")
		stats, err = proxiedAssignedStatistics(ctx, uw, actor)
		assignedDone()
		if err != nil {
			return HandleErrorRespectJSON("failed to get assigned statistics: %v", err)
		}
	}

	var recentActivity *RecentActivitySummary
	if !noActivity {
		activityDone := latSpan("status:getGitActivity")
		recentActivity = getGitActivity(24)
		activityDone()
	}

	renderDone := latSpan("status:renderStatus")
	defer renderDone()
	return renderStatus(stats, recentActivity)
}

func proxiedAssignedStatistics(ctx context.Context, uw uow.UnitOfWork, assignee string) (*types.Statistics, error) {
	assigneePtr := assignee
	searchDone := latSpan("uow:SearchIssues(assigned)")
	page, err := uw.IssueUseCase().SearchIssues(ctx, "", types.IssueFilter{Assignee: &assigneePtr})
	searchDone()
	if err != nil {
		return nil, err
	}

	readyCount := 0
	readyDone := latSpan("uow:GetReadyWork(assigned)")
	readyPage, err := uw.IssueUseCase().GetReadyWork(ctx, types.WorkFilter{Assignee: &assigneePtr})
	readyDone()
	if err == nil {
		readyCount = len(readyPage.Items)
	}

	return buildAssignedStats(page.Items, readyCount), nil
}
