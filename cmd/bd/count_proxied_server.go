package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/workapi"
)

func runCountProxiedServer(cmd *cobra.Command, ctx context.Context) error {
	parseDone := latSpan("count:parseCountFilter")
	filter, groupBy, issueType, includeInfra, err := parseCountFilter(cmd)
	parseDone()
	if err != nil {
		return err
	}

	uw, err := openProxiedListUOW(ctx)
	if err != nil {
		return HandleError("%v", err)
	}
	defer uw.Close(ctx)

	if includeInfra {
		cfgDone := latSpan("count:LoadUOWListConfig")
		cfg, err := workapi.LoadUOWListConfig(ctx, uw)
		cfgDone()
		if err != nil {
			return HandleError("%v", err)
		}
		applyCountIncludeInfra(&filter, issueType, cfg)
	} else {
		filter.SkipWisps = true
	}

	return executeCount(ctx, uw.IssueUseCase(), filter, groupBy)
}
