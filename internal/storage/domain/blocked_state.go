package domain

import (
	"context"

	"github.com/steveyegge/beads/internal/types"
)

type AffectedIDs struct {
	IssueIDs []string
	WispIDs  []string
}

type BlockedStateSQLRepository interface {
	Recompute(ctx context.Context, affected AffectedIDs) error
	MarkOnly(ctx context.Context, affected AffectedIDs) error

	AffectedByStatusChange(ctx context.Context, id string, isWisp bool) (AffectedIDs, error)
	AffectedByDepChange(ctx context.Context, source, target string, depType types.DependencyType, isWispSource bool) (AffectedIDs, error)
	AffectedByDeletion(ctx context.Context, deletedIssues, deletedWisps []string) (AffectedIDs, error)
}
