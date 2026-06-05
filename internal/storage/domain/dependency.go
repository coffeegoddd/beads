package domain

import (
	"context"
	"fmt"

	"github.com/steveyegge/beads/internal/storage/dberrors"
	"github.com/steveyegge/beads/internal/types"
)

type DepDirection int

const (
	DepDirectionBoth DepDirection = iota
	DepDirectionOut
	DepDirectionIn
)

type DepInsertOpts struct {
	UseWispsTable bool
}

type DepListOpts struct {
	Types         []types.DependencyType
	Direction     DepDirection
	UseWispsTable bool
}

type DepCountsOpts struct {
	UseWispsTable bool
}

type DepBulkResult struct {
	Outgoing map[string][]*types.Dependency
	Incoming map[string][]*types.Dependency
}

type DepListFilter struct {
	Types     []types.DependencyType
	Direction DepDirection
}

type BlockingInfo struct {
	BlockedBy map[string][]string
	Blocks    map[string][]string
	Parent    map[string]string
}

type DependencySQLRepository interface {
	Insert(ctx context.Context, dep *types.Dependency, actor string, opts DepInsertOpts) error
	HasCycle(ctx context.Context, issueID, dependsOnID string) (bool, error)
	ListByIssueIDs(ctx context.Context, issueIDs []string, opts DepListOpts) (DepBulkResult, error)
	CountsByIssueIDs(ctx context.Context, issueIDs []string, opts DepCountsOpts) (map[string]*types.DependencyCounts, error)

	GetBlockingInfo(ctx context.Context, issueIDs []string, opts DepListOpts) (BlockingInfo, error)
	GetBlockingInfoAcrossIssuesAndWisps(ctx context.Context, issueIDs []string) (BlockingInfo, error)

	Remove(ctx context.Context, issueID, dependsOnID, actor string, opts DepInsertOpts) error
	ListRecordsForIssue(ctx context.Context, issueID string, opts DepListOpts) ([]*types.Dependency, error)
	ListIssuesDependedOn(ctx context.Context, issueID string, opts DepListOpts) ([]*types.Issue, error)
	ListIssueDependents(ctx context.Context, issueID string, opts DepListOpts) ([]*types.Issue, error)
	ListDependentsWithMetadata(ctx context.Context, issueID string, opts DepListOpts) ([]*types.IssueWithDependencyMetadata, error)
	IsBlocked(ctx context.Context, issueID string, opts DepListOpts) (bool, []string, error)
	ListNewlyUnblockedByClose(ctx context.Context, closedIssueID string, opts DepListOpts) ([]*types.Issue, error)
}

type DependencyUseCase interface {
	AddDependency(ctx context.Context, dep *types.Dependency, actor string) error
	RemoveDependency(ctx context.Context, issueID, dependsOnID, actor string) error
	ListByIssueIDs(ctx context.Context, issueIDs []string, filter DepListFilter) (DepBulkResult, error)
	CountsByIssueIDs(ctx context.Context, issueIDs []string) (map[string]*types.DependencyCounts, error)
	GetBlockingInfo(ctx context.Context, issueIDs []string) (BlockingInfo, error)
	GetForIssueIDs(ctx context.Context, ids []string) (map[string][]*types.Dependency, error)

	GetDependencies(ctx context.Context, issueID string) ([]*types.Issue, error)
	GetDependents(ctx context.Context, issueID string) ([]*types.Issue, error)
	GetDependencyRecords(ctx context.Context, issueID string) ([]*types.Dependency, error)
	GetDependentsWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error)
	IsBlocked(ctx context.Context, issueID string) (bool, []string, error)
	GetNewlyUnblockedByClose(ctx context.Context, closedIssueID string) ([]*types.Issue, error)

	AddWispDependency(ctx context.Context, dep *types.Dependency, actor string) error
	ListByWispIDs(ctx context.Context, wispIDs []string, filter DepListFilter) (DepBulkResult, error)
	CountsByWispIDs(ctx context.Context, wispIDs []string) (map[string]*types.DependencyCounts, error)
}

func NewDependencyUseCase(depRepo DependencySQLRepository, blockedRepo BlockedStateSQLRepository) DependencyUseCase {
	return &dependencyUseCaseImpl{depRepo: depRepo, blockedRepo: blockedRepo}
}

type dependencyUseCaseImpl struct {
	depRepo     DependencySQLRepository
	blockedRepo BlockedStateSQLRepository
}

var _ DependencyUseCase = (*dependencyUseCaseImpl)(nil)

func (u *dependencyUseCaseImpl) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	return u.add(ctx, dep, actor, false)
}

func (u *dependencyUseCaseImpl) AddWispDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	return u.add(ctx, dep, actor, true)
}

func (u *dependencyUseCaseImpl) add(ctx context.Context, dep *types.Dependency, actor string, useWisp bool) error {
	if dep == nil {
		return fmt.Errorf("add dep: dep must not be nil")
	}
	if dep.IssueID == "" || dep.DependsOnID == "" {
		return fmt.Errorf("add dep: IssueID and DependsOnID must be non-empty")
	}

	if isBlockingDep(dep.Type) {
		cycle, err := u.depRepo.HasCycle(ctx, dep.IssueID, dep.DependsOnID)
		if err != nil {
			return fmt.Errorf("add dep: cycle check: %w", err)
		}
		if cycle {
			return fmt.Errorf("add dep: adding %s -> %s would create a cycle", dep.IssueID, dep.DependsOnID)
		}
	}

	if err := u.depRepo.Insert(ctx, dep, actor, DepInsertOpts{UseWispsTable: useWisp}); err != nil {
		return fmt.Errorf("add dep: insert: %w", err)
	}
	return nil
}

func (u *dependencyUseCaseImpl) ListByIssueIDs(ctx context.Context, issueIDs []string, filter DepListFilter) (DepBulkResult, error) {
	return u.list(ctx, issueIDs, filter, false)
}

func (u *dependencyUseCaseImpl) GetForIssueIDs(ctx context.Context, ids []string) (map[string][]*types.Dependency, error) {
	if len(ids) == 0 {
		return map[string][]*types.Dependency{}, nil
	}
	issueRes, err := u.depRepo.ListByIssueIDs(ctx, ids, DepListOpts{Direction: DepDirectionOut})
	if err != nil {
		return nil, fmt.Errorf("GetForIssueIDs: %w", err)
	}
	out := issueRes.Outgoing
	if out == nil {
		out = make(map[string][]*types.Dependency)
	}
	wispRes, err := u.depRepo.ListByIssueIDs(ctx, ids, DepListOpts{Direction: DepDirectionOut, UseWispsTable: true})
	if err != nil && !dberrors.IsTableNotExist(err) {
		return nil, fmt.Errorf("GetForIssueIDs (wisps): %w", err)
	}
	for id, deps := range wispRes.Outgoing {
		out[id] = append(out[id], deps...)
	}
	return out, nil
}

func (u *dependencyUseCaseImpl) ListByWispIDs(ctx context.Context, wispIDs []string, filter DepListFilter) (DepBulkResult, error) {
	return u.list(ctx, wispIDs, filter, true)
}

func (u *dependencyUseCaseImpl) list(ctx context.Context, ids []string, filter DepListFilter, useWisp bool) (DepBulkResult, error) {
	if len(ids) == 0 {
		return DepBulkResult{
			Outgoing: map[string][]*types.Dependency{},
			Incoming: map[string][]*types.Dependency{},
		}, nil
	}
	out, err := u.depRepo.ListByIssueIDs(ctx, ids, DepListOpts{
		Types:         filter.Types,
		Direction:     filter.Direction,
		UseWispsTable: useWisp,
	})
	if err != nil {
		return DepBulkResult{}, fmt.Errorf("list deps: %w", err)
	}
	return out, nil
}

func (u *dependencyUseCaseImpl) CountsByIssueIDs(ctx context.Context, issueIDs []string) (map[string]*types.DependencyCounts, error) {
	return u.counts(ctx, issueIDs, false)
}

func (u *dependencyUseCaseImpl) CountsByWispIDs(ctx context.Context, wispIDs []string) (map[string]*types.DependencyCounts, error) {
	return u.counts(ctx, wispIDs, true)
}

func (u *dependencyUseCaseImpl) counts(ctx context.Context, ids []string, useWisp bool) (map[string]*types.DependencyCounts, error) {
	if len(ids) == 0 {
		return map[string]*types.DependencyCounts{}, nil
	}
	out, err := u.depRepo.CountsByIssueIDs(ctx, ids, DepCountsOpts{UseWispsTable: useWisp})
	if err != nil {
		return nil, fmt.Errorf("dep counts: %w", err)
	}
	return out, nil
}

func (u *dependencyUseCaseImpl) GetBlockingInfo(ctx context.Context, issueIDs []string) (BlockingInfo, error) {
	if len(issueIDs) == 0 {
		return BlockingInfo{
			BlockedBy: map[string][]string{},
			Blocks:    map[string][]string{},
			Parent:    map[string]string{},
		}, nil
	}
	out, err := u.depRepo.GetBlockingInfoAcrossIssuesAndWisps(ctx, issueIDs)
	if err != nil {
		return BlockingInfo{}, fmt.Errorf("GetBlockingInfo: %w", err)
	}
	return out, nil
}

func (u *dependencyUseCaseImpl) RemoveDependency(ctx context.Context, issueID, dependsOnID, actor string) error {
	if issueID == "" || dependsOnID == "" {
		return fmt.Errorf("RemoveDependency: issueID and dependsOnID must not be empty")
	}
	return u.depRepo.Remove(ctx, issueID, dependsOnID, actor, DepInsertOpts{})
}

func (u *dependencyUseCaseImpl) GetDependencies(ctx context.Context, issueID string) ([]*types.Issue, error) {
	if issueID == "" {
		return nil, fmt.Errorf("GetDependencies: issueID must not be empty")
	}
	return u.depRepo.ListIssuesDependedOn(ctx, issueID, DepListOpts{})
}

func (u *dependencyUseCaseImpl) GetDependents(ctx context.Context, issueID string) ([]*types.Issue, error) {
	if issueID == "" {
		return nil, fmt.Errorf("GetDependents: issueID must not be empty")
	}
	return u.depRepo.ListIssueDependents(ctx, issueID, DepListOpts{})
}

func (u *dependencyUseCaseImpl) GetDependencyRecords(ctx context.Context, issueID string) ([]*types.Dependency, error) {
	if issueID == "" {
		return nil, fmt.Errorf("GetDependencyRecords: issueID must not be empty")
	}
	return u.depRepo.ListRecordsForIssue(ctx, issueID, DepListOpts{})
}

func (u *dependencyUseCaseImpl) GetDependentsWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error) {
	if issueID == "" {
		return nil, fmt.Errorf("GetDependentsWithMetadata: issueID must not be empty")
	}
	return u.depRepo.ListDependentsWithMetadata(ctx, issueID, DepListOpts{})
}

func (u *dependencyUseCaseImpl) IsBlocked(ctx context.Context, issueID string) (bool, []string, error) {
	if issueID == "" {
		return false, nil, fmt.Errorf("IsBlocked: issueID must not be empty")
	}
	return u.depRepo.IsBlocked(ctx, issueID, DepListOpts{})
}

func (u *dependencyUseCaseImpl) GetNewlyUnblockedByClose(ctx context.Context, closedIssueID string) ([]*types.Issue, error) {
	if closedIssueID == "" {
		return nil, fmt.Errorf("GetNewlyUnblockedByClose: closedIssueID must not be empty")
	}
	return u.depRepo.ListNewlyUnblockedByClose(ctx, closedIssueID, DepListOpts{})
}

func isBlockingDep(t types.DependencyType) bool {
	return t == types.DepBlocks || t == types.DepConditionalBlocks
}
