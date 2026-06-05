package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/steveyegge/beads/internal/storage/domain"
	"github.com/steveyegge/beads/internal/storage/uow"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/ui"
)

func runUpdateProxiedServer(_ *cobra.Command, ctx context.Context, in updateInput) error {
	if in.repoOverrideSet {
		return errors.New("--repo is not supported with --proxied-server")
	}
	if uowProvider == nil {
		return errors.New("proxied-server UOW provider not initialized")
	}
	if len(in.updates) == 0 && !in.claim {
		fmt.Println("No updates specified")
		return nil
	}

	uw, err := uowProvider.NewUOW(ctx)
	if err != nil {
		return fmt.Errorf("open unit of work: %w", err)
	}
	defer uw.Close(ctx)

	if statusVal, ok := in.updates["status"].(string); ok {
		if err := validateProxiedStatus(ctx, uw, statusVal); err != nil {
			return err
		}
	}

	var updatedIssues []*types.Issue
	var firstUpdatedID string

	for _, id := range in.ids {
		if in.claim {
			if err := uw.IssueUseCase().ClaimIssue(ctx, id, in.actor); err != nil {
				return fmt.Errorf("claiming %s: %w", id, err)
			}
		}

		params, hasRegular := buildUpdateIssueParams(in)
		var updated *types.Issue
		if hasRegular {
			updated, err = uw.IssueUseCase().UpdateIssue(ctx, id, params, in.actor)
			if err != nil {
				return fmt.Errorf("updating %s: %w", id, err)
			}
		}

		if hasLabelChanges(in) {
			setLabels, addLabels, removeLabels := labelChangeSlices(in)
			if err := applyLabelUpdatesUOW(ctx, uw, id, in.actor, setLabels, addLabels, removeLabels); err != nil {
				return fmt.Errorf("updating labels for %s: %w", id, err)
			}
		}

		if newParent, ok := in.updates["parent"].(string); ok {
			if err := uw.IssueUseCase().Reparent(ctx, id, newParent, in.actor); err != nil {
				return fmt.Errorf("reparenting %s: %w", id, err)
			}
		}

		if updated == nil {
			updated, err = uw.IssueUseCase().GetIssue(ctx, id)
			if err != nil {
				return fmt.Errorf("re-fetching %s: %w", id, err)
			}
		}

		if in.jsonOutput {
			updatedIssues = append(updatedIssues, updated)
		} else {
			fmt.Printf("%s Updated issue: %s\n", ui.RenderPass("✓"), formatFeedbackID(id, updated.Title))
		}

		if firstUpdatedID == "" {
			firstUpdatedID = id
		}
	}

	if firstUpdatedID != "" {
		commitMsg := fmt.Sprintf("bd: update %s", firstUpdatedID)
		if err := uw.Commit(ctx, commitMsg); err != nil && !isDoltNothingToCommit(err) {
			return fmt.Errorf("commit: %w", err)
		}
	}

	if in.jsonOutput && len(updatedIssues) > 0 {
		outputJSON(updatedIssues)
	}

	if len(in.ids) > 0 && firstUpdatedID == "" {
		os.Exit(1)
	}
	return nil
}

func validateProxiedStatus(ctx context.Context, uw uow.UnitOfWork, status string) error {
	custom, err := uw.ConfigUseCase().GetCustomStatuses(ctx)
	if err != nil {
		return fmt.Errorf("load custom statuses: %w", err)
	}
	names := make([]string, 0, len(custom))
	for _, cs := range custom {
		names = append(names, cs.Name)
	}
	if !types.Status(status).IsValidWithCustom(names) {
		return fmt.Errorf("invalid status %q (built-in: open, in_progress, blocked, deferred, closed, pinned, hooked; or configure custom statuses via 'bd config set status.custom')", status)
	}
	return nil
}

var directFieldKeys = []string{
	"status", "closed_by_session", "priority", "title", "assignee",
	"description", "design", "notes", "acceptance_criteria",
	"external_ref", "spec_id", "estimated_minutes", "issue_type",
	"await_id", "due_at", "defer_until", "wisp", "no_history",
}

func buildUpdateIssueParams(in updateInput) (domain.UpdateIssueParams, bool) {
	params := domain.UpdateIssueParams{
		ClearDeferStatus: in.clearDeferStatus,
	}
	has := in.clearDeferStatus

	fields := make(map[string]any)
	for _, k := range directFieldKeys {
		if v, ok := in.updates[k]; ok {
			fields[k] = v
			has = true
		}
	}
	if len(fields) > 0 {
		params.Fields = fields
	}

	if v, ok := in.updates["metadata"].(json.RawMessage); ok {
		params.MergeMetadata = v
		has = true
	}
	if v, ok := in.updates["_set_metadata"].([]string); ok && len(v) > 0 {
		params.SetMetadata = v
		has = true
	}
	if v, ok := in.updates["_unset_metadata"].([]string); ok && len(v) > 0 {
		params.UnsetMetadata = v
		has = true
	}
	if v, ok := in.updates["append_notes"].(string); ok {
		s := v
		params.AppendNotes = &s
		has = true
	}

	return params, has
}

func applyLabelUpdatesUOW(ctx context.Context, uw uow.UnitOfWork, issueID, actor string, setLabels, addLabels, removeLabels []string) error {
	if len(setLabels) > 0 {
		if err := uw.LabelUseCase().SetLabels(ctx, issueID, setLabels, actor); err != nil {
			return err
		}
	}
	for _, label := range addLabels {
		if err := uw.LabelUseCase().AddLabel(ctx, issueID, label, actor); err != nil {
			return err
		}
	}
	for _, label := range removeLabels {
		if err := uw.LabelUseCase().RemoveLabel(ctx, issueID, label, actor); err != nil {
			return err
		}
	}
	return nil
}
