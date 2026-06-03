package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/timeparsing"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/ui"
	"github.com/steveyegge/beads/internal/utils"
	"github.com/steveyegge/beads/internal/validation"
)

type updateInput struct {
	ids              []string
	actor            string
	updates          map[string]any
	clearDeferStatus bool
	claim            bool
	jsonOutput       bool
	repoOverrideSet  bool
}

func gatherUpdateInput(cmd *cobra.Command, args []string) updateInput {
	in := updateInput{
		ids:        args,
		actor:      actor,
		updates:    map[string]any{},
		jsonOutput: jsonOutput,
	}

	if cmd.Flags().Changed("status") {
		status, _ := cmd.Flags().GetString("status")
		in.updates["status"] = status
		if status == "closed" {
			session, _ := cmd.Flags().GetString("session")
			if session == "" {
				session = os.Getenv("CLAUDE_SESSION_ID")
			}
			if session != "" {
				in.updates["closed_by_session"] = session
			}
		}
	}
	if cmd.Flags().Changed("priority") {
		priorityStr, _ := cmd.Flags().GetString("priority")
		priority, err := validation.ValidatePriority(priorityStr)
		if err != nil {
			FatalErrorRespectJSON("%v", err)
		}
		in.updates["priority"] = priority
	}
	if cmd.Flags().Changed("title") {
		title, _ := cmd.Flags().GetString("title")
		title = strings.TrimSpace(title)
		if title == "" {
			FatalErrorRespectJSON("title cannot be empty")
		}
		in.updates["title"] = title
	}
	if cmd.Flags().Changed("assignee") {
		assignee, _ := cmd.Flags().GetString("assignee")
		in.updates["assignee"] = assignee
	}

	description, descChanged := getDescriptionFlag(cmd)
	if descChanged {
		if err := validateDescriptionUpdate(cmd, description, descChanged); err != nil {
			FatalErrorRespectJSON("%v", err)
		}
		in.updates["description"] = description
	}
	design, designChanged := getDesignFlag(cmd)
	if designChanged {
		in.updates["design"] = design
	}

	if cmd.Flags().Changed("notes") && cmd.Flags().Changed("append-notes") {
		FatalErrorRespectJSON("cannot specify both --notes and --append-notes")
	}
	if cmd.Flags().Changed("notes") {
		notes, _ := cmd.Flags().GetString("notes")
		in.updates["notes"] = notes
	}
	if cmd.Flags().Changed("append-notes") {
		appendNotes, _ := cmd.Flags().GetString("append-notes")
		in.updates["append_notes"] = appendNotes
	}
	if cmd.Flags().Changed("acceptance") || cmd.Flags().Changed("acceptance-criteria") {
		var ac string
		if cmd.Flags().Changed("acceptance") {
			ac, _ = cmd.Flags().GetString("acceptance")
		} else {
			ac, _ = cmd.Flags().GetString("acceptance-criteria")
		}
		in.updates["acceptance_criteria"] = ac
	}
	if cmd.Flags().Changed("external-ref") {
		externalRef, _ := cmd.Flags().GetString("external-ref")
		if externalRef == "" {
			in.updates["external_ref"] = nil
		} else {
			in.updates["external_ref"] = externalRef
		}
	}
	if cmd.Flags().Changed("spec-id") {
		specID, _ := cmd.Flags().GetString("spec-id")
		in.updates["spec_id"] = specID
	}
	if cmd.Flags().Changed("estimate") {
		estimate, _ := cmd.Flags().GetInt("estimate")
		if estimate < 0 {
			FatalErrorRespectJSON("estimate must be a non-negative number of minutes")
		}
		in.updates["estimated_minutes"] = estimate
	}
	if cmd.Flags().Changed("type") {
		issueType, _ := cmd.Flags().GetString("type")
		issueType = utils.NormalizeIssueType(issueType)
		in.updates["issue_type"] = issueType
	}

	if cmd.Flags().Changed("add-label") {
		addLabels, _ := cmd.Flags().GetStringSlice("add-label")
		in.updates["add_labels"] = addLabels
	}
	if cmd.Flags().Changed("remove-label") {
		removeLabels, _ := cmd.Flags().GetStringSlice("remove-label")
		in.updates["remove_labels"] = removeLabels
	}
	if cmd.Flags().Changed("set-labels") {
		setLabels, _ := cmd.Flags().GetStringSlice("set-labels")
		in.updates["set_labels"] = setLabels
	}

	if cmd.Flags().Changed("parent") {
		parent, _ := cmd.Flags().GetString("parent")
		in.updates["parent"] = parent
	}
	if cmd.Flags().Changed("await-id") {
		awaitID, _ := cmd.Flags().GetString("await-id")
		in.updates["await_id"] = awaitID
	}

	if cmd.Flags().Changed("due") {
		dueStr, _ := cmd.Flags().GetString("due")
		if dueStr == "" {
			in.updates["due_at"] = nil
		} else {
			t, err := timeparsing.ParseRelativeTime(dueStr, time.Now())
			if err != nil {
				FatalErrorRespectJSON("invalid --due format %q. Examples: +6h, tomorrow, next monday, 2025-01-15", dueStr)
			}
			in.updates["due_at"] = t
		}
	}
	if cmd.Flags().Changed("defer") {
		deferStr, _ := cmd.Flags().GetString("defer")
		if deferStr == "" {
			in.updates["defer_until"] = nil
			if _, ok := in.updates["status"]; !ok {
				in.clearDeferStatus = true
			}
		} else {
			t, err := timeparsing.ParseRelativeTime(deferStr, time.Now())
			if err != nil {
				FatalErrorRespectJSON("invalid --defer format %q. Examples: +1h, tomorrow, next monday, 2025-01-15", deferStr)
			}
			inPast := t.Before(time.Now())
			if inPast && !jsonOutput {
				fmt.Fprintf(os.Stderr, "%s Defer date %q is in the past. Issue will appear in bd ready immediately.\n",
					ui.RenderWarn("!"), t.Format("2006-01-02 15:04"))
				fmt.Fprintf(os.Stderr, "  Did you mean a future date? Use --defer=+1h or --defer=tomorrow\n")
			}
			in.updates["defer_until"] = t
			if _, ok := in.updates["status"]; !ok && !inPast {
				in.updates["status"] = string(types.StatusDeferred)
			}
		}
	}

	ephemeralChanged := cmd.Flags().Changed("ephemeral")
	persistentChanged := cmd.Flags().Changed("persistent")
	noHistoryChanged := cmd.Flags().Changed("no-history")
	historyChanged := cmd.Flags().Changed("history")
	if ephemeralChanged && persistentChanged {
		FatalErrorRespectJSON("cannot specify both --ephemeral and --persistent flags")
	}
	if noHistoryChanged && ephemeralChanged {
		FatalErrorRespectJSON("cannot specify both --no-history and --ephemeral flags")
	}
	if noHistoryChanged && historyChanged {
		FatalErrorRespectJSON("cannot specify both --no-history and --history flags")
	}
	if ephemeralChanged {
		in.updates["wisp"] = true
	}
	if persistentChanged {
		in.updates["wisp"] = false
	}
	if noHistoryChanged {
		in.updates["no_history"] = true
	}
	if historyChanged {
		in.updates["no_history"] = false
	}

	if cmd.Flags().Changed("metadata") {
		metadataValue, _ := cmd.Flags().GetString("metadata")
		var metadataJSON string
		if strings.HasPrefix(metadataValue, "@") {
			filePath := metadataValue[1:]
			// #nosec G304 -- user explicitly provides file path via @file.json syntax
			data, err := os.ReadFile(filePath)
			if err != nil {
				FatalErrorRespectJSON("failed to read metadata file %s: %v", filePath, err)
			}
			metadataJSON = string(data)
		} else {
			metadataJSON = metadataValue
		}
		if !json.Valid([]byte(metadataJSON)) {
			FatalErrorRespectJSON("invalid JSON in --metadata: must be valid JSON")
		}
		in.updates["metadata"] = json.RawMessage(metadataJSON)
	}

	setMetadataFlags, _ := cmd.Flags().GetStringArray("set-metadata")
	unsetMetadataFlags, _ := cmd.Flags().GetStringArray("unset-metadata")
	if (len(setMetadataFlags) > 0 || len(unsetMetadataFlags) > 0) && cmd.Flags().Changed("metadata") {
		FatalErrorRespectJSON("cannot combine --metadata with --set-metadata or --unset-metadata")
	}
	if len(setMetadataFlags) > 0 || len(unsetMetadataFlags) > 0 {
		in.updates["_set_metadata"] = setMetadataFlags
		in.updates["_unset_metadata"] = unsetMetadataFlags
	}

	in.claim, _ = cmd.Flags().GetBool("claim")
	in.repoOverrideSet = cmd.Flags().Changed("repo")

	return in
}

func hasLabelChanges(in updateInput) bool {
	_, set := in.updates["set_labels"].([]string)
	_, add := in.updates["add_labels"].([]string)
	_, rem := in.updates["remove_labels"].([]string)
	return set || add || rem
}

func labelChangeSlices(in updateInput) (setLabels, addLabels, removeLabels []string) {
	if v, ok := in.updates["set_labels"].([]string); ok {
		setLabels = v
	}
	if v, ok := in.updates["add_labels"].([]string); ok {
		addLabels = v
	}
	if v, ok := in.updates["remove_labels"].([]string); ok {
		removeLabels = v
	}
	return
}
