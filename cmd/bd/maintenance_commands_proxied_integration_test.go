//go:build cgo

package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestProxiedServerPing(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()
	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "ping")

	t.Run("human", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "ping")
		if err != nil {
			t.Fatalf("bd ping failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "bd ping: ok") {
			t.Errorf("expected 'bd ping: ok' in output, got: %s", stdout)
		}
	})

	t.Run("json", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "ping", "--json")
		if err != nil {
			t.Fatalf("bd ping --json failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, `"status": "ok"`) {
			t.Errorf("expected status ok in JSON, got: %s", stdout)
		}
	})
}

func TestProxiedServerGC(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()
	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "gc")

	t.Run("dry_run_all_phases", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "gc", "--dry-run")
		if err != nil {
			t.Fatalf("bd gc --dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		for _, want := range []string{"Phase 1/3", "Phase 2/3", "Phase 3/3", "DRY RUN complete"} {
			if !strings.Contains(stdout, want) {
				t.Errorf("expected %q in gc --dry-run output, got: %s", want, stdout)
			}
		}
	})

	t.Run("decay_deletes_closed_issue", func(t *testing.T) {
		issue := bdProxiedCreate(t, bd, p.dir, "decay me", "--type", "task")
		bdProxiedClose(t, bd, p.dir, issue.ID)
		bdProxiedSQL(t, bd, p.dir,
			"UPDATE issues SET closed_at = '2000-01-01 00:00:00' WHERE id = '"+issue.ID+"'")

		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir,
			"gc", "--older-than", "1", "--skip-dolt", "--force")
		if err != nil {
			t.Fatalf("bd gc decay failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "Deleted 1 issue") {
			t.Errorf("expected 'Deleted 1 issue' in output, got: %s", stdout)
		}

		rows := bdProxiedSQLJSON(t, bd, p.dir,
			"SELECT COUNT(*) as count FROM issues WHERE id = '"+issue.ID+"'")
		if len(rows) != 1 || !sqlValueEquals(rows[0]["count"], 0) {
			t.Errorf("expected deleted issue gone, got: %v", rows)
		}
	})

	t.Run("refuses_without_force", func(t *testing.T) {
		issue := bdProxiedCreate(t, bd, p.dir, "keep me", "--type", "task")
		bdProxiedClose(t, bd, p.dir, issue.ID)
		bdProxiedSQL(t, bd, p.dir,
			"UPDATE issues SET closed_at = '2000-01-01 00:00:00' WHERE id = '"+issue.ID+"'")

		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir,
			"gc", "--older-than", "1", "--skip-dolt")
		if err == nil {
			t.Fatalf("bd gc without --force should have failed\nstdout:\n%s", stdout)
		}
		if !strings.Contains(stdout+stderr, "--force") {
			t.Errorf("expected --force hint in refusal, got:\n%s\n%s", stdout, stderr)
		}
	})
}

func TestProxiedServerCompactDolt(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()
	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "compact")

	t.Run("dolt_dry_run", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "admin", "compact", "--dolt", "--dry-run")
		if err != nil {
			t.Fatalf("bd admin compact --dolt --dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "Dolt garbage collection") {
			t.Errorf("expected dry-run GC message, got: %s", stdout)
		}
	})

	t.Run("non_dolt_mode_rejected", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "admin", "compact", "--stats")
		if err == nil {
			t.Fatalf("bd admin compact --stats should be rejected in proxied mode\nstdout:\n%s", stdout)
		}
		if !strings.Contains(stdout+stderr, "only 'compact --dolt' is supported") {
			t.Errorf("expected scoped rejection message, got:\n%s\n%s", stdout, stderr)
		}
	})
}

func TestProxiedServerCleanDatabases(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()
	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "clean")

	staleDB := fmt.Sprintf("testdb_clean_probe_%s", strings.ReplaceAll(p.database, "bdtest_", ""))
	bdProxiedSQL(t, bd, p.dir, "CREATE DATABASE "+staleDB)

	t.Run("dry_run_lists_without_dropping", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "dolt", "clean-databases", "--dry-run")
		if err != nil {
			t.Fatalf("clean-databases --dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, staleDB) || !strings.Contains(stdout, "dry run") {
			t.Errorf("expected dry-run to list %s, got: %s", staleDB, stdout)
		}
	})

	t.Run("drops_stale_database", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "dolt", "clean-databases")
		if err != nil {
			t.Fatalf("clean-databases failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "Dropped: "+staleDB) {
			t.Errorf("expected %s dropped, got: %s", staleDB, stdout)
		}

		rows := bdProxiedSQLJSON(t, bd, p.dir,
			"SELECT COUNT(*) as count FROM information_schema.schemata WHERE schema_name = '"+staleDB+"'")
		if len(rows) != 1 || !sqlValueEquals(rows[0]["count"], 0) {
			t.Errorf("expected %s gone, got: %v", staleDB, rows)
		}
	})
}
