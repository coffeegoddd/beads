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

func TestProxiedServerCompactHistory(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()
	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "cphist")

	for _, title := range []string{"keep-alpha", "keep-beta", "keep-gamma"} {
		bdProxiedCreate(t, bd, p.dir, title, "--type", "task")
	}

	commitsBefore := proxiedCommitCount(t, bd, p)
	if commitsBefore <= 1 {
		t.Fatalf("expected multiple commits before compaction, got %d", commitsBefore)
	}

	t.Run("dry_run_previews", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "compact", "--days", "0", "--dry-run")
		if err != nil {
			t.Fatalf("compact --dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "DRY RUN") || !strings.Contains(stdout, "Run with --force") {
			t.Errorf("expected dry-run preview, got: %s", stdout)
		}
	})

	t.Run("refuses_without_force", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "compact", "--days", "0")
		if err == nil {
			t.Fatalf("compact without --force should have failed\nstdout:\n%s", stdout)
		}
		if !strings.Contains(stdout+stderr, "--force") {
			t.Errorf("expected --force hint, got:\n%s\n%s", stdout, stderr)
		}
	})

	t.Run("squashes_and_preserves_data", func(t *testing.T) {
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, "compact", "--days", "0", "--force")
		if err != nil {
			t.Fatalf("compact --force failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
		}
		if !strings.Contains(stdout, "Compacted") {
			t.Errorf("expected 'Compacted' summary, got: %s", stdout)
		}

		commitsAfter := proxiedCommitCount(t, bd, p)
		if commitsAfter >= commitsBefore {
			t.Errorf("expected fewer commits after compaction: before=%d after=%d", commitsBefore, commitsAfter)
		}

		rows := bdProxiedSQLJSON(t, bd, p.dir, "SELECT title FROM issues ORDER BY title")
		if len(rows) != 3 {
			t.Fatalf("expected 3 issues to survive compaction, got %d: %v", len(rows), rows)
		}
		for i, want := range []string{"keep-alpha", "keep-beta", "keep-gamma"} {
			if rows[i]["title"] != want {
				t.Errorf("issue %d: expected %q, got %q", i, want, rows[i]["title"])
			}
		}
	})
}

func proxiedCommitCount(t *testing.T, bd string, p proxiedProject) int {
	t.Helper()
	rows := bdProxiedSQLJSON(t, bd, p.dir, "SELECT COUNT(*) as count FROM dolt_log")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row from dolt_log count, got %d", len(rows))
	}
	switch v := rows[0]["count"].(type) {
	case float64:
		return int(v)
	case string:
		n := 0
		fmt.Sscanf(v, "%d", &n)
		return n
	default:
		t.Fatalf("unexpected count type %T: %v", v, v)
		return 0
	}
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
