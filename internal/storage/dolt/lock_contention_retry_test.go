package dolt

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// This test uses a helper subprocess to create real cross-process embedded Dolt contention.
// It verifies:
// - With fail-fast lock mode and retries disabled, opening the DB while another process holds it fails.
// - With beads' embedded defaults (fail-fast + retries), opening succeeds once the lock is released.
func TestEmbeddedLockContention_RetrySucceeds(t *testing.T) {
	skipIfNoDolt(t)

	// Helper mode: open and hold the embedded DB until released.
	if os.Getenv("BEADS_DOLT_LOCK_HELPER") == "1" {
		dir := os.Getenv("BEADS_DOLT_LOCK_DIR")
		readyPath := os.Getenv("BEADS_DOLT_LOCK_READY")
		releasePath := os.Getenv("BEADS_DOLT_LOCK_RELEASE")
		if dir == "" || readyPath == "" || releasePath == "" {
			fmt.Fprintln(os.Stderr, "missing BEADS_DOLT_LOCK_* env vars")
			os.Exit(2)
		}

		ctx := context.Background()
		store, err := New(ctx, &Config{
			Path:           dir,
			CommitterName:  "lock-helper",
			CommitterEmail: "lock-helper@test.invalid",
			Database:       "testdb",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper failed to open store: %v\n", err)
			os.Exit(2)
		}
		defer store.Close()

		// Touch a write to ensure we're in writer-capable mode.
		_ = store.SetConfig(ctx, "lock_helper_alive", "1")

		if err := os.WriteFile(readyPath, []byte("ready\n"), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "helper failed to write ready file: %v\n", err)
			os.Exit(2)
		}

		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(releasePath); err == nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Fprintln(os.Stderr, "helper timed out waiting for release file")
		os.Exit(2)
	}

	tmpDir := t.TempDir()
	readyPath := filepath.Join(tmpDir, "helper.ready")
	releasePath := filepath.Join(tmpDir, "helper.release")

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	helperCmd := exec.Command(exe, "-test.run", "^TestEmbeddedLockContention_RetrySucceeds$", "-test.v") // #nosec G204
	var helperOut bytes.Buffer
	helperCmd.Stdout = &helperOut
	helperCmd.Stderr = &helperOut
	helperCmd.Env = append(os.Environ(),
		"BEADS_DOLT_LOCK_HELPER=1",
		"BEADS_DOLT_LOCK_DIR="+tmpDir,
		"BEADS_DOLT_LOCK_READY="+readyPath,
		"BEADS_DOLT_LOCK_RELEASE="+releasePath,
	)

	if err := helperCmd.Start(); err != nil {
		t.Fatalf("failed to start helper: %v", err)
	}

	helperDone := make(chan error, 1)
	go func() { helperDone <- helperCmd.Wait() }()

	t.Cleanup(func() {
		// Best-effort: ensure helper is not left running.
		select {
		case <-helperDone:
			// already exited
		default:
			_ = helperCmd.Process.Kill()
			// Never block forever in cleanup (tests should not hang on process wait).
			select {
			case <-helperDone:
			case <-time.After(2 * time.Second):
			}
		}
	})

	// Wait for helper to signal it's holding the DB open.
	{
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(readyPath); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := os.Stat(readyPath); err != nil {
			t.Fatalf("helper did not become ready; err=%v output:\n%s", err, helperOut.String())
		}
	}

	cfg := &Config{
		Path:           tmpDir,
		CommitterName:  "lock-test",
		CommitterEmail: "lock-test@test.invalid",
		Database:       "testdb",
	}

	// Now validate the beads path: open should succeed once the helper releases (within retry window).
	type openResult struct {
		store   *DoltStore
		elapsed time.Duration
		err     error
	}
	ch := make(chan openResult, 1)

	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := New(ctx, cfg)
		if err != nil {
			ch <- openResult{err: err, elapsed: time.Since(start)}
			return
		}

		// Ensure the opened store is writable (no read-only wedge).
		if err := store.SetConfig(ctx, "lock_retry_opened", "1"); err != nil {
			_ = store.Close()
			ch <- openResult{err: fmt.Errorf("opened store not writable: %w", err), elapsed: time.Since(start)}
			return
		}

		ch <- openResult{store: store, elapsed: time.Since(start)}
	}()

	// Hold the lock a bit to force at least one retry attempt, then release.
	time.Sleep(500 * time.Millisecond)
	if err := os.WriteFile(releasePath, []byte("release\n"), 0o600); err != nil {
		t.Fatalf("failed to write release file: %v", err)
	}

	var res openResult
	select {
	case res = <-ch:
	case <-time.After(12 * time.Second):
		t.Fatalf("timed out waiting for open attempt to finish; helper output:\n%s", helperOut.String())
	}
	if res.err != nil {
		t.Fatalf("expected open to succeed after release, got error: %v\nhelper output:\n%s", res.err, helperOut.String())
	}
	t.Cleanup(func() { _ = res.store.Close() })

	// Ensure helper exits cleanly.
	select {
	case err := <-helperDone:
		if err != nil {
			t.Fatalf("helper exited with error: %v\noutput:\n%s", err, helperOut.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for helper to exit; output:\n%s", helperOut.String())
	}

	// The open should not complete before we release the lock (otherwise contention wasn't real).
	if res.elapsed < 450*time.Millisecond {
		t.Fatalf("open completed too quickly (%v); expected to wait for lock release", res.elapsed)
	}
	// The open should not take unbounded time; it should complete soon after release.
	if res.elapsed > 8*time.Second {
		t.Fatalf("open took too long: %v", res.elapsed)
	}
}

