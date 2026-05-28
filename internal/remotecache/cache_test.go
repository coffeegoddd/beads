package remotecache

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
)

// skipIfNoDolt skips the test if the dolt CLI is not installed. Under
// GitHub Actions the test fails instead — CI must install dolt.
//
// (Duplicated inline rather than importing testutil.RequireDoltBinary to avoid
// an import cycle: testutil → doltutil → remotecache.)
func skipIfNoDolt(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("dolt"); err != nil {
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatalf("dolt binary missing under GITHUB_ACTIONS: %v — the CI workflow must install dolt", err)
		}
		t.Skipf("dolt CLI not found, skipping integration test: %v", err)
	}
}

// initDoltRemote creates a file:// dolt remote by initializing a dolt repo,
// adding a file:// remote, and pushing to it. Returns the file:// URL that
// can be used with dolt clone.
func initDoltRemote(t *testing.T, dir string) string {
	t.Helper()

	// Create the "source" repo that we'll push from
	srcDir := filepath.Join(dir, "src")
	if err := os.MkdirAll(srcDir, 0o750); err != nil {
		t.Fatal(err)
	}

	// dolt init
	cmd := exec.Command("dolt", "init", "--name", "test", "--email", "test@test.com")
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt init failed: %v\n%s", err, out)
	}

	// Create a table so there's data to clone
	cmd = exec.Command("dolt", "sql", "-q", "CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(100))")
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create table failed: %v\n%s", err, out)
	}

	cmd = exec.Command("dolt", "add", ".")
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt add failed: %v\n%s", err, out)
	}

	cmd = exec.Command("dolt", "commit", "-m", "init")
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt commit failed: %v\n%s", err, out)
	}

	// Create the remote directory and add it as a file:// remote
	remoteDir := filepath.Join(dir, "remote-storage")
	if err := os.MkdirAll(remoteDir, 0o750); err != nil {
		t.Fatal(err)
	}
	remoteURL := "file://" + remoteDir

	cmd = exec.Command("dolt", "remote", "add", "origin", remoteURL)
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt remote add failed: %v\n%s", err, out)
	}

	// Push to create the remote storage via SQL (CALL DOLT_PUSH).
	cmd = exec.Command("dolt", "sql", "-q", "CALL DOLT_PUSH('origin', 'main')")
	cmd.Dir = srcDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dolt push failed: %v\n%s", err, out)
	}

	return remoteURL
}

// nopOpener satisfies StoreOpener for tests that exercise only cold-start
// behavior (clone). Tests that actually invoke push/pull live at the cmd
// layer where a real opener is available.
var nopOpener StoreOpener = func(_ context.Context, _ string) (storage.DoltStorage, error) {
	panic("nopOpener should not be invoked in cold-start tests")
}

func TestEnsureColdStart(t *testing.T) {
	skipIfNoDolt(t)
	ctx := context.Background()

	tmpDir := t.TempDir()
	remoteURL := initDoltRemote(t, filepath.Join(tmpDir, "remote"))

	cache := &Cache{Dir: filepath.Join(tmpDir, "cache")}
	entryDir, err := cache.Ensure(ctx, remoteURL, nopOpener)
	if err != nil {
		t.Fatalf("Ensure (cold) failed: %v", err)
	}

	// Verify the clone exists
	target := cache.cloneTarget(remoteURL)
	if !cache.doltExists(target) {
		t.Errorf("expected .dolt directory at %s", target)
	}

	// Verify metadata was written
	meta := cache.readMeta(remoteURL)
	if meta.RemoteURL != remoteURL {
		t.Errorf("meta.RemoteURL = %q, want %q", meta.RemoteURL, remoteURL)
	}
	if meta.LastPull == 0 {
		t.Error("meta.LastPull should be set after Ensure")
	}

	// Entry dir should be the parent of the clone target
	if entryDir != cache.entryDir(remoteURL) {
		t.Errorf("entryDir = %q, want %q", entryDir, cache.entryDir(remoteURL))
	}
}

func TestEvict(t *testing.T) {
	skipIfNoDolt(t)
	ctx := context.Background()

	tmpDir := t.TempDir()
	remoteURL := initDoltRemote(t, filepath.Join(tmpDir, "remote"))

	cache := &Cache{Dir: filepath.Join(tmpDir, "cache")}
	if _, err := cache.Ensure(ctx, remoteURL, nopOpener); err != nil {
		t.Fatalf("Ensure failed: %v", err)
	}

	// Verify cache exists
	if !cache.doltExists(cache.cloneTarget(remoteURL)) {
		t.Fatal("expected cache entry to exist before eviction")
	}

	// Evict
	if err := cache.Evict(remoteURL); err != nil {
		t.Fatalf("Evict failed: %v", err)
	}

	// Verify gone
	if cache.doltExists(cache.cloneTarget(remoteURL)) {
		t.Error("expected cache entry to be gone after eviction")
	}
}

func TestDefaultCache(t *testing.T) {
	cache, err := DefaultCache()
	if err != nil {
		t.Fatalf("DefaultCache failed: %v", err)
	}
	if cache.Dir == "" {
		t.Error("cache.Dir should not be empty")
	}
	// Should end with beads/remotes
	if filepath.Base(filepath.Dir(cache.Dir)) != "beads" || filepath.Base(cache.Dir) != "remotes" {
		t.Errorf("unexpected cache dir: %s", cache.Dir)
	}
}
