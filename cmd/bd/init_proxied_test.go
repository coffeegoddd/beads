//go:build cgo

package main

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
	"github.com/steveyegge/beads/internal/testutil"
)

// shutdownProxyOnInterrupt mirrors the helper in
// internal/storage/uow/doltserver_provider_test.go: ensures the per-workspace
// proxy + child dolt sql-server don't survive a Ctrl+C during the test.
func shutdownProxyOnInterrupt(t *testing.T, rootDir string) {
	t.Helper()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case <-ch:
			_ = proxy.Shutdown(rootDir)
			os.Exit(1)
		case <-done:
		}
	}()
	t.Cleanup(func() {
		signal.Stop(ch)
		close(done)
	})
}

// TestProxiedServerInit is the happy-path integration test for the UoW-driven
// init flow. It asserts that bd init --proxied-server constructs a UoW
// provider (spawning the proxy + child dolt), opens a Unit of Work, calls
// every BootstrapUseCase method through the UoW, writes metadata.json with
// dolt_mode=proxied-server, materializes the per-workspace server config
// under .beads/proxieddb/, and exits cleanly.
//
// Stub BootstrapUseCase methods currently return domain.ErrUseCaseNotImplemented;
// init downgrades those to warnings. The test asserts at least one such
// warning is emitted as evidence the wiring is live — that assertion will
// flip to "warning must NOT appear" once domain.BootstrapUseCase
// implementations land.
func TestProxiedServerInit(t *testing.T) {
	testutil.RequireDoltBinary(t)
	if _, err := exec.LookPath("dolt"); err != nil {
		t.Skipf("dolt binary not in PATH: %v", err)
	}

	bd := buildEmbeddedBD(t)

	dir := t.TempDir()
	initGitRepoAt(t, dir)

	beadsDir := filepath.Join(dir, ".beads")
	proxiedRoot := filepath.Join(beadsDir, "proxieddb")
	shutdownProxyOnInterrupt(t, proxiedRoot)
	t.Cleanup(func() {
		if err := proxy.Shutdown(proxiedRoot); err != nil {
			t.Logf("proxy.Shutdown(%s): %v", proxiedRoot, err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, bd,
		"init",
		"--proxied-server",
		"--prefix", "px",
		"--skip-hooks",
		"--skip-agents",
		"--non-interactive",
	)
	cmd.Dir = dir
	cmd.Env = bdEnv(dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bd init --proxied-server failed: %v\n%s", err, out)
	}

	cfg, err := configfile.Load(beadsDir)
	if err != nil {
		t.Fatalf("load metadata.json: %v", err)
	}
	if cfg == nil {
		t.Fatal("metadata.json missing after init")
	}
	if !cfg.IsDoltProxiedServerMode() {
		t.Errorf("metadata.json dolt_mode = %q, want %q",
			cfg.DoltMode, configfile.DoltModeProxiedServer)
	}

	configPath := filepath.Join(proxiedRoot, "server_config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		t.Errorf("expected proxied-server config at %s: %v", configPath, err)
	}

	if strings.Contains(string(out), "panic:") {
		t.Errorf("init output contained panic:\n%s", out)
	}

	if !strings.Contains(string(out), "skipped in proxied-server mode (use case not implemented)") {
		t.Errorf("expected at least one BootstrapUseCase stub warning in init output, got:\n%s", out)
	}
}
