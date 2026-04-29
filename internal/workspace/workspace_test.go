package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHasBeadsProjectFiles(t *testing.T) {
	t.Run("empty dir returns false", func(t *testing.T) {
		dir := t.TempDir()
		if hasBeadsProjectFiles(dir) {
			t.Error("expected false for empty dir")
		}
	})

	t.Run("metadata.json makes it a project", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
		if !hasBeadsProjectFiles(dir) {
			t.Error("expected true with metadata.json")
		}
	})

	t.Run("config.yaml makes it a project", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(""), 0644); err != nil {
			t.Fatal(err)
		}
		if !hasBeadsProjectFiles(dir) {
			t.Error("expected true with config.yaml")
		}
	})

	t.Run("dolt dir makes it a project", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "dolt"), 0755); err != nil {
			t.Fatal(err)
		}
		if !hasBeadsProjectFiles(dir) {
			t.Error("expected true with dolt dir")
		}
	})

	t.Run("embeddeddolt dir makes it a project", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "embeddeddolt"), 0755); err != nil {
			t.Fatal(err)
		}
		if !hasBeadsProjectFiles(dir) {
			t.Error("expected true with embeddeddolt dir")
		}
	})

	t.Run("backup db files ignored", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "beads.backup.db"), []byte(""), 0644); err != nil {
			t.Fatal(err)
		}
		if hasBeadsProjectFiles(dir) {
			t.Error("expected false for backup db files only")
		}
	})

	t.Run("vc.db ignored", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "vc.db"), []byte(""), 0644); err != nil {
			t.Fatal(err)
		}
		if hasBeadsProjectFiles(dir) {
			t.Error("expected false for vc.db only")
		}
	})
}

func TestHasBeadsDatabase(t *testing.T) {
	t.Run("metadata.json alone is not a database", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
		if hasBeadsDatabase(dir) {
			t.Error("expected false for metadata.json only")
		}
	})

	t.Run("dolt dir is a database", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "dolt"), 0755); err != nil {
			t.Fatal(err)
		}
		if !hasBeadsDatabase(dir) {
			t.Error("expected true with dolt dir")
		}
	})
}

func TestFollowRedirect(t *testing.T) {
	t.Run("no redirect returns original", func(t *testing.T) {
		dir := t.TempDir()
		result := followRedirect(dir)
		if result != dir {
			t.Errorf("expected %s, got %s", dir, result)
		}
	})

	t.Run("redirect to valid dir", func(t *testing.T) {
		sourceDir := t.TempDir()
		targetDir := t.TempDir()

		redirectFile := filepath.Join(sourceDir, "redirect")
		if err := os.WriteFile(redirectFile, []byte(targetDir), 0644); err != nil {
			t.Fatal(err)
		}

		result := followRedirect(sourceDir)
		if result != targetDir {
			t.Errorf("expected %s, got %s", targetDir, result)
		}
	})

	t.Run("redirect to nonexistent returns original", func(t *testing.T) {
		dir := t.TempDir()
		redirectFile := filepath.Join(dir, "redirect")
		if err := os.WriteFile(redirectFile, []byte("/nonexistent/path"), 0644); err != nil {
			t.Fatal(err)
		}

		result := followRedirect(dir)
		if result != dir {
			t.Errorf("expected %s, got %s", dir, result)
		}
	})

	t.Run("comments and empty lines skipped", func(t *testing.T) {
		sourceDir := t.TempDir()
		targetDir := t.TempDir()

		content := "# This is a comment\n\n" + targetDir + "\n"
		redirectFile := filepath.Join(sourceDir, "redirect")
		if err := os.WriteFile(redirectFile, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		result := followRedirect(sourceDir)
		if result != targetDir {
			t.Errorf("expected %s, got %s", targetDir, result)
		}
	})

	t.Run("redirect chains not followed", func(t *testing.T) {
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		dir3 := t.TempDir()

		// dir1 -> dir2 -> dir3 (chain)
		if err := os.WriteFile(filepath.Join(dir1, "redirect"), []byte(dir2), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir2, "redirect"), []byte(dir3), 0644); err != nil {
			t.Fatal(err)
		}

		result := followRedirect(dir1)
		// Should stop at dir2, not follow to dir3
		if result != dir2 {
			t.Errorf("expected %s (no chain following), got %s", dir2, result)
		}
	})
}

func TestPortSourceString(t *testing.T) {
	tests := []struct {
		src  PortSource
		want string
	}{
		{PortSourceNone, "none (ephemeral)"},
		{PortSourceEnvVar, "BEADS_DOLT_SERVER_PORT env var"},
		{PortSourcePortFile, "dolt-server.port file"},
		{PortSourceConfigYAML, "config.yaml dolt.port"},
		{PortSourceMetadataJSON, "metadata.json dolt_server_port (deprecated)"},
		{PortSourceSharedDefault, "shared-server default (3308)"},
	}
	for _, tt := range tests {
		if got := tt.src.String(); got != tt.want {
			t.Errorf("PortSource(%d).String() = %q, want %q", tt.src, got, tt.want)
		}
	}
}

func TestWorktreeResolutionString(t *testing.T) {
	tests := []struct {
		wr   WorktreeResolution
		want string
	}{
		{WorktreeNotApplicable, "not-applicable"},
		{WorktreeOwnDatabase, "own-database"},
		{WorktreeRedirect, "redirect"},
		{WorktreeFallbackShared, "fallback-shared"},
	}
	for _, tt := range tests {
		if got := tt.wr.String(); got != tt.want {
			t.Errorf("WorktreeResolution(%d).String() = %q, want %q", tt.wr, got, tt.want)
		}
	}
}

func TestIsPathInSafeBoundary(t *testing.T) {
	t.Run("home dir is safe", func(t *testing.T) {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skip("cannot determine home dir")
		}
		if !isPathInSafeBoundary(filepath.Join(home, "projects", "test")) {
			t.Error("expected home dir subdirectory to be safe")
		}
	})

	t.Run("system dirs are unsafe", func(t *testing.T) {
		unsafePaths := []string{"/etc/beads", "/usr/local/beads", "/root/beads"}
		for _, p := range unsafePaths {
			if isPathInSafeBoundary(p) {
				t.Errorf("expected %s to be unsafe", p)
			}
		}
	})

	t.Run("temp dir is safe", func(t *testing.T) {
		tmpDir := os.TempDir()
		if !isPathInSafeBoundary(filepath.Join(tmpDir, "test-beads")) {
			t.Error("expected temp dir to be safe")
		}
	})
}

func TestResolveForBeadsDir(t *testing.T) {
	t.Run("empty path returns error", func(t *testing.T) {
		_, err := ResolveForBeadsDir("")
		if err == nil {
			t.Error("expected error for empty path")
		}
	})

	t.Run("nonexistent path returns error", func(t *testing.T) {
		_, err := ResolveForBeadsDir("/nonexistent/path/.beads")
		if err == nil {
			t.Error("expected error for nonexistent path")
		}
	})
}
