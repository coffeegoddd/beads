package main

import (
	"path/filepath"
	"testing"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildProxiedServerClientInfo(t *testing.T) {
	t.Run("all empty returns nil", func(t *testing.T) {
		info, err := buildProxiedServerClientInfo("/some/.beads", "", "", "")
		require.NoError(t, err)
		assert.Nil(t, info)
	})

	t.Run("relative paths anchored to beadsDir", func(t *testing.T) {
		bd := "/proj/.beads"
		info, err := buildProxiedServerClientInfo(bd, "alt-root", "configs/server.yaml", "logs/server.log")
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, filepath.Join(bd, "alt-root"), info.RootPath)
		assert.Equal(t, filepath.Join(bd, "configs/server.yaml"), info.ConfigPath)
		assert.Equal(t, filepath.Join(bd, "logs/server.log"), info.LogPath)
	})

	t.Run("absolute paths pass through cleaned", func(t *testing.T) {
		info, err := buildProxiedServerClientInfo("/proj/.beads", "/var/lib/beads/proxieddb", "/etc/dolt/server.yaml", "/var/log/server.log")
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, "/var/lib/beads/proxieddb", info.RootPath)
		assert.Equal(t, "/etc/dolt/server.yaml", info.ConfigPath)
		assert.Equal(t, "/var/log/server.log", info.LogPath)
	})

	t.Run("mixed relative + absolute + empty", func(t *testing.T) {
		bd := "/proj/.beads"
		info, err := buildProxiedServerClientInfo(bd, "alt-root", "/etc/dolt/server.yaml", "")
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, filepath.Join(bd, "alt-root"), info.RootPath)
		assert.Equal(t, "/etc/dolt/server.yaml", info.ConfigPath)
		assert.Equal(t, "", info.LogPath)
	})

	t.Run("empty beadsDir with non-empty inputs errors", func(t *testing.T) {
		_, err := buildProxiedServerClientInfo("", "foo", "", "")
		require.Error(t, err)
	})

	t.Run("result is round-trip-compatible with sidecar resolver", func(t *testing.T) {
		bd := "/proj/.beads"
		info, err := buildProxiedServerClientInfo(bd, "alt-root", "", "")
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.Equal(t, info.RootPath, (&configfile.ProxiedServerClientInfo{RootPath: info.RootPath}).ResolvedRootPath(bd))
	})
}
