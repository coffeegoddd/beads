package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"gopkg.in/yaml.v3"

	"github.com/steveyegge/beads/internal/config"
	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/dbproxy/proxy"
)

const (
	proxiedServerRootName   = "proxieddb"
	proxiedServerConfigName = "server_config.yaml"
	proxiedServerLogName    = "server.log"
)

func proxiedServerRoot(beadsDir string) string {
	return filepath.Join(beadsDir, proxiedServerRootName)
}

func proxiedServerConfigPath(beadsDir string) string {
	return filepath.Join(proxiedServerRoot(beadsDir), proxiedServerConfigName)
}

func proxiedServerLogPath(beadsDir string) string {
	return filepath.Join(proxiedServerRoot(beadsDir), proxiedServerLogName)
}

func resolveProxiedServerRootPath(beadsDir string, cfg *configfile.Config) string {
	if cfg == nil {
		cfg = &configfile.Config{}
	}
	if custom := cfg.GetDoltProxiedServerRootPath(beadsDir); custom != "" {
		return custom
	}
	return proxiedServerRoot(beadsDir)
}

func resolveProxiedServerConfigPath(beadsDir string, cfg *configfile.Config) (path string, isCustom bool) {
	if cfg == nil {
		cfg = &configfile.Config{}
	}
	if custom := cfg.GetDoltProxiedServerConfig(beadsDir); custom != "" {
		return custom, true
	}
	root := resolveProxiedServerRootPath(beadsDir, cfg)
	return filepath.Join(root, proxiedServerConfigName), false
}

func resolveProxiedServerLogPath(beadsDir string, cfg *configfile.Config) (path string, isCustom bool) {
	if cfg == nil {
		cfg = &configfile.Config{}
	}
	if custom := cfg.GetDoltProxiedServerLog(beadsDir); custom != "" {
		return custom, true
	}
	root := resolveProxiedServerRootPath(beadsDir, cfg)
	return filepath.Join(root, proxiedServerLogName), false
}

func ensureProxiedServerConfig(beadsDir string, cfg *configfile.Config) (string, error) {
	path, isCustom := resolveProxiedServerConfigPath(beadsDir, cfg)

	if isCustom {
		info, err := os.Stat(path)
		if err != nil {
			return "", fmt.Errorf("ensureProxiedServerConfig: custom config %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("ensureProxiedServerConfig: custom config %s: not a regular file", path)
		}
		if _, err := servercfg.YamlConfigFromFile(filesys.LocalFS, path); err != nil {
			return "", fmt.Errorf("ensureProxiedServerConfig: custom config %s: parse: %w", path, err)
		}
		return path, nil
	}

	root := filepath.Dir(path)
	if err := os.MkdirAll(root, config.BeadsDirPerm); err != nil {
		return "", fmt.Errorf("ensureProxiedServerConfig: mkdir %s: %w", root, err)
	}

	switch _, err := os.Stat(path); {
	case err == nil:
		return path, nil
	case !os.IsNotExist(err):
		return "", fmt.Errorf("ensureProxiedServerConfig: stat %s: %w", path, err)
	}

	port, err := proxy.PickFreePort()
	if err != nil {
		return "", fmt.Errorf("ensureProxiedServerConfig: pick free port: %w", err)
	}

	body, err := renderProxiedServerConfig(port)
	if err != nil {
		return "", fmt.Errorf("ensureProxiedServerConfig: render YAML: %w", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return "", fmt.Errorf("ensureProxiedServerConfig: write %s: %w", path, err)
	}
	return path, nil
}

func validateProxiedServerConfig(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("--proxied-server-config %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("--proxied-server-config %s: not a regular file", path)
	}
	if _, err := servercfg.YamlConfigFromFile(filesys.LocalFS, path); err != nil {
		return fmt.Errorf("--proxied-server-config %s: parse: %w", path, err)
	}
	return nil
}

func validateProxiedServerRootPath(path string) error {
	switch info, err := os.Stat(path); {
	case err == nil:
		if !info.IsDir() {
			return fmt.Errorf("--proxied-server-root-path %s: not a directory", path)
		}
	case !os.IsNotExist(err):
		return fmt.Errorf("--proxied-server-root-path %s: %w", path, err)
	}
	return nil
}

func validateProxiedServerLogPath(path string) error {
	parent := filepath.Dir(path)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("--proxied-server-log-path %s: parent directory: %w", path, err)
	}
	if !parentInfo.IsDir() {
		return fmt.Errorf("--proxied-server-log-path %s: parent %s is not a directory", path, parent)
	}
	switch info, err := os.Stat(path); {
	case err == nil:
		if !info.Mode().IsRegular() {
			return fmt.Errorf("--proxied-server-log-path %s: not a regular file", path)
		}
	case !os.IsNotExist(err):
		return fmt.Errorf("--proxied-server-log-path %s: %w", path, err)
	}
	return nil
}

func renderProxiedServerConfig(port int) ([]byte, error) {
	host := proxiedServerListenerHost
	logLevel := string(servercfg.LogLevel_Info)
	yc := &servercfg.YAMLConfig{
		LogLevelStr: &logLevel,
		ListenerConfig: servercfg.ListenerYAMLConfig{
			HostStr:    &host,
			PortNumber: &port,
		},
	}
	return yaml.Marshal(yc)
}

const proxiedServerListenerHost = "127.0.0.1"
