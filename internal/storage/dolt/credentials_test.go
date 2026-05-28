package dolt

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
)

func TestEncryptDecryptWithKey(t *testing.T) {
	// Generate a random key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	password := "super-secret-federation-password"

	encrypted, err := encryptWithKey(password, key)
	if err != nil {
		t.Fatalf("encryptWithKey failed: %v", err)
	}
	if len(encrypted) == 0 {
		t.Fatal("encrypted result is empty")
	}

	decrypted, err := decryptWithKey(encrypted, key)
	if err != nil {
		t.Fatalf("decryptWithKey failed: %v", err)
	}
	if decrypted != password {
		t.Errorf("decrypted = %q, want %q", decrypted, password)
	}
}

func TestWithRemoteOperationEnvRestoresS3ChecksumEnv(t *testing.T) {
	t.Setenv(awsResponseChecksumValidationEnv, "when_supported")

	err := withRemoteOperationEnv(nil, true, func() error {
		if got := os.Getenv(awsResponseChecksumValidationEnv); got != "when_required" {
			t.Fatalf("%s during operation = %q, want when_required", awsResponseChecksumValidationEnv, got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withRemoteOperationEnv returned error: %v", err)
	}
	if got := os.Getenv(awsResponseChecksumValidationEnv); got != "when_supported" {
		t.Fatalf("%s after operation = %q, want restored when_supported", awsResponseChecksumValidationEnv, got)
	}
}

func TestWithRemoteOperationEnvUnsetsS3ChecksumEnv(t *testing.T) {
	t.Setenv(awsResponseChecksumValidationEnv, "")
	if err := os.Unsetenv(awsResponseChecksumValidationEnv); err != nil {
		t.Fatalf("unset %s: %v", awsResponseChecksumValidationEnv, err)
	}

	err := withRemoteOperationEnv(nil, true, func() error {
		if got := os.Getenv(awsResponseChecksumValidationEnv); got != "when_required" {
			t.Fatalf("%s during operation = %q, want when_required", awsResponseChecksumValidationEnv, got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withRemoteOperationEnv returned error: %v", err)
	}
	if _, ok := os.LookupEnv(awsResponseChecksumValidationEnv); ok {
		t.Fatalf("%s should be unset after operation", awsResponseChecksumValidationEnv)
	}
}

func TestIsS3RemoteURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want bool
	}{
		{name: "dolt aws", url: "aws://[table:bucket]/db", want: true},
		{name: "s3", url: "s3://bucket/path", want: true},
		{name: "gcs", url: "gs://bucket/path", want: false},
		{name: "azure", url: "az://account.blob.core.windows.net/container", want: false},
		{name: "empty", url: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isS3RemoteURL(tt.url); got != tt.want {
				t.Fatalf("isS3RemoteURL(%q) = %v, want %v", tt.url, got, tt.want)
			}
		})
	}
}

func TestEncryptDecryptWithWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	for i := range key1 {
		key1[i] = byte(i)
		key2[i] = byte(i + 1)
	}

	encrypted, err := encryptWithKey("password", key1)
	if err != nil {
		t.Fatalf("encryptWithKey failed: %v", err)
	}

	_, err = decryptWithKey(encrypted, key2)
	if err == nil {
		t.Fatal("expected decryption with wrong key to fail")
	}
}

func TestCredentialKeyFileGeneration(t *testing.T) {
	tmpDir := t.TempDir()

	store := &DoltStore{dbPath: tmpDir, beadsDir: tmpDir}

	// Key file should not exist yet
	keyPath := filepath.Join(tmpDir, credentialKeyFile)
	if _, err := os.Stat(keyPath); err == nil {
		t.Fatal("key file should not exist before initCredentialKey")
	}

	// initCredentialKey should generate and save a key
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey failed: %v", err)
	}

	// Key should be set on the store
	if len(store.credentialKey) != 32 {
		t.Fatalf("credentialKey length = %d, want 32", len(store.credentialKey))
	}

	// Key file should exist with restrictive permissions
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file should exist after initCredentialKey: %v", err)
	}
	if runtime.GOOS == "windows" {
		t.Log("skipping POSIX mode-bit check on Windows")
	} else if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("key file permissions = %o, want 0600", perm)
	}

	// Reading the key file should return the same key
	savedKey, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("failed to read key file: %v", err)
	}
	if string(savedKey) != string(store.credentialKey) {
		t.Error("saved key does not match store key")
	}
}

func TestCredentialKeyFileReload(t *testing.T) {
	tmpDir := t.TempDir()

	// First store generates the key
	store1 := &DoltStore{dbPath: tmpDir, beadsDir: tmpDir}
	if err := store1.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey (store1) failed: %v", err)
	}

	// Second store should load the same key from file
	store2 := &DoltStore{dbPath: tmpDir, beadsDir: tmpDir}
	if err := store2.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey (store2) failed: %v", err)
	}

	if string(store1.credentialKey) != string(store2.credentialKey) {
		t.Error("second store loaded different key than first store generated")
	}
}

func TestCredentialKeyNotPredictable(t *testing.T) {
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	store1 := &DoltStore{dbPath: tmpDir1, beadsDir: tmpDir1}
	if err := store1.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey (store1) failed: %v", err)
	}

	store2 := &DoltStore{dbPath: tmpDir2, beadsDir: tmpDir2}
	if err := store2.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey (store2) failed: %v", err)
	}

	// Two different installations should generate different keys
	if string(store1.credentialKey) == string(store2.credentialKey) {
		t.Error("different installations generated identical keys — key is not random")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	store := &DoltStore{dbPath: tmpDir, beadsDir: tmpDir}
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey failed: %v", err)
	}

	password := "my-federation-password-123!"

	encrypted, err := store.encryptPassword(password)
	if err != nil {
		t.Fatalf("encryptPassword failed: %v", err)
	}

	decrypted, err := store.decryptPassword(encrypted)
	if err != nil {
		t.Fatalf("decryptPassword failed: %v", err)
	}

	if decrypted != password {
		t.Errorf("round-trip failed: got %q, want %q", decrypted, password)
	}
}

func TestEncryptPasswordEmpty(t *testing.T) {
	store := &DoltStore{}

	encrypted, err := store.encryptPassword("")
	if err != nil {
		t.Fatalf("encryptPassword with empty string failed: %v", err)
	}
	if encrypted != nil {
		t.Error("expected nil for empty password encryption")
	}
}

func TestDecryptPasswordEmpty(t *testing.T) {
	store := &DoltStore{}

	decrypted, err := store.decryptPassword(nil)
	if err != nil {
		t.Fatalf("decryptPassword with nil failed: %v", err)
	}
	if decrypted != "" {
		t.Errorf("expected empty string, got %q", decrypted)
	}
}

func TestEncryptPasswordNoKey(t *testing.T) {
	store := &DoltStore{}

	_, err := store.encryptPassword("password")
	if err == nil {
		t.Fatal("expected error when key is not initialized")
	}
}

func TestInitCredentialKeyEmptyDbPath(t *testing.T) {
	store := &DoltStore{dbPath: ""}
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey with empty dbPath should not fail: %v", err)
	}
	if store.credentialKey != nil {
		t.Error("expected nil key when dbPath is empty")
	}
}

func TestEnsureCredentialKeyAlreadyInitialized(t *testing.T) {
	tmpDir := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	store := &DoltStore{
		dbPath:        filepath.Join(tmpDir, "dolt"),
		beadsDir:      tmpDir,
		credentialKey: append([]byte(nil), key...),
	}

	if err := store.ensureCredentialKey(t.Context()); err != nil {
		t.Fatalf("ensureCredentialKey() error = %v", err)
	}
	if string(store.credentialKey) != string(key) {
		t.Fatalf("credentialKey changed unexpectedly: got %q want %q", string(store.credentialKey), string(key))
	}
	if _, err := os.Stat(filepath.Join(tmpDir, credentialKeyFile)); !os.IsNotExist(err) {
		t.Fatalf("expected no key file write when key already initialized, got err=%v", err)
	}
}

func TestEnsureCredentialKeyInitializesWhenMissing(t *testing.T) {
	tmpDir := t.TempDir()
	store := &DoltStore{
		dbPath:   filepath.Join(tmpDir, "dolt"),
		beadsDir: tmpDir,
	}

	if err := store.ensureCredentialKey(t.Context()); err != nil {
		t.Fatalf("ensureCredentialKey() error = %v", err)
	}
	if len(store.credentialKey) != 32 {
		t.Fatalf("credentialKey length = %d, want 32", len(store.credentialKey))
	}
	if _, err := os.Stat(filepath.Join(tmpDir, credentialKeyFile)); err != nil {
		t.Fatalf("expected key file after lazy init: %v", err)
	}
}

func TestAddFederationPeerReturnsCredentialKeyInitError(t *testing.T) {
	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create parent file: %v", err)
	}

	store := &DoltStore{
		dbPath:   filepath.Join(parentFile, "dolt"),
		beadsDir: filepath.Join(parentFile, ".beads"),
	}

	err := store.AddFederationPeer(t.Context(), &storage.FederationPeer{
		Name:        "peerone",
		RemoteURL:   "file:///tmp/nonexistent-peer",
		Password:    "secret",
		Sovereignty: "T2",
	})
	if err == nil {
		t.Fatal("expected credential key initialization error")
	}
	if !strings.Contains(err.Error(), "failed to initialize credential key") {
		t.Fatalf("expected credential key initialization error, got: %v", err)
	}
}

func TestDecryptWithKeyShortCiphertext(t *testing.T) {
	key := make([]byte, 32)
	if _, err := decryptWithKey([]byte("short"), key); err == nil || err.Error() != "ciphertext too short" {
		t.Fatalf("decryptWithKey(short) error = %v, want ciphertext too short", err)
	}
}

func TestValidatePeerName(t *testing.T) {
	tests := []struct {
		name    string
		peer    string
		wantErr string
	}{
		{name: "valid", peer: "peer_one-2"},
		{name: "empty", peer: "", wantErr: "peer name cannot be empty"},
		{name: "must start with letter", peer: "1peer", wantErr: "peer name must start with a letter and contain only alphanumeric characters, hyphens, and underscores"},
		{name: "invalid character", peer: "peer.one", wantErr: "peer name must start with a letter and contain only alphanumeric characters, hyphens, and underscores"},
		{name: "too long", peer: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_extra", wantErr: "peer name too long (max 64 characters)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePeerName(tt.peer)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validatePeerName(%q) unexpected error: %v", tt.peer, err)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("validatePeerName(%q) error = %v, want %q", tt.peer, err, tt.wantErr)
			}
		})
	}
}

func TestCredentialKeyMigrationFromDbPath(t *testing.T) {
	// Simulate old layout: key file in .beads/dolt/ (dbPath)
	beadsDir := t.TempDir()
	dbPath := filepath.Join(beadsDir, "dolt")
	if err := os.MkdirAll(dbPath, 0700); err != nil {
		t.Fatal(err)
	}

	// Write key to old location
	oldKey := make([]byte, 32)
	for i := range oldKey {
		oldKey[i] = byte(i)
	}
	oldKeyPath := filepath.Join(dbPath, credentialKeyFile)
	if err := os.WriteFile(oldKeyPath, oldKey, 0600); err != nil {
		t.Fatal(err)
	}

	store := &DoltStore{dbPath: dbPath, beadsDir: beadsDir}
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey failed: %v", err)
	}

	// Key should be loaded from old location
	if string(store.credentialKey) != string(oldKey) {
		t.Error("migrated key does not match original")
	}

	// New location should now have the key
	newKeyPath := filepath.Join(beadsDir, credentialKeyFile)
	newKey, err := os.ReadFile(newKeyPath)
	if err != nil {
		t.Fatalf("key file should exist at new location: %v", err)
	}
	if string(newKey) != string(oldKey) {
		t.Error("key at new location does not match original")
	}

	// Old location should be cleaned up
	if _, err := os.Stat(oldKeyPath); err == nil {
		t.Error("old key file should have been removed after migration")
	}
}

func TestCredentialKeyNoGhostDir(t *testing.T) {
	// In shared-server mode, dbPath (.beads/dolt/) should NOT be created
	beadsDir := t.TempDir()
	dbPath := filepath.Join(beadsDir, "dolt") // does not exist

	store := &DoltStore{dbPath: dbPath, beadsDir: beadsDir}
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey failed: %v", err)
	}

	// The key should be written to beadsDir, not dbPath
	if _, err := os.Stat(filepath.Join(beadsDir, credentialKeyFile)); err != nil {
		t.Error("key file should exist in beadsDir")
	}

	// dbPath directory should NOT have been created
	if _, err := os.Stat(dbPath); err == nil {
		t.Error("dbPath directory should not be created in shared-server mode — ghost directory bug")
	}
}

// TestCredentialKeyCreatesBeadsDir verifies that initCredentialKey creates the
// .beads/ directory if it doesn't exist. This is needed for external server
// setups where bd connects to a pre-existing dolt server without bd init (GH#2641).
func TestCredentialKeyCreatesBeadsDir(t *testing.T) {
	parentDir := t.TempDir()
	beadsDir := filepath.Join(parentDir, ".beads") // does not exist yet

	store := &DoltStore{dbPath: "", beadsDir: beadsDir}
	if err := store.initCredentialKey(t.Context()); err != nil {
		t.Fatalf("initCredentialKey failed when beadsDir doesn't exist: %v", err)
	}

	// Key should be generated
	if len(store.credentialKey) != 32 {
		t.Fatalf("credentialKey length = %d, want 32", len(store.credentialKey))
	}

	// .beads/ directory should have been created
	info, err := os.Stat(beadsDir)
	if err != nil {
		t.Fatalf(".beads/ directory should have been created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal(".beads/ should be a directory")
	}

	// Key file should exist in the newly created directory
	keyPath := filepath.Join(beadsDir, credentialKeyFile)
	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("key file should exist in newly created .beads/: %v", err)
	}
}

func TestFederationPeerCredentialLifecycleLazyKeyInit(t *testing.T) {
	skipIfNoServer(t)

	ctx := context.Background()
	baseDir := t.TempDir()
	beadsDir := filepath.Join(baseDir, ".beads")
	dbName := fmt.Sprintf("test_federation_credentials_%d", testServerPort)

	assertDatabaseNotExists(t, testServerPort, dbName)
	t.Cleanup(func() { dropTestDatabase(t, testServerPort, dbName) })

	store, err := New(ctx, &Config{
		Path:            filepath.Join(beadsDir, "dolt"),
		BeadsDir:        beadsDir,
		ServerHost:      "127.0.0.1",
		ServerPort:      testServerPort,
		Database:        dbName,
		MaxOpenConns:    1,
		CreateIfMissing: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer store.Close()

	peer := &storage.FederationPeer{
		Name:        "peerone",
		RemoteURL:   "file:///tmp/nonexistent-peer",
		Username:    "alice",
		Password:    "s3cret",
		Sovereignty: "T2",
	}

	if err := store.AddFederationPeer(ctx, peer); err != nil {
		t.Fatalf("AddFederationPeer() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(beadsDir, credentialKeyFile)); err != nil {
		t.Fatalf("expected credential key file after adding peer with password: %v", err)
	}

	store.credentialKey = nil
	got, err := store.GetFederationPeer(ctx, peer.Name)
	if err != nil {
		t.Fatalf("GetFederationPeer() error = %v", err)
	}
	if got.Password != peer.Password {
		t.Fatalf("GetFederationPeer().Password = %q, want %q", got.Password, peer.Password)
	}

	store.credentialKey = nil
	peers, err := store.ListFederationPeers(ctx)
	if err != nil {
		t.Fatalf("ListFederationPeers() error = %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("ListFederationPeers() length = %d, want 1", len(peers))
	}
	if peers[0].Password != peer.Password {
		t.Fatalf("ListFederationPeers()[0].Password = %q, want %q", peers[0].Password, peer.Password)
	}
}
