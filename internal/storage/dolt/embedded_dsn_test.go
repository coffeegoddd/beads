package dolt

import (
	"net/url"
	"strings"
	"testing"
)

func parseEmbeddedDSN(t *testing.T, dsn string) (dir string, params url.Values) {
	t.Helper()
	const prefix = "file://"
	if !strings.HasPrefix(dsn, prefix) {
		t.Fatalf("expected DSN to start with %q, got %q", prefix, dsn)
	}

	rest := strings.TrimPrefix(dsn, prefix)
	qIdx := strings.IndexByte(rest, '?')
	if qIdx < 0 {
		return rest, url.Values{}
	}
	dir = rest[:qIdx]
	rawQuery := rest[qIdx+1:]
	v, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatalf("failed to parse DSN query: %v (dsn=%q)", err, dsn)
	}
	return dir, v
}

func TestEmbeddedDSN_DefaultOpenParams_Applied(t *testing.T) {
	cfg := &Config{
		// Intentionally include spaces to ensure the directory portion remains raw/unescaped.
		Path:           "/tmp/beads dolt dbs",
		CommitterName:  "Alice Example",
		CommitterEmail: "alice+beads@example.com",
		Database:       "beads",
	}

	initDSN := embeddedInitDSN(cfg, embeddedDefaultOpenParams())
	mainDSN := embeddedDBDSN(cfg, embeddedDefaultOpenParams())

	initDir, initQ := parseEmbeddedDSN(t, initDSN)
	mainDir, mainQ := parseEmbeddedDSN(t, mainDSN)

	if initDir != cfg.Path {
		t.Fatalf("init DSN dir mismatch: got %q want %q", initDir, cfg.Path)
	}
	if mainDir != cfg.Path {
		t.Fatalf("main DSN dir mismatch: got %q want %q", mainDir, cfg.Path)
	}

	// Base params
	if got := initQ.Get("commitname"); got != cfg.CommitterName {
		t.Fatalf("init commitname mismatch: got %q want %q", got, cfg.CommitterName)
	}
	if got := initQ.Get("commitemail"); got != cfg.CommitterEmail {
		t.Fatalf("init commitemail mismatch: got %q want %q", got, cfg.CommitterEmail)
	}
	if got := mainQ.Get("commitname"); got != cfg.CommitterName {
		t.Fatalf("main commitname mismatch: got %q want %q", got, cfg.CommitterName)
	}
	if got := mainQ.Get("commitemail"); got != cfg.CommitterEmail {
		t.Fatalf("main commitemail mismatch: got %q want %q", got, cfg.CommitterEmail)
	}

	// Init DSN must not select a database.
	if got := initQ.Get("database"); got != "" {
		t.Fatalf("init DSN should not include database param, got %q", got)
	}
	// Main DSN must select the database.
	if got := mainQ.Get("database"); got != cfg.Database {
		t.Fatalf("main database mismatch: got %q want %q", got, cfg.Database)
	}

	// Default tuning params (embedded only)
	for _, q := range []url.Values{initQ, mainQ} {
		if got := q.Get("nocache"); got != "true" {
			t.Fatalf("expected nocache=true, got %q", got)
		}
		if got := q.Get("failonlocktimeout"); got != "true" {
			t.Fatalf("expected failonlocktimeout=true, got %q", got)
		}
	}

	// Default retry params (embedded only)
	for _, q := range []url.Values{initQ, mainQ} {
		if got := q.Get("retry"); got != "true" {
			t.Fatalf("expected retry=true, got %q", got)
		}
		if got := q.Get("retrytimeout"); got != "2s" {
			t.Fatalf("expected retrytimeout=2s, got %q", got)
		}
		if got := q.Get("retrymaxattempts"); got != "200" {
			t.Fatalf("expected retrymaxattempts=200, got %q", got)
		}
		if got := q.Get("retryinitialdelay"); got != "10ms" {
			t.Fatalf("expected retryinitialdelay=10ms, got %q", got)
		}
		if got := q.Get("retrymaxdelay"); got != "100ms" {
			t.Fatalf("expected retrymaxdelay=100ms, got %q", got)
		}
	}
}

