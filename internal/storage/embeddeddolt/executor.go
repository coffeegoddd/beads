package embeddeddolt

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	doltembed "github.com/dolthub/driver"
)

func buildDSN(dir string, database string) string {
	// DSN format expected by github.com/dolthub/driver:
	// file:///path/to/multi-db-dir?commitname=...&commitemail=...&database=...
	v := url.Values{}
	v.Set(doltembed.CommitNameParam, "beads")
	v.Set(doltembed.CommitEmailParam, "beads@local")
	// We keep multi-statement enabled for parity with examples; we still Exec statements individually.
	v.Set(doltembed.MultiStatementsParam, "true")
	if database != "" {
		v.Set(doltembed.DatabaseParam, database)
	}
	prefix := "file://"
	pathPart := dir
	if filepath.IsAbs(dir) {
		// ParseDataSource strips only the "file://" prefix, so for absolute paths we
		// want the remainder to begin with a single leading slash.
		prefix = "file:///"
		pathPart = strings.TrimPrefix(dir, "/")
	}
	return prefix + pathPart + "?" + v.Encode()
}

// Executor is responsible for on-demand construction of:
// - a dolt embedded Connector
// - a database/sql *sql.DB bound to that connector
//
// ...followed by SQL execution, followed by immediate Close of both.
//
// This ensures EmbeddedDoltStore does not hold long-lived process-global state.
type Executor struct {
	multiDir string // directory containing one or more dolt databases (subdirs)
}

func NewExecutor(multiDir string) *Executor {
	return &Executor{multiDir: multiDir}
}

// withDB opens a short-lived *sql.DB for the given database, runs fn, then closes
// both the sql.DB and the underlying connector.
//
// NOTE: callers MUST NOT retain the *sql.DB outside fn.
func (e *Executor) withDB(ctx context.Context, database string, fn func(db *sql.DB) error) error {
	if e == nil || e.multiDir == "" {
		return fmt.Errorf("embedded dolt executor is not configured")
	}
	dsn := buildDSN(e.multiDir, database)

	// Parse and build a connector (this also validates the directory exists).
	cfg, err := doltembed.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse embedded dolt DSN: %w", err)
	}
	// Ensure database selection matches the callsite, even if DSN was built without it.
	cfg.Database = database

	connector, err := doltembed.NewConnector(cfg)
	if err != nil {
		return fmt.Errorf("failed to create embedded dolt connector: %w", err)
	}
	db := sql.OpenDB(connector)

	// Always close resources immediately after use.
	defer func() { _ = db.Close() }()
	defer func() { _ = connector.Close() }()

	// Ensure engine is open and usable before running operations.
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to open embedded dolt database: %w", err)
	}

	return fn(db)
}

// ExecContext opens a short-lived connection, executes a statement, and closes immediately.
func (e *Executor) ExecContext(ctx context.Context, database string, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := e.withDB(ctx, database, func(db *sql.DB) error {
		r, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		result = r
		return nil
	})
	return result, err
}

// QueryContext opens a short-lived connection, runs a query, fully materializes results,
// and closes immediately.
//
// This returns a slice of rows keyed by column name, since returning *sql.Rows would
// require keeping the underlying connection open.
func (e *Executor) QueryContext(ctx context.Context, database string, query string, args ...any) ([]map[string]any, error) {
	var out []map[string]any
	err := e.withDB(ctx, database, func(db *sql.DB) error {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				v := vals[i]
				// Normalize common DB return type for text columns.
				if b, ok := v.([]byte); ok {
					row[c] = string(b)
				} else {
					row[c] = v
				}
			}
			out = append(out, row)
		}

		return rows.Err()
	})
	return out, err
}

