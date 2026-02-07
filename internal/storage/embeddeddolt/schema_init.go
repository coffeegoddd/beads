package embeddeddolt

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// InitSchemaOnDB initializes the embedded-dolt schema on the provided database.
//
// This intentionally does NOT create any JSONL/flush/dirty tracking tables.
// It also proactively drops those tables if they exist, to enforce the forward-only
// embedded-dolt direction.
func InitSchemaOnDB(ctx context.Context, db *sql.DB) error {
	// Forward-only enforcement: drop removed tables if they exist.
	drops := []string{
		"DROP TABLE IF EXISTS dirty_issues",
		"DROP TABLE IF EXISTS export_hashes",
		"DROP TABLE IF EXISTS repo_mtimes",
	}
	for _, stmt := range drops {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to drop removed table: %w\nStatement: %s", err, stmt)
		}
	}

	// Create tables
	for _, stmt := range splitStatements(schema) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || isOnlyComments(stmt) {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create schema: %w\nStatement: %s", err, truncateForError(stmt))
		}
	}

	// Insert default config values
	for _, stmt := range splitStatements(defaultConfig) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || isOnlyComments(stmt) {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to insert default config: %w", err)
		}
	}

	// Apply index migrations for existing databases.
	// CREATE TABLE IF NOT EXISTS won't add new indexes to existing tables.
	indexMigrations := []string{
		"CREATE INDEX idx_issues_issue_type ON issues(issue_type)",
	}
	for _, migration := range indexMigrations {
		_, err := db.ExecContext(ctx, migration)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate") &&
			!strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("failed to apply index migration: %w", err)
		}
	}

	// Remove FK constraint on depends_on_id to allow external references.
	// This is idempotent - DROP FOREIGN KEY fails if constraint doesn't exist.
	_, err := db.ExecContext(ctx, "ALTER TABLE dependencies DROP FOREIGN KEY fk_dep_depends_on")
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "can't drop") &&
		!strings.Contains(strings.ToLower(err.Error()), "doesn't exist") &&
		!strings.Contains(strings.ToLower(err.Error()), "check that it exists") &&
		!strings.Contains(strings.ToLower(err.Error()), "was not found") {
		return fmt.Errorf("failed to drop fk_dep_depends_on: %w", err)
	}

	// Create views
	if _, err := db.ExecContext(ctx, readyIssuesView); err != nil {
		return fmt.Errorf("failed to create ready_issues view: %w", err)
	}
	if _, err := db.ExecContext(ctx, blockedIssuesView); err != nil {
		return fmt.Errorf("failed to create blocked_issues view: %w", err)
	}

	return nil
}

// splitStatements splits a SQL script into individual statements.
func splitStatements(script string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(script); i++ {
		c := script[i]

		if inString {
			current.WriteByte(c)
			if c == stringChar && (i == 0 || script[i-1] != '\\') {
				inString = false
			}
			continue
		}

		if c == '\'' || c == '"' || c == '`' {
			inString = true
			stringChar = c
			current.WriteByte(c)
			continue
		}

		if c == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// Handle last statement without semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// truncateForError truncates a string for use in error messages.
func truncateForError(s string) string {
	if len(s) > 100 {
		return s[:100] + "..."
	}
	return s
}

// isOnlyComments returns true if the statement contains only SQL comments.
func isOnlyComments(stmt string) bool {
	lines := strings.Split(stmt, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		return false
	}
	return true
}
