package versioncontrolops

import (
	"context"
	"fmt"
)

func DoltGC(ctx context.Context, conn DBConn) error {
	if _, err := conn.ExecContext(ctx, "CALL DOLT_GC()"); err != nil {
		return fmt.Errorf("dolt gc: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_STATS_GC()"); err != nil {
		return fmt.Errorf("dolt status gc: %w", err)
	}
	return nil
}
