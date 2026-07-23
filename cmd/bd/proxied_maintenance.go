package main

import (
	"context"
	"database/sql"

	"github.com/steveyegge/beads/internal/storage/uow"
)

func runProxiedNonTx(ctx context.Context, fn func(ctx context.Context, conn *sql.Conn) error) error {
	if uowProvider == nil {
		return HandleError("proxied-server UOW provider not initialized")
	}
	mp, ok := uowProvider.(uow.MaintenanceProvider)
	if !ok {
		return HandleError("proxied-server provider does not support maintenance operations")
	}
	return mp.RunNonTx(ctx, fn)
}
