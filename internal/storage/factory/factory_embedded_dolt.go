package factory

import (
	"context"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
)

func init() {
	RegisterBackend(configfile.BackendEmbeddedDolt, func(ctx context.Context, path string, opts Options) (storage.Storage, error) {
		store, err := embeddeddolt.New(ctx, &embeddeddolt.Config{
			Path:     path,
			ReadOnly: opts.ReadOnly,
		})
		if err != nil {
			return nil, err
		}
		return store, nil
	})
}

