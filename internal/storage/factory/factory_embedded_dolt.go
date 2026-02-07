package factory

import (
	"context"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/embeddeddolt"
)

func init() {
	RegisterBackend(configfile.BackendEmbeddedDolt, func(ctx context.Context, path string, opts Options) (storage.Storage, error) {
		// Embedded Dolt placeholder backend uses open-or-create semantics for now.
		// `bd init` also calls Create() to lay down a deterministic skeleton, but we keep
		// New() here to match other backends' "construct or open" behavior.
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
