package domain

import (
	"context"
	"errors"
)

type BootstrapUseCase interface {
	// EnsureIssuePrefix sets the issue_prefix config row only if it is not
	// already set, and returns the value that ended up persisted. Mirrors
	// the read-then-set pattern at cmd/bd/init.go:853-862, which avoids
	// clobbering when multiple rigs share the same Dolt database.
	EnsureIssuePrefix(ctx context.Context, prefix string) (persisted string, err error)

	// RecordBDVersion writes bd_version to clone-local metadata
	// (dolt-ignored, no merge conflicts). Mirrors cmd/bd/init.go:870.
	RecordBDVersion(ctx context.Context, version string) error

	// RecordRepoID writes repo_id to metadata and reads it back to confirm
	// the write landed. Returns verified=true when the read-back matches.
	// Mirrors verifyMetadata(ctx, store, "repo_id", ...) at
	// cmd/bd/init.go:881.
	RecordRepoID(ctx context.Context, repoID string) (verified bool, err error)

	// RecordCloneID is the clone_id analog of RecordRepoID. Mirrors
	// verifyMetadata(ctx, store, "clone_id", ...) at cmd/bd/init.go:893.
	RecordCloneID(ctx context.Context, cloneID string) (verified bool, err error)

	// ReadProjectID returns the existing _project_id, or empty string if
	// none is set. Used during project-identity adoption when --database
	// targets a pre-existing database or init bootstrapped from a remote
	// whose Dolt history already carries a _project_id (cmd/bd/init.go:929).
	ReadProjectID(ctx context.Context) (string, error)

	// RecordProjectID writes _project_id to metadata for cross-project
	// verification (GH#2372). Mirrors cmd/bd/init.go:1016.
	RecordProjectID(ctx context.Context, projectID string) error

	// EnsureRemote registers a Dolt remote with the given name and URL
	// only when no remote with that name already exists. Mirrors the
	// HasRemote / AddRemote pair at cmd/bd/init.go:835-843.
	EnsureRemote(ctx context.Context, name, url string) error
}

// ErrUseCaseNotImplemented is returned by every method on the placeholder
// BootstrapUseCase / ConfigUseCase implementations. It exists so callers can
// distinguish "use case isn't wired up yet" from real runtime failures during
// the transitional period before domain use case implementations land.
var ErrUseCaseNotImplemented = errors.New("domain: use case not implemented yet (BootstrapUseCase / ConfigUseCase impls land in a follow-up)")

func NewBootstrapUseCase() BootstrapUseCase {
	return bootstrapUseCaseImpl{}
}

type bootstrapUseCaseImpl struct{}

var _ BootstrapUseCase = bootstrapUseCaseImpl{}

func (bootstrapUseCaseImpl) EnsureIssuePrefix(_ context.Context, _ string) (string, error) {
	return "", ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) RecordBDVersion(_ context.Context, _ string) error {
	return ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) RecordRepoID(_ context.Context, _ string) (bool, error) {
	return false, ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) RecordCloneID(_ context.Context, _ string) (bool, error) {
	return false, ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) ReadProjectID(_ context.Context) (string, error) {
	return "", ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) RecordProjectID(_ context.Context, _ string) error {
	return ErrUseCaseNotImplemented
}

func (bootstrapUseCaseImpl) EnsureRemote(_ context.Context, _, _ string) error {
	return ErrUseCaseNotImplemented
}
