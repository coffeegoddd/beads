package uow

import (
	"context"

	"github.com/steveyegge/beads/internal/storage/domain"
)

type UnitOfWork interface {
	Close(ctx context.Context)
	Commit(ctx context.Context, message string) error
	Bootstrap() domain.BootstrapUseCase
	Config() domain.ConfigUseCase
}

type UnitOfWorkProvider interface {
	NewUOW(ctx context.Context) (UnitOfWork, error)
	Close(ctx context.Context) error
}

func NewUOW(ctx context.Context, p TxProvider) (UnitOfWork, error) {
	tx, err := p.NewTx(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Begin(ctx); err != nil {
		return nil, err
	}
	return &baseUOW{tx: tx}, nil
}

type baseUOW struct {
	tx Tx
}

func (u *baseUOW) Commit(ctx context.Context, message string) error {
	return u.tx.Commit(ctx, message)
}

func (u *baseUOW) Close(ctx context.Context) {
	u.tx.RollbackUnlessCommitted(ctx)
}

func (u *baseUOW) Bootstrap() domain.BootstrapUseCase {
	return domain.NewBootstrapUseCase()
}

func (u *baseUOW) Config() domain.ConfigUseCase {
	return domain.NewConfigUseCase()
}
