package domain

import "context"

type ConfigSQLRepository interface {
	GetMetadata(ctx context.Context, key string) (string, error)
	SetMetadata(ctx context.Context, key, value string) error
	SetLocalMetadata(ctx context.Context, key, value string) error
	GetConfig(ctx context.Context, key string) (string, error)
	SetConfig(ctx context.Context, key, value string) error
	GetStatistics(ctx context.Context) (Statistics, error)
}

type ConfigUseCase interface {
	ConfigureContributorMode(ctx context.Context, params ContributorModeParams) error
	ConfigureTeamMode(ctx context.Context, params TeamModeParams) error
	VerifyInit(ctx context.Context) (VerifyResult, error)
}

type Issue struct{}

type BatchCreateOptions struct{}

type Statistics struct{}

type ContributorModeParams struct{}

type TeamModeParams struct{}

type GlobalDatabaseParams struct{}

type ImportResult struct{}

type VerifyResult struct{}

func NewConfigUseCase() ConfigUseCase {
	return configUseCaseImpl{}
}

type configUseCaseImpl struct{}

var _ ConfigUseCase = configUseCaseImpl{}

func (configUseCaseImpl) ConfigureContributorMode(_ context.Context, _ ContributorModeParams) error {
	return ErrUseCaseNotImplemented
}

func (configUseCaseImpl) ConfigureTeamMode(_ context.Context, _ TeamModeParams) error {
	return ErrUseCaseNotImplemented
}

func (configUseCaseImpl) VerifyInit(_ context.Context) (VerifyResult, error) {
	return VerifyResult{}, ErrUseCaseNotImplemented
}
