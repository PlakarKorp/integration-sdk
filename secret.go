package integration

import (
	"context"
)

type Provider interface {
	Ping(ctx context.Context) error
	Resolve(ctx context.Context, handle string) (secret string, err error)
	Close(ctx context.Context) error
}
