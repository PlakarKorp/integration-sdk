package sdk

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type secretClient struct {
	client SecretProviderClient
}

func unwrap(err error) error {
	if err == nil {
		return nil
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	default:
		return fmt.Errorf("%s", status.Message())
	}
}

func NewSecretProvider(ctx context.Context, conn *grpc.ClientConn, config map[string]string) (Provider, error) {
	client := NewSecretProviderClient(conn)

	if _, err := client.Init(ctx, &InitRequest{Config: config}); err != nil {
		return nil, err
	}

	return &secretClient{client: client}, nil
}

func (g *secretClient) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &PingRequest{})
	return unwrap(err)
}

func (g *secretClient) Resolve(ctx context.Context, handle string) (string, error) {
	res, err := g.client.Resolve(ctx, &ResolveRequest{
		Handle: handle,
	})
	if err != nil {
		return "", unwrap(err)
	}

	return res.Secret, nil
}

func (g *secretClient) Close(ctx context.Context) error {
	_, err := g.client.Close(ctx, &CloseRequest{})
	return unwrap(err)
}
