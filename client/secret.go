package client

import (
	"context"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/secret"
	"google.golang.org/grpc"
)

type secretClient struct {
	client secret.SecretProviderClient
}

func NewSecretProvider(ctx context.Context, conn *grpc.ClientConn, config map[string]string) (integration.SecretProvider, error) {
	client := secret.NewSecretProviderClient(conn)

	if _, err := client.Init(ctx, &secret.InitRequest{Config: config}); err != nil {
		return nil, unwrap(err)
	}

	return &secretClient{client: client}, nil
}

func (g *secretClient) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &secret.PingRequest{})
	return unwrap(err)
}

func (g *secretClient) Resolve(ctx context.Context, handle string) (string, error) {
	res, err := g.client.Resolve(ctx, &secret.ResolveRequest{
		Handle: handle,
	})
	if err != nil {
		return "", unwrap(err)
	}

	return res.Secret, nil
}

func (g *secretClient) Close(ctx context.Context) error {
	_, err := g.client.Close(ctx, &secret.CloseRequest{})
	return unwrap(err)
}
