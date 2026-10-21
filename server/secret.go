package server

import (
	"context"
	"fmt"
	"net"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/secret"
	"google.golang.org/grpc"
)

type SecretProviderFn func(context.Context, map[string]string) (integration.SecretProvider, error)

type secretServer struct {
	secret.UnimplementedSecretProviderServer

	instance    instance[integration.SecretProvider]
	constructor SecretProviderFn
}

func (g *secretServer) Init(ctx context.Context, req *secret.InitRequest) (*secret.InitResponse, error) {
	_, err := g.instance.init(func() (integration.SecretProvider, error) {
		return g.constructor(ctx, req.Config)
	})
	if err != nil {
		return nil, err
	}
	return &secret.InitResponse{}, nil
}

func (g *secretServer) Ping(ctx context.Context, req *secret.PingRequest) (*secret.PingResponse, error) {
	provider, err := g.instance.get()
	if err != nil {
		return nil, err
	}

	if err := provider.Ping(ctx); err != nil {
		return nil, err
	}
	return &secret.PingResponse{}, nil
}

func (g *secretServer) Resolve(ctx context.Context, req *secret.ResolveRequest) (*secret.ResolveResponse, error) {
	provider, err := g.instance.get()
	if err != nil {
		return nil, err
	}

	s, err := provider.Resolve(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	return &secret.ResolveResponse{Secret: s}, nil
}

func (g *secretServer) Close(ctx context.Context, req *secret.CloseRequest) (*secret.CloseResponse, error) {
	provider, err := g.instance.take()
	if err != nil {
		return nil, err
	}

	if err := provider.Close(ctx); err != nil {
		return nil, err
	}
	return &secret.CloseResponse{}, nil
}

func RunSecretProvider(ctx context.Context, constructor SecretProviderFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunSecretProviderOn(ctx, constructor, listener)
}

func RunSecretProviderOn(ctx context.Context, constructor SecretProviderFn, listener net.Listener) error {
	server := grpc.NewServer()
	secret.RegisterSecretProviderServer(server, &secretServer{
		constructor: constructor,
	})

	return serve(ctx, server, listener)
}
