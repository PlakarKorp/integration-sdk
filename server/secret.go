package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/secret"
	"google.golang.org/grpc"
)

type ProviderFn func(context.Context, map[string]string) (integration.Provider, error)

type secretServer struct {
	secret.UnimplementedSecretProviderServer

	provider    integration.Provider
	constructor ProviderFn
}

func (g *secretServer) Init(ctx context.Context, req *secret.InitRequest) (*secret.InitResponse, error) {
	provider, err := g.constructor(ctx, req.Config)
	if err != nil {
		return nil, err
	}

	g.provider = provider
	return &secret.InitResponse{}, nil
}

func (g *secretServer) Ping(ctx context.Context, req *secret.PingRequest) (*secret.PingResponse, error) {
	if err := g.provider.Ping(ctx); err != nil {
		return nil, err
	}
	return &secret.PingResponse{}, nil
}

func (g *secretServer) Resolve(ctx context.Context, req *secret.ResolveRequest) (*secret.ResolveResponse, error) {
	s, err := g.provider.Resolve(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	return &secret.ResolveResponse{Secret: s}, nil
}

func (g *secretServer) Close(ctx context.Context, req *secret.CloseRequest) (*secret.CloseResponse, error) {
	if err := g.provider.Close(ctx); err != nil {
		return nil, err
	}
	return &secret.CloseResponse{}, nil
}

func RunProvider(constructor ProviderFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	return RunProviderOn(constructor, listener)
}

func RunProviderOn(constructor ProviderFn, listener net.Listener) error {
	server := grpc.NewServer()
	secret.RegisterSecretProviderServer(server, &secretServer{
		constructor: constructor,
	})

	return server.Serve(listener)
}

func Entrypoint(args []string, constructor ProviderFn) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}

	if err := RunProvider(constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "provider plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
