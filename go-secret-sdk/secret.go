package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"google.golang.org/grpc"
)

type Provider interface {
	Ping(ctx context.Context) error
	Resolve(ctx context.Context, handle string) (secret string, err error)
	Close(ctx context.Context) error
}

type ProviderFn func(context.Context, map[string]string) (Provider, error)

func RunProvider(constructor ProviderFn) error {
	conn, listener, err := initconn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	return RunProviderOn(constructor, listener)
}

func RunProviderOn(constructor ProviderFn, listener net.Listener) error {
	server := grpc.NewServer()
	RegisterSecretProviderServer(server, &secretServer{
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
