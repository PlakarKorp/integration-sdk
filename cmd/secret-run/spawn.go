package main

import (
	"context"
	"net"
	"os"
	"os/exec"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/integration-sdk/stdioconn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcSecretProvider struct {
	integration.SecretProvider

	conn *grpc.ClientConn
}

func (g *grpcSecretProvider) Close(ctx context.Context) error {
	ret := g.SecretProvider.Close(ctx)
	if err := g.conn.Close(); err != nil {
		if ret == nil {
			ret = err
		}
	}
	return ret
}

func spawn(ctx context.Context, exe string, args []string) (*grpc.ClientConn, error) {
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Stderr = os.Stderr // let child's stderr pass through for logging

	conn, err := stdioconn.Spawn(cmd)
	if err != nil {
		return nil, err
	}

	client, err := grpc.NewClient("127.0.0.1:0",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return conn, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(0),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return client, nil
}

func ExecSecretProvider(ctx context.Context, params map[string]string, exe string, args []string) (integration.SecretProvider, error) {
	conn, err := spawn(ctx, exe, args)
	if err != nil {
		return nil, err
	}

	sp, err := client.NewSecretProvider(ctx, conn, params)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &grpcSecretProvider{sp, conn}, nil
}
