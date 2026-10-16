package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"

	"github.com/PlakarKorp/go-inventory-sdk/inventory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcinventory struct {
	inventory.Inventory

	conn *grpc.ClientConn
}

func (g *grpcinventory) Close(ctx context.Context) error {
	ret := g.Inventory.Close(ctx)
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

	wr, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	rd, err := cmd.StdoutPipe()
	if err != nil {
		wr.Close()
		return nil, err
	}

	stdin, ok := rd.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdin is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	stdout, ok := wr.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdout is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	if err := cmd.Start(); err != nil {
		wr.Close()
		rd.Close()
		return nil, err
	}

	conn := NewStdioConn(stdin, stdout, cmd)

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

func ExecInventory(ctx context.Context, params map[string]string, exe string, args []string) (inventory.Inventory, error) {
	conn, err := spawn(ctx, exe, args)
	if err != nil {
		return nil, err
	}

	inv, err := NewGrpcInventory(ctx, conn, params)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &grpcinventory{inv, conn}, nil
}
