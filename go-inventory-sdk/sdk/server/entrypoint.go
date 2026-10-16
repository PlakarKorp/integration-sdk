package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/PlakarKorp/go-inventory-sdk/sdk"
	"google.golang.org/grpc"
)

func RunInventory(constructor InventoryFn) error {
	conn, listener, err := InitConn()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer conn.Close()

	return RunImporterOn(constructor, listener)
}

func RunImporterOn(constructor InventoryFn, listener net.Listener) error {
	server := grpc.NewServer()
	sdk.RegisterInventoryServer(server, NewGrpcInventoryServer(constructor))
	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}

func Entrypoint(args []string, constructor InventoryFn) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}

	if err := RunInventory(constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "Inventory plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
