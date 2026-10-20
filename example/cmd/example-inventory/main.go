package main

import (
	"context"
	"os"

	"github.com/PlakarKorp/integration-sdk/example"
	"github.com/PlakarKorp/integration-sdk/server"
)

func main() {
	server.EntrypointInventory(context.Background(), example.NewInventory, os.Args)
}
