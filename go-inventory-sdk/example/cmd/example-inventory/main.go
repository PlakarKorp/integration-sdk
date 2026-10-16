package main

import (
	"os"

	"github.com/PlakarKorp/go-inventory-sdk/example"
	"github.com/PlakarKorp/go-inventory-sdk/sdk/server"
)

func main() {
	server.Entrypoint(os.Args, example.NewInventory)
}
