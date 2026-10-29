package main

import (
	"os"

	"github.com/PlakarKorp/integration-sdk/example"
	"github.com/PlakarKorp/integration-sdk/server"
)

func main() {
	server.EntrypointProvisioner(example.NewProvisioner, example.Resolver{}, os.Args)
}
