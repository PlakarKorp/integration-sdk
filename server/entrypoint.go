package server

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/PlakarKorp/integration-sdk"
)

func checkArgs(args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}
}

func EntrypointImporter(ctor integration.ImporterFn, args []string) {
	checkArgs(args)

	if err := RunImporter(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "importer plugin process exited with unexpected error: %s\n", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointExporter(ctor integration.ExporterFn, args []string) {
	checkArgs(args)

	if err := RunExporter(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "exporter plugin process exited with unexpected error: %s\n", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointStorage(ctor integration.StoreFn, args []string) {
	checkArgs(args)

	if err := RunStorage(ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "storage plugin process exited with unexpected error: %s\n", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointInventory(constructor InventoryFn, args []string) {
	checkArgs(args)

	if err := RunInventory(constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "Inventory plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func EntrypointSecretProvider(constructor SecretProviderFn, args []string) {
	checkArgs(args)

	if err := RunSecretProvider(constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "provider plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
