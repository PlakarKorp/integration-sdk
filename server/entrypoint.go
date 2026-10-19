package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	exporter "github.com/PlakarKorp/kloset/connectors/exporter"
	importer "github.com/PlakarKorp/kloset/connectors/importer"
	storage "github.com/PlakarKorp/kloset/connectors/storage"
)

func checkArgs(args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}
}

func EntrypointImporter(ctx context.Context, ctor importer.ImporterFn, args []string) {
	checkArgs(args)

	if err := RunImporter(ctx, ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "importer plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointExporter(ctx context.Context, ctor exporter.ExporterFn, args []string) {
	checkArgs(args)

	if err := RunExporter(ctx, ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "exporter plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointStorage(ctx context.Context, ctor storage.StoreFn, args []string) {
	checkArgs(args)

	if err := RunStorage(ctx, ctor); err != nil {
		if !errors.Is(err, io.EOF) {
			fmt.Fprintf(os.Stderr, "storage plugin process exited with unexpected error: %s", err)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func EntrypointInventory(ctx context.Context, constructor InventoryFn, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}

	if err := RunInventory(ctx, constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "Inventory plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func EntrypointSecretProvider(constructor SecretProviderFn, args []string) {
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s\n", args[0])
		os.Exit(1)
	}

	if err := RunSecretProvider(constructor); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "provider plugin failed unexpectedly: %s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}
