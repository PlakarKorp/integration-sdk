/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package server

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	exporterpb "github.com/PlakarKorp/integration-sdk/proto/exporter"
	importerpb "github.com/PlakarKorp/integration-sdk/proto/importer"
	inventorypb "github.com/PlakarKorp/integration-sdk/proto/inventory"
	secretpb "github.com/PlakarKorp/integration-sdk/proto/secret"
	storagepb "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/PlakarKorp/integration-sdk/stdioconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The entrypoints end the process, so they run in a child: this test
// binary itself, told through the environment which plugin to be.  The
// child writes its coverage next to ours when there is any.
const entrypointEnv = "INTEGRATION_SDK_TEST_ENTRYPOINT"

func TestMain(m *testing.M) {
	flag.Parse()
	if kind := os.Getenv(entrypointEnv); kind != "" {
		runEntrypoint(kind, append([]string{"plugin"}, flag.Args()...))
	}
	os.Exit(m.Run())
}

// runEntrypoint is the child: it never returns.
func runEntrypoint(kind string, args []string) {
	switch kind {
	case "importer":
		EntrypointImporter(importing(&fakeImporter{}), args)
	case "exporter":
		EntrypointExporter(func(context.Context, *integration.Options, string, map[string]string) (integration.Exporter, error) {
			return &fakeExporter{}, nil
		}, args)
	case "storage":
		EntrypointStorage(returning(&fakeStore{}), args)
	case "inventory":
		EntrypointInventory(func(context.Context, map[string]string) (integration.Inventory, error) {
			return &fakeInventory{}, nil
		}, args)
	case "secret":
		EntrypointSecretProvider(func(context.Context, map[string]string) (integration.SecretProvider, error) {
			return &fakeSecret{}, nil
		}, args)
	}
	fmt.Fprintf(os.Stderr, "unknown entrypoint %q\n", kind)
	os.Exit(2)
}

// entrypointCmd is this test binary as the plugin of the given kind,
// with args after its own name.
func entrypointCmd(t *testing.T, kind string, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), entrypointEnv+"="+kind, ListenEnv+"=")
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir.Value.String())
	}
	return cmd
}

// exitCode runs cmd to its end and returns its exit code and stderr.
func exitCode(t *testing.T, cmd *exec.Cmd) (int, string) {
	t.Helper()

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode(), stderr.String()
	}
	require.NoError(t, err)
	return 0, stderr.String()
}

var kinds = []string{"importer", "exporter", "storage", "inventory", "secret"}

func TestEntrypointUsage(t *testing.T) {
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			code, stderr := exitCode(t, entrypointCmd(t, kind, "extra"))
			require.Equal(t, 1, code)
			require.Contains(t, stderr, "Usage: plugin")
		})
	}
}

func TestEntrypointListenFailure(t *testing.T) {
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			cmd := entrypointCmd(t, kind)
			cmd.Env = append(cmd.Env, ListenEnv+"=tcp://127.0.0.1:1")
			code, stderr := exitCode(t, cmd)
			require.Equal(t, 1, code)
			require.Contains(t, stderr, "failed to initialize connection")
		})
	}
}

// Over stdio the plugin serves until its input ends, which is a
// normal exit.
func TestEntrypointServesOverStdio(t *testing.T) {
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()

			cmd := entrypointCmd(t, kind)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			pipe, err := stdioconn.Spawn(cmd)
			require.NoError(t, err)

			cc, err := grpc.NewClient("passthrough:///stdio",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return pipe, nil }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)

			switch kind {
			case "importer":
				_, err = importerpb.NewImporterClient(cc).Init(ctx, &importerpb.InitRequest{Options: &importerpb.Options{}})
			case "exporter":
				_, err = exporterpb.NewExporterClient(cc).Init(ctx, &exporterpb.InitRequest{Options: &exporterpb.Options{}})
			case "storage":
				_, err = storagepb.NewStoreClient(cc).Init(ctx, &storagepb.InitRequest{})
			case "inventory":
				_, err = inventorypb.NewInventoryClient(cc).Init(ctx, &inventorypb.InitRequest{})
			case "secret":
				_, err = secretpb.NewSecretProviderClient(cc).Init(ctx, &secretpb.InitRequest{})
			}
			require.NoError(t, err, "Init")

			// hanging up is what ends the plugin
			require.NoError(t, cc.Close())
			closed := make(chan error, 1)
			go func() { closed <- pipe.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err, "stderr: %s", stderr.String())
			case <-time.After(10 * time.Second):
				cmd.Process.Kill()
				require.Fail(t, "the plugin did not exit once its input ended", "stderr: %s", stderr.String())
			}
			require.Equal(t, 0, cmd.ProcessState.ExitCode())
			require.Empty(t, stderr.String())
		})
	}
}
