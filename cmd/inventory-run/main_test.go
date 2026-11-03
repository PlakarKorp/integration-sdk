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

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

// main ends the process, so it runs in a child: this test binary again,
// told through the environment to be the command, and once more to be
// the inventory plugin the command spawns.  The children write their
// coverage next to ours when there is any.
const modeEnv = "INVENTORY_RUN_TEST_MODE"

func TestMain(m *testing.M) {
	switch os.Getenv(modeEnv) {
	case "main":
		os.Setenv(modeEnv, "plugin")
		main()
		os.Exit(0)
	case "plugin":
		server.EntrypointInventory(newTestInventory, []string{"plugin"})
	}
	os.Exit(m.Run())
}

// testInventory lists the config it was given, one entry per key, and
// fails the step its "fail" key names.
type testInventory struct {
	config map[string]string
}

func newTestInventory(_ context.Context, config map[string]string) (integration.Inventory, error) {
	if config["fail"] == "init" {
		return nil, errors.New("init refused")
	}
	return &testInventory{config: config}, nil
}

func (inv *testInventory) List(_ context.Context, c chan<- *integration.InventoryEntry) error {
	defer close(c)

	keys := make([]string, 0, len(inv.config))
	for k := range inv.config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c <- &integration.InventoryEntry{
			Class: integration.ResourceClassCompute,
			Name:  k + "=" + inv.config[k],
		}
	}
	// an entry without a class is not shown
	c <- &integration.InventoryEntry{Name: "hidden"}

	if inv.config["fail"] == "list" {
		return errors.New("list refused")
	}
	return nil
}

func (inv *testInventory) Close(context.Context) error {
	if inv.config["fail"] == "close" {
		return errors.New("close refused")
	}
	return nil
}

// runMain runs the command with args and returns what it printed and
// its exit code.
func runMain(t *testing.T, env []string, args ...string) (stdout, stderr string, code int) {
	t.Helper()

	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), modeEnv+"=main")
	cmd.Env = append(cmd.Env, env...)
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir.Value.String())
	}

	var out, errs bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errs
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return out.String(), errs.String(), exit.ExitCode()
	}
	require.NoError(t, err)
	return out.String(), errs.String(), 0
}

func TestMainListsInventory(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "conf.json")
	require.NoError(t, os.WriteFile(conf, []byte(`{"a": "9", "d": "4"}`), 0o600))

	stdout, stderr, code := runMain(t, []string{"INVENTORY_RUN_TEST_B=fromenv"},
		"-config", conf,
		"-o", "a=val:1",
		"-o", "b=env:INVENTORY_RUN_TEST_B",
		"-o", "c=cmd:echo hi",
		"-o", "e=plain",
		os.Args[0])
	require.Equal(t, 0, code, "stderr: %s", stderr)
	for _, want := range []string{`"a=1"`, `"b=fromenv"`, `"c=hi"`, `"d=4"`, `"e=plain"`} {
		require.Contains(t, stdout, want)
	}
	require.NotContains(t, stdout, "hidden")
	require.NotContains(t, stdout, `"a=9"`, "the config file overrode -o")
}

func TestMainFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	bad := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte("{"), 0o600))

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no executable", nil, "usage:"},
		{"bad option", []string{"-o", "novalue", os.Args[0]}, "expected key=value"},
		{"missing config", []string{"-config", missing, os.Args[0]}, "cannot open"},
		{"bad config", []string{"-config", bad, os.Args[0]}, "failed to decode json"},
		{"command fails", []string{"-o", "x=cmd:false", os.Args[0]}, "failed to exec"},
		{"plugin missing", []string{filepath.Join(t.TempDir(), "no-such-plugin")}, "ConnectPlugin failed"},
		{"plugin refuses", []string{"-o", "fail=init", os.Args[0]}, "init refused"},
		{"listing fails", []string{"-o", "fail=list", os.Args[0]}, "list refused"},
		{"close fails", []string{"-o", "fail=close", os.Args[0]}, "close refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, code := runMain(t, nil, tc.args...)
			require.Equal(t, 1, code, "stderr: %s", stderr)
			require.True(t, strings.Contains(stderr, tc.want), "stderr %q does not mention %q", stderr, tc.want)
		})
	}
}

type closingInventory struct {
	integration.Inventory
	err error
}

func (c closingInventory) Close(context.Context) error { return c.err }

// Closing reports the inventory's error first, then the connection's.
func TestGrpcInventoryClose(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///nowhere", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	inv := &grpcinventory{Inventory: closingInventory{}, conn: conn}
	require.ErrorIs(t, inv.Close(t.Context()), grpc.ErrClientConnClosing)

	boom := errors.New("boom")
	inv = &grpcinventory{Inventory: closingInventory{err: boom}, conn: conn}
	require.ErrorIs(t, inv.Close(t.Context()), boom)
}

// grpc.NewClient only fails on a target it cannot resolve, which the
// hard-coded one is not: unregistering its scheme is the one way to see
// spawn clean up after a client it could not build.
func TestSpawnClientFailure(t *testing.T) {
	t.Setenv(modeEnv, "plugin")
	scheme := resolver.GetDefaultScheme()
	resolver.SetDefaultScheme("unregistered")
	t.Cleanup(func() { resolver.SetDefaultScheme(scheme) })

	_, err := spawn(t.Context(), os.Args[0], nil)
	require.ErrorContains(t, err, "unregistered")
}
