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
// the secret provider the command spawns.  The children write their
// coverage next to ours when there is any.
const modeEnv = "SECRET_RUN_TEST_MODE"

func TestMain(m *testing.M) {
	switch os.Getenv(modeEnv) {
	case "main":
		os.Setenv(modeEnv, "plugin")
		main()
		os.Exit(0)
	case "plugin":
		server.EntrypointSecretProvider(newTestSecret, []string{"plugin"})
	}
	os.Exit(m.Run())
}

// testSecret resolves a handle to the config value of the same name,
// and fails the step its "fail" key names.
type testSecret struct {
	config map[string]string
}

func newTestSecret(_ context.Context, config map[string]string) (integration.SecretProvider, error) {
	if config["fail"] == "init" {
		return nil, errors.New("init refused")
	}
	return &testSecret{config: config}, nil
}

func (s *testSecret) Ping(context.Context) error {
	if s.config["fail"] == "ping" {
		return errors.New("ping refused")
	}
	return nil
}

func (s *testSecret) Resolve(_ context.Context, handle string) (string, error) {
	if s.config["fail"] == "resolve" {
		return "", errors.New("resolve refused")
	}
	return handle + "=" + s.config[handle], nil
}

func (s *testSecret) Close(context.Context) error { return nil }

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

func TestMainResolvesSecret(t *testing.T) {
	conf := filepath.Join(t.TempDir(), "conf.json")
	require.NoError(t, os.WriteFile(conf, []byte(`{"db": "fromfile", "other": "x"}`), 0o600))

	for _, tc := range []struct {
		name string
		env  []string
		args []string
		want string
	}{
		{"from the config file", nil, []string{"-config", conf, os.Args[0], "db"}, "db=fromfile\n"},
		{"option wins", nil, []string{"-config", conf, "-o", "db=val:hunter2", os.Args[0], "db"}, "db=hunter2\n"},
		{"from the environment", []string{"SECRET_RUN_TEST_DB=fromenv"}, []string{"-o", "db=env:SECRET_RUN_TEST_DB", os.Args[0], "db"}, "db=fromenv\n"},
		{"from a command", nil, []string{"-o", "db=cmd:echo hi", os.Args[0], "db"}, "db=hi\n"},
		{"without a ping", nil, []string{"-noping", "-o", "fail=ping", "-o", "db=plain", os.Args[0], "db"}, "db=plain\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, code := runMain(t, tc.env, tc.args...)
			require.Equal(t, 0, code, "stderr: %s", stderr)
			require.Equal(t, tc.want, stdout)
		})
	}
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
		{"no handle", []string{os.Args[0]}, "usage:"},
		{"bad option", []string{"-o", "novalue", os.Args[0], "db"}, "expected key=value"},
		{"missing config", []string{"-config", missing, os.Args[0], "db"}, "cannot open"},
		{"bad config", []string{"-config", bad, os.Args[0], "db"}, "failed to decode json"},
		{"command fails", []string{"-o", "x=cmd:false", os.Args[0], "db"}, "failed to exec"},
		{"plugin missing", []string{filepath.Join(t.TempDir(), "no-such-plugin"), "db"}, "Failed to exec secret provider"},
		{"plugin refuses", []string{"-o", "fail=init", os.Args[0], "db"}, "init refused"},
		{"ping fails", []string{"-o", "fail=ping", os.Args[0], "db"}, "ping refused"},
		{"resolve fails", []string{"-o", "fail=resolve", os.Args[0], "db"}, "resolve refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, code := runMain(t, nil, tc.args...)
			require.Equal(t, 1, code, "stderr: %s", stderr)
			require.True(t, strings.Contains(stderr, tc.want), "stderr %q does not mention %q", stderr, tc.want)
		})
	}
}

type closingProvider struct {
	integration.SecretProvider
	err error
}

func (c closingProvider) Close(context.Context) error { return c.err }

// Closing reports the provider's error first, then the connection's.
func TestGrpcSecretProviderClose(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///nowhere", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	sp := &grpcSecretProvider{SecretProvider: closingProvider{}, conn: conn}
	require.ErrorIs(t, sp.Close(t.Context()), grpc.ErrClientConnClosing)

	boom := errors.New("boom")
	sp = &grpcSecretProvider{SecretProvider: closingProvider{err: boom}, conn: conn}
	require.ErrorIs(t, sp.Close(t.Context()), boom)
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
