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
	"flag"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/integration-sdk/stdioconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main ends the process, so it runs in a child: this test binary again,
// told through the environment to be the plugin.  The child writes its
// coverage next to ours when there is any.
const modeEnv = "ROT13_TEST_MODE"

func TestMain(m *testing.M) {
	if os.Getenv(modeEnv) == "main" {
		os.Args = []string{"rot13"}
		main()
	}
	os.Exit(m.Run())
}

func TestRot13(t *testing.T) {
	ctx := t.Context()

	sp, err := newrot13(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, sp.Ping(ctx))
	got, err := sp.Resolve(ctx, "Hello World")
	require.NoError(t, err)
	require.Equal(t, "uryyb jbeyq", got)
	_, err = sp.Resolve(ctx, "a1")
	require.ErrorContains(t, err, "bad input")
	require.NoError(t, sp.Close(ctx))

	sp, err = newrot13(ctx, map[string]string{"n": "1"})
	require.NoError(t, err)
	got, err = sp.Resolve(ctx, "abz")
	require.NoError(t, err)
	require.Equal(t, "bca", got)

	_, err = newrot13(ctx, map[string]string{"n": "one"})
	require.Error(t, err)
}

func TestMainServesSecrets(t *testing.T) {
	ctx := t.Context()

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), modeEnv+"=main")
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir.Value.String())
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	pipe, err := stdioconn.Spawn(cmd)
	require.NoError(t, err)

	cc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return pipe, nil }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	sp, err := client.NewSecretProvider(ctx, cc, map[string]string{"n": "3"})
	require.NoError(t, err)
	got, err := sp.Resolve(ctx, "abc")
	require.NoError(t, err)
	require.Equal(t, "def", got)
	require.NoError(t, sp.Close(ctx))

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
}
