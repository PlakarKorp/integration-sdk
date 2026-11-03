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
	"context"
	"errors"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/secret"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeSecret resolves the handles it was given and fails where told to.
type fakeSecret struct {
	secrets  map[string]string
	pingErr  error
	closeErr error
	closed   bool
}

func (f *fakeSecret) Ping(context.Context) error { return f.pingErr }

func (f *fakeSecret) Resolve(_ context.Context, handle string) (string, error) {
	s, ok := f.secrets[handle]
	if !ok {
		return "", errors.New("unknown handle " + handle)
	}
	return s, nil
}

func (f *fakeSecret) Close(context.Context) error {
	f.closed = true
	return f.closeErr
}

func dialSecret(t *testing.T, ctor SecretProviderFn) secret.SecretProviderClient {
	t.Helper()
	conn := serveOn(t, func(lis net.Listener) error { return RunSecretProviderOn(ctor, lis) })
	return secret.NewSecretProviderClient(conn)
}

func TestSecretServerLifecycle(t *testing.T) {
	ctx := t.Context()
	fake := &fakeSecret{secrets: map[string]string{"db": "hunter2"}}

	var gotConfig map[string]string
	client := dialSecret(t, func(_ context.Context, config map[string]string) (integration.SecretProvider, error) {
		gotConfig = config
		return fake, nil
	})

	// nothing works before Init
	for name, call := range map[string]func() error{
		"Ping":    func() error { _, err := client.Ping(ctx, &secret.PingRequest{}); return err },
		"Resolve": func() error { _, err := client.Resolve(ctx, &secret.ResolveRequest{Handle: "db"}); return err },
		"Close":   func() error { _, err := client.Close(ctx, &secret.CloseRequest{}); return err },
	} {
		require.Equal(t, codes.FailedPrecondition, status.Code(call()), name)
	}

	_, err := client.Init(ctx, &secret.InitRequest{Config: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"k": "v"}, gotConfig)
	_, err = client.Init(ctx, &secret.InitRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "second Init")

	_, err = client.Ping(ctx, &secret.PingRequest{})
	require.NoError(t, err)

	res, err := client.Resolve(ctx, &secret.ResolveRequest{Handle: "db"})
	require.NoError(t, err)
	require.Equal(t, "hunter2", res.Secret)

	_, err = client.Resolve(ctx, &secret.ResolveRequest{Handle: "nope"})
	require.ErrorContains(t, err, "unknown handle nope")

	_, err = client.Close(ctx, &secret.CloseRequest{})
	require.NoError(t, err)
	require.True(t, fake.closed)

	_, err = client.Close(ctx, &secret.CloseRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Close after Close")
}

func TestSecretServerErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	t.Run("constructor", func(t *testing.T) {
		client := dialSecret(t, func(context.Context, map[string]string) (integration.SecretProvider, error) {
			return nil, boom
		})
		_, err := client.Init(ctx, &secret.InitRequest{})
		require.ErrorContains(t, err, "boom")
	})

	t.Run("provider", func(t *testing.T) {
		fake := &fakeSecret{pingErr: boom, closeErr: boom}
		client := dialSecret(t, func(context.Context, map[string]string) (integration.SecretProvider, error) {
			return fake, nil
		})
		_, err := client.Init(ctx, &secret.InitRequest{})
		require.NoError(t, err)
		_, err = client.Ping(ctx, &secret.PingRequest{})
		require.ErrorContains(t, err, "boom")
		_, err = client.Close(ctx, &secret.CloseRequest{})
		require.ErrorContains(t, err, "boom")
	})
}
