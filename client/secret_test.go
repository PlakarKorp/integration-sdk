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

package client

import (
	"context"
	"net"
	"testing"

	secretpb "github.com/PlakarKorp/integration-sdk/proto/secret"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSecretServer struct {
	secretpb.UnimplementedSecretProviderServer

	secret     string
	initErr    error
	pingErr    error
	resolveErr error
	closeErr   error

	gotConfig map[string]string
	gotHandle string
}

func (f *fakeSecretServer) Init(_ context.Context, req *secretpb.InitRequest) (*secretpb.InitResponse, error) {
	f.gotConfig = req.Config
	return &secretpb.InitResponse{}, f.initErr
}

func (f *fakeSecretServer) Ping(context.Context, *secretpb.PingRequest) (*secretpb.PingResponse, error) {
	return &secretpb.PingResponse{}, f.pingErr
}

func (f *fakeSecretServer) Resolve(_ context.Context, req *secretpb.ResolveRequest) (*secretpb.ResolveResponse, error) {
	f.gotHandle = req.Handle
	return &secretpb.ResolveResponse{Secret: f.secret}, f.resolveErr
}

func (f *fakeSecretServer) Close(context.Context, *secretpb.CloseRequest) (*secretpb.CloseResponse, error) {
	return &secretpb.CloseResponse{}, f.closeErr
}

func dialSecretProvider(t *testing.T, srv *fakeSecretServer) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	secretpb.RegisterSecretProviderServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestSecretProvider(t *testing.T) {
	ctx := t.Context()
	srv := &fakeSecretServer{secret: "hunter2"}

	sp, err := NewSecretProvider(ctx, dialSecretProvider(t, srv), map[string]string{"k": "v"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"k": "v"}, srv.gotConfig)

	require.NoError(t, sp.Ping(ctx))
	secret, err := sp.Resolve(ctx, "db")
	require.NoError(t, err)
	require.Equal(t, "hunter2", secret)
	require.Equal(t, "db", srv.gotHandle)
	require.NoError(t, sp.Close(ctx))
}

func TestSecretProvider_Errors(t *testing.T) {
	ctx := t.Context()

	_, err := NewSecretProvider(ctx, dialSecretProvider(t, &fakeSecretServer{initErr: status.Error(codes.Unavailable, "down")}), nil)
	require.ErrorContains(t, err, "I/O error")

	srv := &fakeSecretServer{
		pingErr:    status.Error(codes.Internal, "ping failed"),
		resolveErr: status.Error(codes.NotFound, "resolve failed"),
		closeErr:   status.Error(codes.Internal, "close failed"),
	}
	sp, err := NewSecretProvider(ctx, dialSecretProvider(t, srv), nil)
	require.NoError(t, err)
	require.EqualError(t, sp.Ping(ctx), "ping failed")
	_, err = sp.Resolve(ctx, "db")
	require.EqualError(t, err, "resolve failed")
	require.EqualError(t, sp.Close(ctx), "close failed")
}
