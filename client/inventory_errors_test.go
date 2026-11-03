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
	"errors"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	inventorypb "github.com/PlakarKorp/integration-sdk/proto/inventory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// scriptedInventoryServer sends the entries it was given, then fails
// where told to.
type scriptedInventoryServer struct {
	inventorypb.UnimplementedInventoryServer

	entries  []*inventorypb.ListResponse
	initErr  error
	listErr  error
	closeErr error
}

func (f *scriptedInventoryServer) Init(context.Context, *inventorypb.InitRequest) (*inventorypb.InitResponse, error) {
	return &inventorypb.InitResponse{}, f.initErr
}

func (f *scriptedInventoryServer) List(_ *inventorypb.ListRequest, stream grpc.ServerStreamingServer[inventorypb.ListResponse]) error {
	for _, e := range f.entries {
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	return f.listErr
}

func (f *scriptedInventoryServer) Close(context.Context, *inventorypb.CloseRequest) (*inventorypb.CloseResponse, error) {
	return &inventorypb.CloseResponse{}, f.closeErr
}

func dialScriptedInventory(t *testing.T, srv *scriptedInventoryServer) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	inventorypb.RegisterInventoryServer(s, srv)
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

// listAll collects what List sends and the error it ends with.
func listAll(t *testing.T, inv integration.Inventory) ([]*integration.InventoryEntry, error) {
	t.Helper()

	c := make(chan *integration.InventoryEntry)
	var entries []*integration.InventoryEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range c {
			entries = append(entries, e)
		}
	}()
	err := inv.List(t.Context(), c)
	<-done
	return entries, err
}

func TestInventory_ListEndpoints(t *testing.T) {
	srv := &scriptedInventoryServer{entries: []*inventorypb.ListResponse{{Entry: &inventorypb.InventoryEntry{
		Urn: "urn:test:one",
		Endpoints: []*inventorypb.HostEndpoint{
			{Type: "host", Endpoint: "db.example.com", Attributes: map[string]string{"port": "5432"}},
			{Type: "inet4", Endpoint: "10.0.0.1"},
		},
	}}}}
	inv, err := NewInventory(t.Context(), dialScriptedInventory(t, srv), nil)
	require.NoError(t, err)

	entries, err := listAll(t, inv)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []integration.HostEndpoint{
		{Type: integration.EndpointHost, Endpoint: "db.example.com", Attributes: map[string]string{"port": "5432"}},
		{Type: integration.EndpointInet4, Endpoint: "10.0.0.1"},
	}, entries[0].Endpoints)

	require.NoError(t, inv.Close(t.Context()))
}

func TestInventory_Errors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	t.Run("init", func(t *testing.T) {
		_, err := NewInventory(ctx, dialScriptedInventory(t, &scriptedInventoryServer{initErr: status.Error(codes.Internal, "nope")}), nil)
		require.EqualError(t, err, "nope")
	})

	t.Run("listing cannot start", func(t *testing.T) {
		inv := &grpcInventoryClient{client: inventorypb.NewInventoryClient(failingConn{boom})}
		_, err := listAll(t, inv)
		require.ErrorIs(t, err, boom)
	})

	t.Run("listing fails", func(t *testing.T) {
		srv := &scriptedInventoryServer{
			entries: []*inventorypb.ListResponse{{Entry: &inventorypb.InventoryEntry{Urn: "one"}}},
			listErr: status.Error(codes.Internal, "nope"),
		}
		inv, err := NewInventory(ctx, dialScriptedInventory(t, srv), nil)
		require.NoError(t, err)
		entries, err := listAll(t, inv)
		require.EqualError(t, err, "nope")
		require.Len(t, entries, 1, "what came before the failure is kept")
	})

	t.Run("missing entry", func(t *testing.T) {
		srv := &scriptedInventoryServer{entries: []*inventorypb.ListResponse{{}}}
		inv, err := NewInventory(ctx, dialScriptedInventory(t, srv), nil)
		require.NoError(t, err)
		_, err = listAll(t, inv)
		require.ErrorContains(t, err, "expected a record")
	})

	t.Run("close", func(t *testing.T) {
		inv, err := NewInventory(ctx, dialScriptedInventory(t, &scriptedInventoryServer{closeErr: status.Error(codes.Internal, "nope")}), nil)
		require.NoError(t, err)
		require.EqualError(t, inv.Close(ctx), "nope")
	})
}
