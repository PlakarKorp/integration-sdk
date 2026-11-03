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
	inventorypb "github.com/PlakarKorp/integration-sdk/proto/inventory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scriptedInventory lists what it is given, or keeps listing until
// told to stop when endless.
type scriptedInventory struct {
	entries  []*integration.InventoryEntry
	listErr  error
	closeErr error
	endless  bool
	closed   bool
}

func (s *scriptedInventory) List(ctx context.Context, c chan<- *integration.InventoryEntry) error {
	defer close(c)

	if s.endless {
		for {
			select {
			case c <- &integration.InventoryEntry{Name: "more"}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	for _, e := range s.entries {
		c <- e
	}
	return s.listErr
}

func (s *scriptedInventory) Close(context.Context) error {
	s.closed = true
	return s.closeErr
}

func newInventoryPlugin(t *testing.T, inv integration.Inventory) *grpcInventoryServer {
	t.Helper()
	plugin := &grpcInventoryServer{
		constructor: func(context.Context, map[string]string) (integration.Inventory, error) { return inv, nil },
	}
	_, err := plugin.Init(t.Context(), &inventorypb.InitRequest{})
	require.NoError(t, err)
	return plugin
}

func TestInventoryServerEndpointsAndClose(t *testing.T) {
	ctx := t.Context()
	inv := &scriptedInventory{entries: []*integration.InventoryEntry{{
		Name: "one",
		Endpoints: []integration.HostEndpoint{
			{Type: integration.EndpointHost, Endpoint: "db.example.com", Attributes: map[string]string{"port": "5432"}},
			{Type: integration.EndpointInet4, Endpoint: "10.0.0.1"},
		},
	}}}
	client := dialInventory(t, inv)

	stream, err := client.List(ctx, &inventorypb.ListRequest{})
	require.NoError(t, err)
	res, err := stream.Recv()
	require.NoError(t, err)
	endpoints := res.GetEntry().GetEndpoints()
	require.Len(t, endpoints, 2)
	require.Equal(t, "host", endpoints[0].Type)
	require.Equal(t, "db.example.com", endpoints[0].Endpoint)
	require.Equal(t, map[string]string{"port": "5432"}, endpoints[0].Attributes)
	require.Equal(t, "inet4", endpoints[1].Type)

	_, err = client.Close(ctx, &inventorypb.CloseRequest{})
	require.NoError(t, err)
	require.True(t, inv.closed)
	_, err = client.Close(ctx, &inventorypb.CloseRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "Close after Close")
}

func TestInventoryServerErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	t.Run("uninitialized", func(t *testing.T) {
		plugin := &grpcInventoryServer{}
		stream := &fakeStream[inventorypb.ListRequest, inventorypb.ListResponse]{ctx: ctx}
		require.ErrorIs(t, plugin.List(&inventorypb.ListRequest{}, stream), ErrNotInitialized)
		_, err := plugin.Close(ctx, &inventorypb.CloseRequest{})
		require.ErrorIs(t, err, ErrNotInitialized)
	})

	t.Run("constructor", func(t *testing.T) {
		plugin := &grpcInventoryServer{
			constructor: func(context.Context, map[string]string) (integration.Inventory, error) { return nil, boom },
		}
		_, err := plugin.Init(ctx, &inventorypb.InitRequest{})
		require.ErrorIs(t, err, boom)
	})

	t.Run("listing", func(t *testing.T) {
		plugin := newInventoryPlugin(t, &scriptedInventory{listErr: boom})
		stream := &fakeStream[inventorypb.ListRequest, inventorypb.ListResponse]{ctx: ctx}
		require.ErrorIs(t, plugin.List(&inventorypb.ListRequest{}, stream), boom)
	})

	// The stream dying is what stops the inventory, and what is reported.
	t.Run("sending", func(t *testing.T) {
		plugin := newInventoryPlugin(t, &scriptedInventory{endless: true})
		stream := &fakeStream[inventorypb.ListRequest, inventorypb.ListResponse]{
			ctx:  ctx,
			send: func(*inventorypb.ListResponse) error { return boom },
		}
		require.ErrorIs(t, plugin.List(&inventorypb.ListRequest{}, stream), boom)
	})

	t.Run("closing", func(t *testing.T) {
		plugin := newInventoryPlugin(t, &scriptedInventory{closeErr: boom})
		_, err := plugin.Close(ctx, &inventorypb.CloseRequest{})
		require.ErrorIs(t, err, boom)
	})
}

func TestRunInventoryOn(t *testing.T) {
	conn := serveOn(t, func(lis net.Listener) error {
		return RunInventoryOn(func(context.Context, map[string]string) (integration.Inventory, error) {
			return &scriptedInventory{}, nil
		}, lis)
	})

	_, err := inventorypb.NewInventoryClient(conn).Init(t.Context(), &inventorypb.InitRequest{})
	require.NoError(t, err)
}
