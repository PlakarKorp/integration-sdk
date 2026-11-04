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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/location"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// scriptedStoreServer answers every RPC with what it was given.
type scriptedStoreServer struct {
	proto.UnimplementedStoreServer

	errs map[string]error

	flags uint32
	macs  [][]byte

	// batches are what ListStream sends before it ends, with
	// listStreamErr when set; unimplemented makes it refuse instead.
	batches       [][]*proto.Resource
	listStreamErr error
	unimplemented bool

	gotRange *proto.GetRequest_Range
}

func (f *scriptedStoreServer) Init(context.Context, *proto.InitRequest) (*proto.InitResponse, error) {
	return &proto.InitResponse{Origin: "o", Type: "t", Root: "/r", Flags: f.flags}, f.errs["Init"]
}

func (f *scriptedStoreServer) Create(context.Context, *proto.CreateRequest) (*proto.CreateResponse, error) {
	return &proto.CreateResponse{}, f.errs["Create"]
}

func (f *scriptedStoreServer) Open(context.Context, *proto.OpenRequest) (*proto.OpenResponse, error) {
	return &proto.OpenResponse{}, f.errs["Open"]
}

func (f *scriptedStoreServer) Ping(context.Context, *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{}, f.errs["Ping"]
}

func (f *scriptedStoreServer) Mode(context.Context, *proto.ModeRequest) (*proto.ModeResponse, error) {
	return &proto.ModeResponse{Mode: int32(integration.ModeRead)}, f.errs["Mode"]
}

func (f *scriptedStoreServer) Size(context.Context, *proto.SizeRequest) (*proto.SizeResponse, error) {
	return &proto.SizeResponse{}, f.errs["Size"]
}

func (f *scriptedStoreServer) Delete(context.Context, *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	return &proto.DeleteResponse{}, f.errs["Delete"]
}

func (f *scriptedStoreServer) Close(context.Context, *proto.CloseRequest) (*proto.CloseResponse, error) {
	return &proto.CloseResponse{}, f.errs["Close"]
}

func (f *scriptedStoreServer) List(context.Context, *proto.ListRequest) (*proto.ListResponse, error) {
	return &proto.ListResponse{Macs: f.macs}, f.errs["List"]
}

func (f *scriptedStoreServer) ListStream(_ *proto.ListRequest, stream grpc.ServerStreamingServer[proto.ListStreamResponse]) error {
	if f.unimplemented {
		return status.Error(codes.Unimplemented, "no ListStream here")
	}
	for _, batch := range f.batches {
		if err := stream.Send(&proto.ListStreamResponse{Resources: batch}); err != nil {
			return err
		}
	}
	return f.listStreamErr
}

func (f *scriptedStoreServer) Get(req *proto.GetRequest, stream grpc.ServerStreamingServer[proto.GetResponse]) error {
	f.gotRange = req.Range
	return stream.Send(&proto.GetResponse{Chunk: []byte("data")})
}

func dialScriptedStore(t *testing.T, srv *scriptedStoreServer) integration.Store {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	proto.RegisterStoreServer(s, srv)
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

	store, err := NewStorage(t.Context(), conn, "p", nil)
	require.NoError(t, err)
	return store
}

func fullMAC(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestStore_InitAndGetters(t *testing.T) {
	store := dialScriptedStore(t, &scriptedStoreServer{flags: uint32(location.FLAG_FILE)})
	require.Equal(t, "o", store.Origin())
	require.Equal(t, "t", store.Type())
	require.Equal(t, "/r", store.Root())
	require.Equal(t, location.FLAG_FILE, store.Flags())

	mode, err := store.Mode(t.Context())
	require.NoError(t, err)
	require.Equal(t, integration.ModeRead, mode)
	require.NoError(t, store.Delete(t.Context(), integration.StorageResourcePackfile, 0, integration.MAC{}))

	_, err = NewStorage(t.Context(), failingConn{status.Error(codes.Internal, "nope")}, "p", nil)
	require.EqualError(t, err, "nope")
}

func TestStore_RPCErrors(t *testing.T) {
	ctx := t.Context()
	srv := &scriptedStoreServer{errs: map[string]error{}}
	for _, op := range []string{"Create", "Open", "Ping", "Mode", "Size", "Delete", "Close"} {
		srv.errs[op] = status.Error(codes.Internal, op+" failed")
	}
	store := dialScriptedStore(t, srv)

	require.EqualError(t, store.Create(ctx, nil), "Create failed")
	_, err := store.Open(ctx)
	require.EqualError(t, err, "Open failed")
	require.EqualError(t, store.Ping(ctx), "Ping failed")
	_, err = store.Mode(ctx)
	require.EqualError(t, err, "Mode failed")
	_, err = store.Size(ctx)
	require.EqualError(t, err, "Size failed")
	require.ErrorContains(t, store.Delete(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}), "Delete failed")
	require.EqualError(t, store.Close(ctx), "Close failed")
}

func TestStore_ListStream(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1234, 0).UTC()

	collect := func(t *testing.T, store integration.Store, stopAfter int) ([]*integration.StorageResourceEntry, error) {
		t.Helper()
		var got []*integration.StorageResourceEntry
		for res, err := range store.List(ctx, integration.StorageResourcePackfile, 0) {
			if err != nil {
				return got, err
			}
			got = append(got, res)
			if len(got) == stopAfter {
				break
			}
		}
		return got, nil
	}

	t.Run("resources", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{batches: [][]*proto.Resource{
			{{Mac: fullMAC(1), Size: 7, Mtime: timestamppb.New(now)}},
			{{Mac: fullMAC(2), Size: -1}},
		}})
		got, err := collect(t, store, 0)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, integration.MAC(fullMAC(1)), got[0].MAC)
		require.Equal(t, int64(7), got[0].Size)
		require.True(t, now.Equal(got[0].MTime))
		require.Equal(t, int64(-1), got[1].Size)
	})

	t.Run("stopped early", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{batches: [][]*proto.Resource{
			{{Mac: fullMAC(1)}, {Mac: fullMAC(2)}},
		}})
		got, err := collect(t, store, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})

	// a hole in a listing cannot come off the wire, only from a client
	// handing us its own messages.
	t.Run("missing resource", func(t *testing.T) {
		store := &Store{client: &listClient{stream: &fakeClientStream[proto.ListRequest, proto.ListStreamResponse]{
			recv: recvScript(io.EOF, &proto.ListStreamResponse{Resources: []*proto.Resource{nil}}),
		}}}
		_, err := collect(t, store, 0)
		require.ErrorContains(t, err, "nil resource")
	})

	t.Run("bad mac", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{batches: [][]*proto.Resource{{{Mac: []byte("short")}}}})
		_, err := collect(t, store, 0)
		require.ErrorContains(t, err, "invalid MAC length: 5")
	})

	t.Run("stream fails", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{
			batches:       [][]*proto.Resource{{{Mac: fullMAC(1)}}},
			listStreamErr: status.Error(codes.Internal, "nope"),
		})
		got, err := collect(t, store, 0)
		require.Len(t, got, 1)
		require.ErrorContains(t, err, "nope")
	})

	t.Run("cannot start", func(t *testing.T) {
		boom := errors.New("boom")
		store := &Store{client: proto.NewStoreClient(failingConn{boom})}
		_, err := collect(t, store, 0)
		require.ErrorIs(t, err, boom)
	})

	// An older plugin without ListStream is listed the old way.
	t.Run("legacy", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{
			unimplemented: true,
			macs:          [][]byte{fullMAC(1), fullMAC(2)},
		})
		got, err := collect(t, store, 0)
		require.NoError(t, err)
		require.Equal(t, []*integration.StorageResourceEntry{{MAC: integration.MAC(fullMAC(1))}, {MAC: integration.MAC(fullMAC(2))}}, got)

		got, err = collect(t, store, 1)
		require.NoError(t, err)
		require.Len(t, got, 1, "stopped early")
	})

	t.Run("legacy fails", func(t *testing.T) {
		store := dialScriptedStore(t, &scriptedStoreServer{
			unimplemented: true,
			errs:          map[string]error{"List": status.Error(codes.Internal, "nope")},
		})
		_, err := collect(t, store, 0)
		require.ErrorContains(t, err, "failed to list")
		require.ErrorContains(t, err, "nope")
	})
}

func TestStore_PutStreamFailures(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	store := &Store{client: proto.NewStoreClient(failingConn{boom})}
	_, err := store.Put(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, strings.NewReader("x"))
	require.ErrorContains(t, err, "failed to start Put stream")
	require.ErrorIs(t, err, boom)

	// the stream opens but the header cannot be sent
	store = &Store{client: &putClient{stream: &fakeClientStream[proto.PutRequest, proto.PutResponse]{
		send: sendScript[proto.PutRequest](boom),
	}}}
	_, err = store.Put(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, strings.NewReader("x"))
	require.ErrorContains(t, err, "failed to send mac")
	require.ErrorIs(t, err, boom)
}

// listClient is a store client whose ListStream hands out the given stream.
type listClient struct {
	proto.StoreClient
	stream grpc.ServerStreamingClient[proto.ListStreamResponse]
}

func (c *listClient) ListStream(context.Context, *proto.ListRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ListStreamResponse], error) {
	return c.stream, nil
}

// putClient is a store client whose Put hands out the given stream.
type putClient struct {
	proto.StoreClient
	stream grpc.ClientStreamingClient[proto.PutRequest, proto.PutResponse]
}

func (c *putClient) Put(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[proto.PutRequest, proto.PutResponse], error) {
	return c.stream, nil
}

func TestStore_Get(t *testing.T) {
	ctx := t.Context()
	srv := &scriptedStoreServer{}
	store := dialScriptedStore(t, srv)

	rd, err := store.Get(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, &integration.Range{Offset: 3, Length: 4})
	require.NoError(t, err)
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
	require.Equal(t, "data", string(data))
	require.Equal(t, uint64(3), srv.gotRange.GetOffset())
	require.Equal(t, uint32(4), srv.gotRange.GetLength())

	boom := errors.New("boom")
	failing := &Store{client: proto.NewStoreClient(failingConn{boom})}
	_, err = failing.Get(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, nil)
	require.ErrorContains(t, err, "get packfile")
	require.ErrorIs(t, err, boom)
}

func TestReceiveChunks_EmptyRead(t *testing.T) {
	rd := ReceiveChunks(func() ([]byte, error) { return nil, io.EOF })
	n, err := rd.Read(nil)
	require.NoError(t, err)
	require.Zero(t, n)
}
