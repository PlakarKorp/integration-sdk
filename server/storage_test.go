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
	"io"
	"iter"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/integration-sdk/location"
	storagepb "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeStore keeps what it is given and fails the operations named in
// errs.
type fakeStore struct {
	errs map[string]error

	config    []byte
	resources []*integration.StorageResourceEntry
	data      []byte

	put      []byte
	putMAC   integration.MAC
	putRes   integration.StorageResource
	putFlag  integration.StorageFlag
	gotRange *integration.Range
	deleted  []integration.MAC
	closed   bool
}

func (f *fakeStore) Create(_ context.Context, config []byte) error {
	if err := f.errs["Create"]; err != nil {
		return err
	}
	f.config = config
	return nil
}

func (f *fakeStore) Open(context.Context) ([]byte, error) {
	return f.config, f.errs["Open"]
}

func (f *fakeStore) Ping(context.Context) error { return f.errs["Ping"] }

func (f *fakeStore) Origin() string        { return "o" }
func (f *fakeStore) Type() string          { return "t" }
func (f *fakeStore) Root() string          { return "/r" }
func (f *fakeStore) Flags() location.Flags { return location.FLAG_FILE }
func (f *fakeStore) Mode(context.Context) (integration.Mode, error) {
	return integration.ModeRead | integration.ModeWrite, f.errs["Mode"]
}
func (f *fakeStore) Size(context.Context) (int64, error) {
	return int64(len(f.data)), f.errs["Size"]
}

func (f *fakeStore) List(context.Context, integration.StorageResource, integration.StorageFlag) iter.Seq2[*integration.StorageResourceEntry, error] {
	return func(yield func(*integration.StorageResourceEntry, error) bool) {
		for _, res := range f.resources {
			if !yield(res, nil) {
				return
			}
		}
		if err := f.errs["List"]; err != nil {
			yield(nil, err)
		}
	}
}

func (f *fakeStore) Put(_ context.Context, res integration.StorageResource, flag integration.StorageFlag, mac integration.MAC, rd io.Reader) (int64, error) {
	if err := f.errs["Put"]; err != nil {
		return 0, err
	}
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}
	f.put, f.putMAC, f.putRes, f.putFlag = data, mac, res, flag
	return int64(len(data)), nil
}

func (f *fakeStore) Get(_ context.Context, _ integration.StorageResource, _ integration.StorageFlag, _ integration.MAC, rg *integration.Range) (io.ReadCloser, error) {
	if err := f.errs["Get"]; err != nil {
		return nil, err
	}
	f.gotRange = rg
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (f *fakeStore) Delete(_ context.Context, _ integration.StorageResource, _ integration.StorageFlag, mac integration.MAC) error {
	if err := f.errs["Delete"]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, mac)
	return nil
}

func (f *fakeStore) Close(context.Context) error {
	f.closed = true
	return f.errs["Close"]
}

func dialStorage(t *testing.T, ctor integration.StoreFn) *grpc.ClientConn {
	t.Helper()
	return serveOn(t, func(lis net.Listener) error { return RunStorageOn(ctor, lis) })
}

func returning(store integration.Store) integration.StoreFn {
	return func(context.Context, string, map[string]string) (integration.Store, error) {
		return store, nil
	}
}

func newStoragePlugin(t *testing.T, store integration.Store) *storagePluginServer {
	t.Helper()
	plugin := &storagePluginServer{constructor: returning(store)}
	_, err := plugin.Init(t.Context(), &storagepb.InitRequest{})
	require.NoError(t, err)
	return plugin
}

// The plugin is driven through the client of the same SDK, the way
// plakar does.
func TestStorageServerRoundTrip(t *testing.T) {
	ctx := t.Context()
	fake := &fakeStore{
		data: []byte("payload"),
		resources: []*integration.StorageResourceEntry{
			{MAC: integration.MAC{1}, Size: 7, MTime: time.Unix(10, 0).UTC()},
			{MAC: integration.MAC{2}, Size: -1},
		},
	}

	var gotProto string
	var gotConfig map[string]string
	conn := dialStorage(t, func(_ context.Context, p string, config map[string]string) (integration.Store, error) {
		gotProto, gotConfig = p, config
		return fake, nil
	})

	store, err := client.NewStorage(ctx, conn, "fs", map[string]string{"k": "v"})
	require.NoError(t, err)
	require.Equal(t, "fs", gotProto)
	require.Equal(t, map[string]string{"k": "v"}, gotConfig)
	require.Equal(t, "o", store.Origin())
	require.Equal(t, "t", store.Type())
	require.Equal(t, "/r", store.Root())
	require.Equal(t, location.FLAG_FILE, store.Flags())

	require.NoError(t, store.Create(ctx, []byte("cfg")))
	config, err := store.Open(ctx)
	require.NoError(t, err)
	require.Equal(t, "cfg", string(config))
	require.NoError(t, store.Ping(ctx))

	mode, err := store.Mode(ctx)
	require.NoError(t, err)
	require.Equal(t, integration.ModeRead|integration.ModeWrite, mode)
	size, err := store.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), size)

	var listed []*integration.StorageResourceEntry
	for res, err := range store.List(ctx, integration.StorageResourcePackfile, integration.StorageHot) {
		require.NoError(t, err)
		listed = append(listed, res)
	}
	require.Len(t, listed, 2)
	for i, res := range listed {
		require.Equal(t, fake.resources[i].MAC, res.MAC)
		require.Equal(t, fake.resources[i].Size, res.Size)
		require.True(t, fake.resources[i].MTime.Equal(res.MTime), "resource %d mtime", i)
	}

	n, err := store.Put(ctx, integration.StorageResourceState, integration.StorageHot, integration.MAC{3}, strings.NewReader("hello"))
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	require.Equal(t, "hello", string(fake.put))
	require.Equal(t, integration.MAC{3}, fake.putMAC)
	require.Equal(t, integration.StorageResourceState, fake.putRes)
	require.Equal(t, integration.StorageHot, fake.putFlag)

	rd, err := store.Get(ctx, integration.StorageResourcePackfile, 0, integration.MAC{1}, &integration.Range{Offset: 1, Length: 2})
	require.NoError(t, err)
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
	require.Equal(t, "payload", string(data))
	require.Equal(t, &integration.Range{Offset: 1, Length: 2}, fake.gotRange)

	rd, err = store.Get(ctx, integration.StorageResourcePackfile, 0, integration.MAC{1}, nil)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
	require.Nil(t, fake.gotRange)

	require.NoError(t, store.Delete(ctx, integration.StorageResourceLock, 0, integration.MAC{4}))
	require.Equal(t, []integration.MAC{{4}}, fake.deleted)

	require.NoError(t, store.Close(ctx))
	require.True(t, fake.closed)
}

func TestStorageServerStoreErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	fake := &fakeStore{errs: map[string]error{}}
	for _, op := range []string{"Create", "Open", "Ping", "Mode", "Size", "List", "Put", "Get", "Delete", "Close"} {
		fake.errs[op] = errors.New(op + " failed")
	}
	conn := dialStorage(t, returning(fake))

	store, err := client.NewStorage(ctx, conn, "p", nil)
	require.NoError(t, err)
	require.ErrorContains(t, store.Create(ctx, nil), "Create failed")
	_, err = store.Open(ctx)
	require.ErrorContains(t, err, "Open failed")
	require.ErrorContains(t, store.Ping(ctx), "Ping failed")
	_, err = store.Mode(ctx)
	require.ErrorContains(t, err, "Mode failed")
	_, err = store.Size(ctx)
	require.ErrorContains(t, err, "Size failed")
	for _, err := range store.List(ctx, integration.StorageResourcePackfile, 0) {
		require.ErrorContains(t, err, "List failed")
	}
	_, err = store.Put(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, strings.NewReader("x"))
	require.ErrorContains(t, err, "Put failed")
	rd, err := store.Get(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}, nil)
	require.NoError(t, err, "the failure comes with the first chunk")
	_, err = io.ReadAll(rd)
	require.ErrorContains(t, err, "Get failed")
	require.ErrorContains(t, store.Delete(ctx, integration.StorageResourcePackfile, 0, integration.MAC{}), "Delete failed")
	require.ErrorContains(t, store.Close(ctx), "Close failed")

	t.Run("constructor", func(t *testing.T) {
		conn := dialStorage(t, func(context.Context, string, map[string]string) (integration.Store, error) {
			return nil, boom
		})
		_, err := client.NewStorage(ctx, conn, "p", nil)
		require.ErrorContains(t, err, "boom")
	})
}

// Every RPC refuses to work before Init, streaming ones included.
func TestStorageServerUninitialized(t *testing.T) {
	ctx := t.Context()
	raw := storagepb.NewStoreClient(dialStorage(t, returning(&fakeStore{})))

	calls := map[string]func() error{
		"Create": func() error { _, err := raw.Create(ctx, &storagepb.CreateRequest{}); return err },
		"Open":   func() error { _, err := raw.Open(ctx, &storagepb.OpenRequest{}); return err },
		"Ping":   func() error { _, err := raw.Ping(ctx, &storagepb.PingRequest{}); return err },
		"Mode":   func() error { _, err := raw.Mode(ctx, &storagepb.ModeRequest{}); return err },
		"Size":   func() error { _, err := raw.Size(ctx, &storagepb.SizeRequest{}); return err },
		"List":   func() error { _, err := raw.List(ctx, &storagepb.ListRequest{}); return err },
		"Delete": func() error { _, err := raw.Delete(ctx, &storagepb.DeleteRequest{}); return err },
		"Close":  func() error { _, err := raw.Close(ctx, &storagepb.CloseRequest{}); return err },
		"ListStream": func() error {
			stream, err := raw.ListStream(ctx, &storagepb.ListRequest{})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
		"Put": func() error {
			stream, err := raw.Put(ctx)
			if err != nil {
				return err
			}
			_, err = stream.CloseAndRecv()
			return err
		},
		"Get": func() error {
			stream, err := raw.Get(ctx, &storagepb.GetRequest{})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
	}
	for name, call := range calls {
		require.Equal(t, codes.FailedPrecondition, status.Code(call()), name)
	}
}

// The legacy List, which the client only falls back to, still answers.
func TestStorageServerLegacyList(t *testing.T) {
	ctx := t.Context()
	fake := &fakeStore{resources: []*integration.StorageResourceEntry{{MAC: integration.MAC{1}}, {MAC: integration.MAC{2}}}}
	raw := storagepb.NewStoreClient(dialStorage(t, returning(fake)))

	_, err := raw.Init(ctx, &storagepb.InitRequest{})
	require.NoError(t, err)

	res, err := raw.List(ctx, &storagepb.ListRequest{})
	require.NoError(t, err)
	require.Equal(t, [][]byte{bytes.Repeat([]byte{0}, 32), bytes.Repeat([]byte{0}, 32)}[:0], res.Macs[:0])
	require.Len(t, res.Macs, 2)
	require.Equal(t, integration.MAC{2}, integration.MAC(res.Macs[1]))

	fake.errs = map[string]error{"List": errors.New("boom")}
	_, err = raw.List(ctx, &storagepb.ListRequest{})
	require.ErrorContains(t, err, "boom")
}

// A listing longer than one message is flushed as it goes.
func TestStorageServerListStreamBatches(t *testing.T) {
	ctx := t.Context()
	fake := &fakeStore{}
	for i := range 1024 + 1 {
		fake.resources = append(fake.resources, &integration.StorageResourceEntry{MAC: integration.MAC{byte(i), byte(i >> 8)}})
	}
	store, err := client.NewStorage(ctx, dialStorage(t, returning(fake)), "p", nil)
	require.NoError(t, err)

	n := 0
	for res, err := range store.List(ctx, integration.StorageResourcePackfile, 0) {
		require.NoError(t, err)
		require.Equal(t, fake.resources[n].MAC, res.MAC)
		n++
	}
	require.Equal(t, len(fake.resources), n)
}

func TestStorageServerStreamFailures(t *testing.T) {
	boom := errors.New("boom")

	t.Run("ListStream flush", func(t *testing.T) {
		fake := &fakeStore{}
		for range 1024 {
			fake.resources = append(fake.resources, &integration.StorageResourceEntry{})
		}
		plugin := newStoragePlugin(t, fake)
		stream := &fakeStream[storagepb.ListRequest, storagepb.ListStreamResponse]{
			ctx:  t.Context(),
			send: func(*storagepb.ListStreamResponse) error { return boom },
		}
		require.ErrorIs(t, plugin.ListStream(&storagepb.ListRequest{}, stream), boom)
	})

	t.Run("ListStream empty", func(t *testing.T) {
		plugin := newStoragePlugin(t, &fakeStore{})
		stream := &fakeStream[storagepb.ListRequest, storagepb.ListStreamResponse]{
			ctx:  t.Context(),
			send: func(*storagepb.ListStreamResponse) error { return boom },
		}
		require.NoError(t, plugin.ListStream(&storagepb.ListRequest{}, stream), "nothing to send")
	})

	t.Run("ListStream tail", func(t *testing.T) {
		plugin := newStoragePlugin(t, &fakeStore{resources: []*integration.StorageResourceEntry{{}}})

		stream := &fakeStream[storagepb.ListRequest, storagepb.ListStreamResponse]{
			ctx:  t.Context(),
			send: func(*storagepb.ListStreamResponse) error { return boom },
		}
		require.ErrorIs(t, plugin.ListStream(&storagepb.ListRequest{}, stream), boom)
	})

	t.Run("Put without a header", func(t *testing.T) {
		plugin := newStoragePlugin(t, &fakeStore{})
		stream := &fakeStream[storagepb.PutRequest, storagepb.PutResponse]{
			ctx:  t.Context(),
			recv: scripted[storagepb.PutRequest](boom),
		}
		require.ErrorIs(t, plugin.Put(stream), boom)
	})

	t.Run("Get send", func(t *testing.T) {
		plugin := newStoragePlugin(t, &fakeStore{data: []byte("x")})
		stream := &fakeStream[storagepb.GetRequest, storagepb.GetResponse]{
			ctx:  t.Context(),
			send: func(*storagepb.GetResponse) error { return boom },
		}
		require.ErrorIs(t, plugin.Get(&storagepb.GetRequest{Mac: make([]byte, 32)}, stream), boom)
	})
}
