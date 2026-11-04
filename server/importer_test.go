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
	"io"
	"io/fs"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/integration-sdk/location"
	conn "github.com/PlakarKorp/integration-sdk/proto"
	importerpb "github.com/PlakarKorp/integration-sdk/proto/importer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// closeTracker is a payload that remembers being closed.
type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

// fakeImporter hands out the records it was given and collects the acks.
type fakeImporter struct {
	flags   location.Flags
	records []*integration.Record

	importErr error
	pingErr   error
	closeErr  error

	acks   []*integration.Result
	closed bool
}

func (f *fakeImporter) Origin() string             { return "o" }
func (f *fakeImporter) Type() string               { return "t" }
func (f *fakeImporter) Root() string               { return "/r" }
func (f *fakeImporter) Flags() location.Flags      { return f.flags }
func (f *fakeImporter) Ping(context.Context) error { return f.pingErr }

func (f *fakeImporter) Import(_ context.Context, records chan<- *integration.Record, results <-chan *integration.Result) (map[string]string, error) {
	for _, r := range f.records {
		records <- r
	}
	close(records)

	if results != nil {
		for res := range results {
			f.acks = append(f.acks, res)
		}
	}
	return nil, f.importErr
}

func (f *fakeImporter) Close(context.Context) error {
	f.closed = true
	return f.closeErr
}

func importing(imp integration.Importer) integration.ImporterFn {
	return func(context.Context, *integration.Options, string, map[string]string) (integration.Importer, error) {
		return imp, nil
	}
}

func dialImporter(t *testing.T, ctor integration.ImporterFn) *grpc.ClientConn {
	t.Helper()
	return serveOn(t, func(lis net.Listener) error { return RunImporterOn(ctor, lis) })
}

func newImporterPlugin(t *testing.T, imp integration.Importer) *importerPluginServer {
	t.Helper()
	plugin := &importerPluginServer{constructor: importing(imp)}
	_, err := plugin.Init(t.Context(), &importerpb.InitRequest{Options: &importerpb.Options{Maxconcurrency: 4}})
	require.NoError(t, err)
	return plugin
}

type importStream = fakeStream[importerpb.ImportRequest, importerpb.ImportResponse]

type openStream = fakeStream[importerpb.OpenRequest, importerpb.OpenResponse]

// The plugin is driven through the client of the same SDK, the way
// plakar does: records flow one way, acks the other, and the payload
// the client asks for is served on its own stream.
func TestImporterServerRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags location.Flags
		acks  int
	}{
		{"with acks", location.FLAG_NEEDACK, 3},
		{"without acks", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			opened := &closeTracker{Reader: strings.NewReader("hello")}
			ignored := &closeTracker{Reader: strings.NewReader("never read")}
			xattr := &closeTracker{Reader: strings.NewReader("attr")}
			fake := &fakeImporter{
				flags: tc.flags,
				records: []*integration.Record{
					{Pathname: "/a", FileInfo: integration.FileInfo{Lname: "a", Lsize: 5, Lmode: 0o644}, Reader: opened},
					{Pathname: "/a", IsXattr: true, XattrName: "user.x", Reader: xattr},
					{Pathname: "/b", FileInfo: integration.FileInfo{Lname: "b", Lsize: 10, Lmode: 0o644}, Reader: ignored},
				},
			}

			var gotOpts integration.Options
			c := dialImporter(t, func(_ context.Context, opts *integration.Options, _ string, _ map[string]string) (integration.Importer, error) {
				gotOpts = *opts
				return fake, nil
			})

			imp, err := client.NewImporter(ctx, c, &integration.Options{Hostname: "h", MaxConcurrency: 4}, "fs", nil)
			require.NoError(t, err)
			require.Equal(t, "h", gotOpts.Hostname)
			require.Equal(t, 4, gotOpts.MaxConcurrency)
			require.Equal(t, "o", imp.Origin())
			require.Equal(t, "t", imp.Type())
			require.Equal(t, "/r", imp.Root())
			require.Equal(t, tc.flags|location.FLAG_NEEDACK, imp.Flags())
			require.NoError(t, imp.Ping(ctx))

			records := make(chan *integration.Record, 4)
			results := make(chan *integration.Result, 4)
			done := make(chan error, 1)
			go func() { _, err := imp.Import(ctx, records, results); done <- err }()

			var seen []string
			for rec := range records {
				seen = append(seen, rec.Pathname)
				switch {
				case rec.IsXattr:
					data, err := io.ReadAll(rec.Reader)
					require.NoError(t, err)
					require.Equal(t, "attr", string(data))
				case rec.Pathname == "/a":
					data, err := io.ReadAll(rec.Reader)
					require.NoError(t, err)
					require.Equal(t, "hello", string(data))
				}
				results <- rec.Ok()
			}
			close(results)

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.Fail(t, "timeout waiting for Import to return")
			}
			require.Equal(t, []string{"/a", "/a", "/b"}, seen)
			require.Len(t, fake.acks, tc.acks)
			require.True(t, opened.closed, "the payload that was opened is left open")
			require.True(t, xattr.closed, "the xattr that was opened is left open")
			require.True(t, ignored.closed, "the payload that was acked unread is left open")

			require.NoError(t, imp.Close(ctx))
			require.True(t, fake.closed)
		})
	}
}

func TestImporterServerImportErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	t.Run("importer", func(t *testing.T) {
		fake := &fakeImporter{importErr: boom, pingErr: boom, closeErr: boom}
		imp, err := client.NewImporter(ctx, dialImporter(t, importing(fake)), &integration.Options{}, "p", nil)
		require.NoError(t, err)
		require.ErrorContains(t, imp.Ping(ctx), "boom")

		records := make(chan *integration.Record, 4)
		results := make(chan *integration.Result, 4)
		done := make(chan error, 1)
		go func() { _, err := imp.Import(ctx, records, results); done <- err }()
		for range records {
		}
		close(results)
		require.ErrorContains(t, <-done, "boom")

		require.ErrorContains(t, imp.Close(ctx), "boom")
	})

	t.Run("constructor", func(t *testing.T) {
		c := dialImporter(t, func(context.Context, *integration.Options, string, map[string]string) (integration.Importer, error) {
			return nil, boom
		})
		_, err := client.NewImporter(ctx, c, &integration.Options{}, "p", nil)
		require.ErrorContains(t, err, "boom")
	})

	t.Run("init", func(t *testing.T) {
		plugin := newImporterPlugin(t, &fakeImporter{})
		_, err := plugin.Init(ctx, &importerpb.InitRequest{})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "missing options")
		_, err = plugin.Init(ctx, &importerpb.InitRequest{Options: &importerpb.Options{}})
		require.ErrorIs(t, err, ErrAlreadyInitialized)
	})

	t.Run("uninitialized", func(t *testing.T) {
		plugin := &importerPluginServer{}
		_, err := plugin.Ping(ctx, &importerpb.PingRequest{})
		require.ErrorIs(t, err, ErrNotInitialized)
		require.ErrorIs(t, plugin.Import(&importStream{ctx: ctx}), ErrNotInitialized)
		require.ErrorIs(t, plugin.Open(&importerpb.OpenRequest{Record: &conn.Record{}}, &openStream{ctx: ctx}), ErrNotInitialized)
		_, err = plugin.Close(ctx, &importerpb.CloseRequest{})
		require.ErrorIs(t, err, ErrNotInitialized)
	})

	t.Run("sending a record", func(t *testing.T) {
		plugin := newImporterPlugin(t, &fakeImporter{})
		rd := &closeTracker{Reader: strings.NewReader("x")}
		records := make(chan *integration.Record, 1)
		records <- &integration.Record{Pathname: "/x", Reader: rd}
		close(records)
		stream := &importStream{ctx: ctx, send: func(*importerpb.ImportResponse) error { return boom }}
		require.ErrorIs(t, plugin.sendRecords(stream, records), boom)
		require.NoError(t, plugin.readers.Close())
		require.True(t, rd.closed, "a record the client never got is left open")
	})

	t.Run("receiving results", func(t *testing.T) {
		plugin := newImporterPlugin(t, &fakeImporter{})
		for name, tc := range map[string]struct {
			recv func() (*importerpb.ImportRequest, error)
			want error
		}{
			"stream failure": {scripted[importerpb.ImportRequest](boom), boom},
			"missing result": {scripted(io.EOF, &importerpb.ImportRequest{}), conn.ErrInvalidRecord},
		} {
			t.Run(name, func(t *testing.T) {
				results := make(chan *integration.Result, 1)
				err := plugin.receiveResults(&importStream{ctx: ctx, recv: tc.recv}, results)
				require.ErrorIs(t, err, tc.want)
				_, open := <-results
				require.False(t, open, "results left open")
			})
		}
	})
}

func TestImporterServerOpenErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")
	plugin := newImporterPlugin(t, &fakeImporter{})

	err := plugin.Open(&importerpb.OpenRequest{}, &openStream{ctx: ctx})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "missing record")

	err = plugin.Open(&importerpb.OpenRequest{Record: &conn.Record{Pathname: "/nope"}}, &openStream{ctx: ctx})
	require.ErrorIs(t, err, fs.ErrNotExist)

	failing := &closeTracker{Reader: io.MultiReader(strings.NewReader("x"), errReader{boom})}
	plugin.readers.Track(&integration.Record{Pathname: "/failing", Reader: failing})
	var chunks int
	err = plugin.Open(&importerpb.OpenRequest{Record: &conn.Record{Pathname: "/failing"}}, &openStream{
		ctx:  ctx,
		send: func(*importerpb.OpenResponse) error { chunks++; return nil },
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, chunks, "what could be read was sent")
	require.True(t, failing.closed)

	plugin.readers.Track(&integration.Record{Pathname: "/x", Reader: io.NopCloser(strings.NewReader("x"))})
	err = plugin.Open(&importerpb.OpenRequest{Record: &conn.Record{Pathname: "/x"}}, &openStream{
		ctx:  ctx,
		send: func(*importerpb.OpenResponse) error { return boom },
	})
	require.ErrorIs(t, err, boom)
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
