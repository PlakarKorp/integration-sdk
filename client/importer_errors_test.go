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
	"io"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/importer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type importClientStream = fakeClientStream[proto.ImportRequest, proto.ImportResponse]

func TestImporter_StreamFailures(t *testing.T) {
	boom := errors.New("boom")
	g := &Importer{client: proto.NewImporterClient(failingConn{boom})}

	err := g.Import(t.Context(), make(chan *integration.Record), make(chan *integration.Result))
	require.ErrorIs(t, err, boom)

	_, err = g.open(t.Context(), &integration.Record{Pathname: "/x"})
	require.ErrorIs(t, err, boom)
}

func TestImporter_ReceiveRecordsFailures(t *testing.T) {
	boom := errors.New("boom")

	for name, tc := range map[string]struct {
		recv func() (*proto.ImportResponse, error)
		want error
		msg  string
	}{
		"stream ends":    {recvScript[proto.ImportResponse](io.EOF), nil, ""},
		"stream fails":   {recvScript[proto.ImportResponse](boom), boom, ""},
		"missing record": {recvScript(io.EOF, &proto.ImportResponse{}), nil, "expected a record"},
		"bad record":     {recvScript(io.EOF, &proto.ImportResponse{Record: &conn.Record{Pathname: "/x"}}), conn.ErrInvalidRecord, ""},
	} {
		t.Run(name, func(t *testing.T) {
			g := &Importer{}
			records := make(chan *integration.Record, 1)
			err := g.receiveRecords(t.Context(), &importClientStream{recv: tc.recv}, records)
			if tc.msg != "" {
				require.ErrorContains(t, err, tc.msg)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			_, open := <-records
			require.False(t, open, "records left open")
		})
	}
}

// The results still coming when the stream dies are drained, or the
// producer would hang.
func TestImporter_SendResultsFailure(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"with EOF", io.EOF, nil},
		{"with an error", boom, boom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &Importer{}
			results := make(chan *integration.Result, 2)
			results <- &integration.Result{}
			results <- &integration.Result{}
			close(results)

			stream := &importClientStream{send: sendScript[proto.ImportRequest](tc.err)}
			require.ErrorIs(t, g.sendResults(stream, results), tc.want)
			_, open := <-results
			require.False(t, open, "the results were not drained")
		})
	}
}

func TestImporter_BadRecordsFailImport(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record *conn.Record
		want   string
	}{
		{"missing", nil, "expected a record"},
		{"without fileinfo", &conn.Record{Pathname: "/x"}, "missing fileinfo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fakeImporter{
				initResp: &proto.InitResponse{},
				records:  []*conn.Record{tc.record},
			}
			c, cleanup := dialImporter(t, srv)
			defer cleanup()

			imp, err := NewImporter(t.Context(), c, &integration.Options{}, "p", nil)
			require.NoError(t, err)

			err, seen := runImport(t, imp.(*Importer))
			require.ErrorContains(t, err, tc.want)
			require.Empty(t, seen)
		})
	}
}

// A payload is read a few bytes at a time, across the chunks it came in.
func TestImporter_StreamReaderBuffers(t *testing.T) {
	srv := &fakeImporter{
		initResp: &proto.InitResponse{},
		openChunks: map[string][][]byte{
			"/a":     {[]byte("hello"), []byte("world")},
			"/empty": {[]byte{}},
		},
	}
	c, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), c, &integration.Options{}, "p", nil)
	require.NoError(t, err)
	gi := imp.(*Importer)

	rd, err := gi.open(t.Context(), &integration.Record{Pathname: "/a"})
	require.NoError(t, err)
	var got []byte
	buf := make([]byte, 2)
	for {
		n, err := rd.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, "helloworld", string(got))
	require.NoError(t, rd.Close())

	// an empty chunk is not a payload
	rd, err = gi.open(t.Context(), &integration.Record{Pathname: "/empty"})
	require.NoError(t, err)
	_, err = rd.Read(buf)
	require.ErrorContains(t, err, "unexpected response")
	require.NoError(t, rd.Close())
}

// blockingOpenServer never answers an Open until the stream is gone.
type blockingOpenServer struct {
	proto.UnimplementedImporterServer
}

func (blockingOpenServer) Init(context.Context, *proto.InitRequest) (*proto.InitResponse, error) {
	return &proto.InitResponse{}, nil
}

func (blockingOpenServer) Open(_ *proto.OpenRequest, stream grpc.ServerStreamingServer[proto.OpenResponse]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// Closing a reader whose session was cancelled from above reports the
// cancellation: it is not the reader hanging up early.
func TestImporter_CloseReportsOuterCancellation(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	proto.RegisterImporterServer(s, blockingOpenServer{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	c, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	imp, err := NewImporter(t.Context(), c, &integration.Options{}, "p", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	rd, err := imp.(*Importer).open(ctx, &integration.Record{Pathname: "/x"})
	require.NoError(t, err)

	cancel()
	require.ErrorIs(t, rd.Close(), context.Canceled)
}
