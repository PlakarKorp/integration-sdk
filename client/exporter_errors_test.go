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
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type exportClientStream = fakeClientStream[proto.ExportRequest, proto.ExportResponse]

type putDataClientStream = fakeClientStream[proto.PutDataRequest, proto.PutDataResponse]

// fakeExporterClient hands out the streams the test makes.
type fakeExporterClient struct {
	proto.ExporterClient
	export  func() (grpc.BidiStreamingClient[proto.ExportRequest, proto.ExportResponse], error)
	putData func() (grpc.ClientStreamingClient[proto.PutDataRequest, proto.PutDataResponse], error)
}

func (c *fakeExporterClient) Export(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[proto.ExportRequest, proto.ExportResponse], error) {
	return c.export()
}

func (c *fakeExporterClient) PutData(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[proto.PutDataRequest, proto.PutDataResponse], error) {
	return c.putData()
}

func TestIgnoreEOF(t *testing.T) {
	boom := errors.New("boom")
	require.NoError(t, ignoreEOF(nil))
	require.NoError(t, ignoreEOF(io.EOF))
	require.ErrorIs(t, ignoreEOF(boom), boom)
}

func TestExporter_InitFailure(t *testing.T) {
	_, err := NewExporter(t.Context(), failingConn{status.Error(codes.Internal, "nope")}, &integration.Options{}, "p", nil)
	require.EqualError(t, err, "nope")
}

func TestExporter_PingCloseFlags(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{})
	require.Zero(t, exp.Flags())
	require.NoError(t, exp.Ping(t.Context()))
	require.NoError(t, exp.Close(t.Context()))
}

func TestExporter_ExportStreamFailure(t *testing.T) {
	boom := errors.New("boom")
	g := &Exporter{client: proto.NewExporterClient(failingConn{boom})}
	err := g.Export(t.Context(), recordsOf[integration.Record](), make(chan *integration.Result))
	require.ErrorIs(t, err, boom)
}

func TestExporter_TransmitRecordsFailures(t *testing.T) {
	boom := errors.New("boom")
	g := &Exporter{}

	// the stream dying under us is io.EOF, which is not ours to report
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"with EOF", io.EOF, nil},
		{"with an error", boom, boom},
	} {
		t.Run("header "+tc.name, func(t *testing.T) {
			rd := &trackingReader{Reader: strings.NewReader("data")}
			rec := exportableRecord()
			rec.Reader = rd
			stream := &exportClientStream{send: sendScript[proto.ExportRequest](tc.err)}
			require.ErrorIs(t, g.transmitRecords(stream, recordsOf(rec, exportableRecord())), tc.want)
			require.True(t, rd.closed.Load(), "the record was left open")
		})
		t.Run("chunk "+tc.name, func(t *testing.T) {
			stream := &exportClientStream{send: sendScript[proto.ExportRequest](nil, tc.err)}
			require.ErrorIs(t, g.transmitRecords(stream, recordsOf(exportableRecord())), tc.want)
		})
		t.Run("terminator "+tc.name, func(t *testing.T) {
			stream := &exportClientStream{send: sendScript[proto.ExportRequest](nil, nil, tc.err)}
			require.ErrorIs(t, g.transmitRecords(stream, recordsOf(exportableRecord())), tc.want)
		})
	}

	t.Run("header of a record without payload", func(t *testing.T) {
		dir := &integration.Record{Pathname: "/d", FileInfo: integration.FileInfo{Lname: "d", Lmode: 0o755 | os.ModeDir}}
		stream := &exportClientStream{send: sendScript[proto.ExportRequest](boom)}
		require.ErrorIs(t, g.transmitRecords(stream, recordsOf(dir)), boom)
	})

	t.Run("payload read fails", func(t *testing.T) {
		rec := recordOf("f", []byte("data"), boom)
		stream := &exportClientStream{}
		require.ErrorIs(t, g.transmitRecords(stream, recordsOf(rec)), boom)
	})
}

func TestExporter_TransmitEntriesFailures(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")
	g := &Exporter{maxConcurrency: 1}

	t.Run("close send", func(t *testing.T) {
		stream := &exportClientStream{closeSend: func() error { return boom }}
		require.ErrorIs(t, g.transmitEntries(ctx, stream, recordsOf[integration.Record]()), boom)

		stream = &exportClientStream{closeSend: func() error { return io.EOF }}
		require.NoError(t, g.transmitEntries(ctx, stream, recordsOf[integration.Record]()))
	})

	big := bytes.Repeat([]byte("x"), inlineThreshold*2)
	for _, tc := range []struct {
		name string
		rec  *integration.Record
	}{
		{"without payload", &integration.Record{Pathname: "/d", FileInfo: integration.FileInfo{Lname: "d", Lmode: 0o755 | os.ModeDir}}},
		{"inline", recordOf("small", []byte("data"), nil)},
		{"streamed", recordOf("big", big, nil)},
	} {
		t.Run("entry send "+tc.name, func(t *testing.T) {
			var rd *trackingReader
			if tc.rec.Reader != nil {
				rd = &trackingReader{Reader: tc.rec.Reader}
				tc.rec.Reader = rd
			}
			stream := &exportClientStream{send: sendScript[proto.ExportRequest](boom)}
			require.ErrorIs(t, g.transmitEntries(ctx, stream, recordsOf(tc.rec)), boom)
			if rd != nil {
				require.True(t, rd.closed.Load(), "the record was left open")
			}
		})
	}
}

// An xattr has no size to go by: the probe reads a full inline buffer
// and finds more, so the payload goes on a PutData stream, the probed
// bytes first.
func TestExporter_PayloadWithoutSizeIsProbed(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), inlineThreshold+10)

	var (
		sent bytes.Buffer
		ids  []uint64
	)
	client := &fakeExporterClient{
		putData: func() (grpc.ClientStreamingClient[proto.PutDataRequest, proto.PutDataResponse], error) {
			return &putDataClientStream{
				send: func(req *proto.PutDataRequest) error {
					switch p := req.Packet.(type) {
					case *proto.PutDataRequest_Id:
						ids = append(ids, p.Id)
					case *proto.PutDataRequest_Chunk:
						sent.Write(p.Chunk)
					}
					return nil
				},
				closeAndRecv: func() (*proto.PutDataResponse, error) { return &proto.PutDataResponse{}, nil },
			}, nil
		},
	}
	g := &Exporter{client: client, maxConcurrency: 1}

	var entries []*proto.ExportEntry
	stream := &exportClientStream{send: func(req *proto.ExportRequest) error {
		entries = append(entries, req.GetEntry())
		return nil
	}}
	xattr := &integration.Record{
		Pathname:  "/f",
		IsXattr:   true,
		XattrName: "user.x",
		Reader:    io.NopCloser(bytes.NewReader(payload)),
	}
	require.NoError(t, g.transmitEntries(t.Context(), stream, recordsOf(xattr)))

	require.Len(t, entries, 1)
	require.NotZero(t, entries[0].Id, "the payload did not go on its own stream")
	require.Nil(t, entries[0].Chunk)
	require.Equal(t, []uint64{entries[0].Id}, ids)
	require.True(t, bytes.Equal(payload, sent.Bytes()), "payload: got %d bytes, want %d", sent.Len(), len(payload))
}

func TestExporter_PutDataFailures(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")
	closeErr := errors.New("close failed")

	streamOf := func(sends []error, res *proto.PutDataResponse, err error) *putDataClientStream {
		return &putDataClientStream{
			send:         sendScript[proto.PutDataRequest](sends...),
			closeAndRecv: func() (*proto.PutDataResponse, error) { return res, err },
		}
	}
	jobOf := func(payload string, buf []byte, rdErr error) payloadJob {
		var rd io.Reader = strings.NewReader(payload)
		if rdErr != nil {
			rd = io.MultiReader(rd, errReader{rdErr})
		}
		return payloadJob{id: 1, buf: buf, rd: io.NopCloser(rd)}
	}

	for _, tc := range []struct {
		name   string
		stream *putDataClientStream
		job    payloadJob
		want   error
	}{
		{"id send", streamOf([]error{boom}, nil, closeErr), jobOf("x", nil, nil), closeErr},
		{"probed bytes send", streamOf([]error{nil, boom}, &proto.PutDataResponse{}, nil), jobOf("x", []byte("abc"), nil), errStoppedReading},
		{"chunk send", streamOf([]error{nil, io.EOF}, &proto.PutDataResponse{}, nil), jobOf("x", nil, nil), errStoppedReading},
		{"chunk send, aborted", streamOf([]error{nil, io.EOF}, &proto.PutDataResponse{Aborted: true}, nil), jobOf("x", nil, nil), nil},
		{"read failure report", streamOf([]error{nil, nil, boom}, nil, closeErr), jobOf("x", nil, errors.New("disk-on-fire")), closeErr},
		{"close", streamOf(nil, nil, closeErr), jobOf("x", nil, nil), closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := &Exporter{client: &fakeExporterClient{
				putData: func() (grpc.ClientStreamingClient[proto.PutDataRequest, proto.PutDataResponse], error) {
					return tc.stream, nil
				},
			}}
			err := g.putData(ctx, tc.job)
			if tc.want == errStoppedReading {
				require.ErrorContains(t, err, "stopped reading payload 1")
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}

	t.Run("stream cannot be opened", func(t *testing.T) {
		g := &Exporter{client: &fakeExporterClient{
			putData: func() (grpc.ClientStreamingClient[proto.PutDataRequest, proto.PutDataResponse], error) {
				return nil, boom
			},
		}}
		require.ErrorIs(t, g.putData(ctx, jobOf("x", nil, nil)), boom)
	})
}

// errStoppedReading marks the case where the integration gave up on the
// payload without saying so.
var errStoppedReading = errors.New("stopped reading")

func TestExporter_ReceiveResultsRejectsBadRecord(t *testing.T) {
	g := &Exporter{}
	stream := &exportClientStream{recv: recvScript(io.EOF, &proto.ExportResponse{Result: &conn.Result{}})}
	results := make(chan *integration.Result, 1)
	require.ErrorIs(t, g.receiveResults(stream, results), conn.ErrInvalidRecord)
	_, open := <-results
	require.False(t, open, "results left open")
}
