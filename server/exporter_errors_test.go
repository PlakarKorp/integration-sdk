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
	"net"
	"os"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flakyExporter fails Ping and Close when told to.
type flakyExporter struct {
	fakeExporter
	pingErr  error
	closeErr error
}

func (f *flakyExporter) Ping(context.Context) error  { return f.pingErr }
func (f *flakyExporter) Close(context.Context) error { return f.closeErr }

// holdingExporter takes one record, reports it, then waits to be failed.
type holdingExporter struct {
	fakeExporter
	got  chan *integration.Record
	fail chan error
}

func (h *holdingExporter) Export(_ context.Context, records <-chan *integration.Record, results chan<- *integration.Result) error {
	defer close(results)
	h.got <- <-records
	return <-h.fail
}

type exportServerStream = fakeStream[proto.ExportRequest, proto.ExportResponse]

type putDataServerStream = fakeStream[proto.PutDataRequest, proto.PutDataResponse]

func newExporterPlugin(t *testing.T, exp integration.Exporter) *exporterPluginServer {
	t.Helper()
	plugin := &exporterPluginServer{
		constructor: func(context.Context, *integration.Options, string, map[string]string) (integration.Exporter, error) {
			return exp, nil
		},
	}
	_, err := plugin.Init(t.Context(), &proto.InitRequest{Options: &proto.Options{Maxconcurrency: 4}})
	require.NoError(t, err)
	return plugin
}

// runExport runs one Export session over stream and returns its error.
func runExport(t *testing.T, plugin *exporterPluginServer, stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse]) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- plugin.Export(stream) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for Export to return")
		return nil
	}
}

func legacyPacket(record *conn.Record) *proto.ExportRequest {
	return &proto.ExportRequest{Packet: &proto.ExportRequest_Record{Record: record}}
}

func entryPacket(entry *proto.ExportEntry) *proto.ExportRequest {
	return &proto.ExportRequest{Packet: &proto.ExportRequest_Entry{Entry: entry}}
}

func chunkPacket(chunk []byte) *proto.ExportRequest {
	return &proto.ExportRequest{Packet: &proto.ExportRequest_Chunk{Chunk: chunk}}
}

func putDataID(id uint64) *proto.PutDataRequest {
	return &proto.PutDataRequest{Packet: &proto.PutDataRequest_Id{Id: id}}
}

func putDataChunk(chunk string) *proto.PutDataRequest {
	return &proto.PutDataRequest{Packet: &proto.PutDataRequest_Chunk{Chunk: []byte(chunk)}}
}

func TestExporterServerInitErrors(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")
	plugin := &exporterPluginServer{
		constructor: func(context.Context, *integration.Options, string, map[string]string) (integration.Exporter, error) {
			return nil, boom
		},
	}

	_, err := plugin.Init(ctx, &proto.InitRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "missing options")

	_, err = plugin.Init(ctx, &proto.InitRequest{Options: &proto.Options{}})
	require.ErrorIs(t, err, boom)

	// a failed constructor is final
	_, err = plugin.Init(ctx, &proto.InitRequest{Options: &proto.Options{}})
	require.ErrorIs(t, err, ErrAlreadyInitialized)
}

func TestExporterServerPingAndClose(t *testing.T) {
	ctx := t.Context()
	boom := errors.New("boom")

	plugin := &exporterPluginServer{}
	_, err := plugin.Ping(ctx, &proto.PingRequest{})
	require.ErrorIs(t, err, ErrNotInitialized)
	_, err = plugin.Close(ctx, &proto.CloseRequest{})
	require.ErrorIs(t, err, ErrNotInitialized)
	require.ErrorIs(t, plugin.Export(&exportServerStream{ctx: ctx}), ErrNotInitialized)
	require.ErrorIs(t, plugin.PutData(&putDataServerStream{ctx: ctx}), ErrNotInitialized)

	exp := &flakyExporter{}
	plugin = newExporterPlugin(t, exp)
	_, err = plugin.Ping(ctx, &proto.PingRequest{})
	require.NoError(t, err)

	exp.pingErr = boom
	_, err = plugin.Ping(ctx, &proto.PingRequest{})
	require.ErrorIs(t, err, boom)

	exp.closeErr = boom
	_, err = plugin.Close(ctx, &proto.CloseRequest{})
	require.ErrorIs(t, err, boom)
}

// The legacy payload reader reports the stream ending or breaking under
// it, whether it is being read or drained.
func TestExporterServerLegacyReaderErrors(t *testing.T) {
	boom := errors.New("boom")

	for name, tc := range map[string]struct {
		reqs  []*proto.ExportRequest
		final error
		want  string
	}{
		"stream ends":  {nil, io.EOF, io.ErrUnexpectedEOF.Error()},
		"stream fails": {nil, boom, "boom"},
		"not a chunk":  {[]*proto.ExportRequest{{}}, io.EOF, "unexpected response"},
	} {
		t.Run("read "+name, func(t *testing.T) {
			stream := &exportServerStream{ctx: t.Context(), recv: scripted(tc.final, tc.reqs...)}
			rd := open(stream, make(chan struct{}))
			_, err := rd.Read(make([]byte, 1))
			require.ErrorContains(t, err, tc.want)
		})
		t.Run("close "+name, func(t *testing.T) {
			stream := &exportServerStream{ctx: t.Context(), recv: scripted(tc.final, tc.reqs...)}
			end := make(chan struct{})
			rd := open(stream, end)
			err := rd.Close()
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, err, rd.Close(), "second Close")
			_, open := <-end
			require.False(t, open, "the record was not released")
		})
	}

	t.Run("close drains to the terminator", func(t *testing.T) {
		stream := &exportServerStream{ctx: t.Context(), recv: scripted(io.EOF, chunkPacket([]byte("x")), chunkPacket([]byte{}))}
		rd := open(stream, make(chan struct{}))
		require.NoError(t, rd.Close())
	})
}

func TestExporterServerReceiveErrors(t *testing.T) {
	boom := errors.New("boom")

	for _, tc := range []struct {
		name string
		req  *proto.ExportRequest
		want string
	}{
		{"unknown packet", &proto.ExportRequest{}, "bad type"},
		{"legacy record without fileinfo", legacyPacket(&conn.Record{Pathname: "/x"}), "missing fileinfo"},
		{"entry without a record", entryPacket(&proto.ExportEntry{}), "entry without a record"},
		{"entry without fileinfo", entryPacket(&proto.ExportEntry{Record: &conn.Record{Pathname: "/x"}}), "missing fileinfo"},
		{"payload both inline and streamed", entryPacket(&proto.ExportEntry{Record: recordOf("/x", 1), Chunk: []byte("x"), Id: 1}), "both inline and on a stream"},
		{"payload announced but not sent", entryPacket(&proto.ExportEntry{Record: recordOf("/x", 1)}), "announced but not sent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := newExporterPlugin(t, &fakeExporter{})
			stream := &exportServerStream{ctx: t.Context(), recv: scripted(io.EOF, tc.req)}
			require.ErrorContains(t, runExport(t, plugin, stream), tc.want)
		})
	}

	t.Run("payload after the session ended", func(t *testing.T) {
		plugin := newExporterPlugin(t, &fakeExporter{})
		plugin.inflight.fail(boom)
		entry := entryOf("/x", 1)
		entry.Id = 1
		stream := &exportServerStream{ctx: t.Context(), recv: scripted(io.EOF, entryPacket(entry))}
		require.ErrorIs(t, runExport(t, plugin, stream), boom)
	})

	t.Run("sending a result", func(t *testing.T) {
		plugin := newExporterPlugin(t, &fakeExporter{})
		entry := entryOf("/x", 0)
		entry.Chunk = []byte{}
		stream := &exportServerStream{
			ctx:  t.Context(),
			recv: scripted(io.EOF, entryPacket(entry)),
			send: func(*proto.ExportResponse) error { return boom },
		}
		require.ErrorIs(t, runExport(t, plugin, stream), boom)
	})
}

// A legacy record without a payload, a directory, is handed out at once.
func TestExporterServerLegacyRecordWithoutPayload(t *testing.T) {
	exp := &fakeExporter{}
	plugin := newExporterPlugin(t, exp)

	dir := conn.RecordToProto(&integration.Record{
		Pathname: "/d",
		FileInfo: integration.FileInfo{Lname: "d", Lmode: 0o755 | os.ModeDir},
	})
	stream := &exportServerStream{ctx: t.Context(), recv: scripted(io.EOF, legacyPacket(dir))}
	require.NoError(t, runExport(t, plugin, stream))

	got := exp.records()
	require.Len(t, got, 1)
	require.Equal(t, "/d", got[0].pathname)
	require.Nil(t, got[0].payload)
}

// Under the legacy protocol the plugin waits for the exporter to be
// done with a payload before taking the next record; the exporter
// failing instead must not leave it waiting.
func TestExporterServerLegacyPayloadAbandonedOnFailure(t *testing.T) {
	boom := errors.New("boom")
	exp := &holdingExporter{got: make(chan *integration.Record, 1), fail: make(chan error)}
	plugin := newExporterPlugin(t, exp)

	stream := &exportStream{ctx: t.Context(), in: make(chan *proto.ExportRequest)}
	exported := make(chan error, 1)
	go func() { exported <- plugin.Export(stream) }()

	stream.in <- legacyPacket(recordOf("/f", 4))
	select {
	case <-exp.got:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for the exporter to get the record")
	}

	exp.fail <- boom
	select {
	case err := <-exported:
		require.ErrorIs(t, err, boom)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the plugin is still waiting on a payload nobody will read")
	}
}

func TestExporterServerPutDataErrors(t *testing.T) {
	boom := errors.New("boom")

	for name, tc := range map[string]struct {
		recv func() (*proto.PutDataRequest, error)
		want string
	}{
		"no id":                 {scripted[proto.PutDataRequest](io.EOF), "payload without an id"},
		"failure before the id": {scripted[proto.PutDataRequest](boom), "boom"},
		"chunk before the id":   {scripted(io.EOF, putDataChunk("x")), "expected an ID"},
		"failure after the id":  {scripted(boom, putDataID(1)), "boom"},
		"unknown packet":        {scripted(io.EOF, putDataID(1), &proto.PutDataRequest{}), "expected data or error"},
	} {
		t.Run(name, func(t *testing.T) {
			plugin := newExporterPlugin(t, &fakeExporter{})
			err := plugin.PutData(&putDataServerStream{ctx: t.Context(), recv: tc.recv})
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("after the session ended", func(t *testing.T) {
		plugin := newExporterPlugin(t, &fakeExporter{})
		plugin.inflight.fail(boom)
		err := plugin.PutData(&putDataServerStream{ctx: t.Context(), recv: scripted(io.EOF, putDataID(1))})
		require.ErrorIs(t, err, boom)
	})

	// Closing the plugin while a payload is being written fails the
	// write with the session error, which is not the exporter giving up
	// on that record.
	t.Run("session closed while writing", func(t *testing.T) {
		plugin := newExporterPlugin(t, &fakeExporter{})

		claimed := make(chan struct{})
		released := make(chan struct{})
		calls := 0
		stream := &putDataServerStream{ctx: t.Context(), recv: func() (*proto.PutDataRequest, error) {
			calls++
			switch calls {
			case 1:
				return putDataID(1), nil
			case 2:
				close(claimed)
				<-released
				return putDataChunk("x"), nil
			default:
				return nil, io.EOF
			}
		}}

		done := make(chan error, 1)
		go func() { done <- plugin.PutData(stream) }()

		<-claimed
		_, err := plugin.Close(t.Context(), &proto.CloseRequest{})
		require.NoError(t, err)
		close(released)

		select {
		case err := <-done:
			require.ErrorIs(t, err, ErrExporterClosed)
		case <-time.After(5 * time.Second):
			require.Fail(t, "PutData is still blocked on a payload nobody will read")
		}
	})
}

func TestRunExporterOn(t *testing.T) {
	c := serveOn(t, func(lis net.Listener) error {
		return RunExporterOn(func(context.Context, *integration.Options, string, map[string]string) (integration.Exporter, error) {
			return &fakeExporter{}, nil
		}, lis)
	})

	_, err := proto.NewExporterClient(c).Init(t.Context(), &proto.InitRequest{Options: &proto.Options{}})
	require.NoError(t, err)
}
