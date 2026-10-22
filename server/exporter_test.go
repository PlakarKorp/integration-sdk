package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// A fake exporter, standing in for the plugin's own implementation
// ---------------------------------------------------------------------------

type gotRecord struct {
	pathname string
	payload  []byte
	err      error
}

type fakeExporter struct {
	mu  sync.Mutex
	got []gotRecord

	// readLimit, when positive, makes the exporter close a payload
	// after that many bytes instead of reading it to the end.
	readLimit int

	// closeEarly makes the exporter close the payload itself before
	// acking the record, which closes it once more.
	closeEarly bool

	// reading and seen, when set, report a record about to be read
	// and one that has been.
	reading chan struct{}
	seen    chan gotRecord
}

func (f *fakeExporter) Origin() string              { return "o" }
func (f *fakeExporter) Type() string                { return "t" }
func (f *fakeExporter) Root() string                { return "/" }
func (f *fakeExporter) Flags() location.Flags       { return 0 }
func (f *fakeExporter) Ping(context.Context) error  { return nil }
func (f *fakeExporter) Close(context.Context) error { return nil }

func (f *fakeExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	var wg sync.WaitGroup
	for record := range records {
		wg.Go(func() {
			results <- f.consume(record)
		})
	}
	wg.Wait()

	return nil
}

func (f *fakeExporter) consume(record *connectors.Record) *connectors.Result {
	got := gotRecord{pathname: record.Pathname}

	if f.reading != nil {
		f.reading <- struct{}{}
	}

	if record.Reader != nil {
		var rd io.Reader = record.Reader
		if f.readLimit > 0 {
			rd = io.LimitReader(rd, int64(f.readLimit))
		}
		got.payload, got.err = io.ReadAll(rd)
		if f.closeEarly {
			record.Reader.Close()
		}
	}

	f.mu.Lock()
	f.got = append(f.got, got)
	f.mu.Unlock()

	if f.seen != nil {
		f.seen <- got
	}

	return record.Ok()
}

func (f *fakeExporter) records() []gotRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]gotRecord(nil), f.got...)
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialPlugin(t *testing.T, exp exporter.Exporter) proto.ExporterClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	proto.RegisterExporterServer(srv, &exporterPluginServer{
		constructor: func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return exp, nil
		},
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return dialOn(t, lis)
}

// dialOn connects to a plugin already listening on lis and inits it.
func dialOn(t *testing.T, lis *bufconn.Listener) proto.ExporterClient {
	t.Helper()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := proto.NewExporterClient(conn)
	_, err = client.Init(t.Context(), &proto.InitRequest{
		Options: &proto.Options{Maxconcurrency: 4},
		Proto:   "p",
	})
	require.NoError(t, err)

	return client
}

func recordOf(pathname string, size int64) *conn.Record {
	return conn.RecordToProto(&connectors.Record{
		Pathname: pathname,
		FileInfo: objects.FileInfo{Lname: pathname, Lsize: size, Lmode: 0o644},
		// RecordToProto only looks at whether this is set
		Reader: io.NopCloser(strings.NewReader("")),
	})
}

func entryOf(pathname string, size int64) *proto.ExportEntry {
	return &proto.ExportEntry{Record: recordOf(pathname, size)}
}

// putData uploads one payload the way the client does.
// It runs on helper goroutines too, so it reports with assert and returns
// nil on failure instead of stopping the test.
func putData(t *testing.T, client proto.ExporterClient, id uint64, body []byte, readErr error) *proto.PutDataResponse {
	t.Helper()

	stream, err := client.PutData(t.Context())
	if !assert.NoError(t, err, "PutData") {
		return nil
	}

	// the exporter may hang up at any point, which ends the stream
	// with io.EOF and is answered by the response, not by the send.
	send := func(req *proto.PutDataRequest) bool {
		err := stream.Send(req)
		if err != nil && !errors.Is(err, io.EOF) {
			assert.NoError(t, err, "send")
		}
		return err == nil
	}

	send(&proto.PutDataRequest{
		Packet: &proto.PutDataRequest_Id{Id: id},
	})

	if readErr != nil {
		send(&proto.PutDataRequest{
			Packet: &proto.PutDataRequest_Error{Error: readErr.Error()},
		})
	} else {
		for chunk := range chunked(body, 4096) {
			pkt := &proto.PutDataRequest_Chunk{Chunk: chunk}
			if !send(&proto.PutDataRequest{Packet: pkt}) {
				break
			}
		}
	}

	res, err := stream.CloseAndRecv()
	assert.NoError(t, err, "CloseAndRecv")

	return res
}

// chunked yields body one message at a time, the way a client sends it.
func chunked(body []byte, size int) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for len(body) > size {
			if !yield(body[:size]) {
				return
			}
			body = body[size:]
		}
		yield(body)
	}
}

// exportEntries runs a whole Export session, sending entries and
// uploading the bodies the dispatch function hands back.
func exportEntries(t *testing.T, client proto.ExporterClient, entries []*proto.ExportEntry, upload func()) {
	t.Helper()

	stream, err := client.Export(t.Context())
	require.NoError(t, err)

	results := make(chan int, 1)
	go func() {
		n := 0
		for {
			if _, err := stream.Recv(); err != nil {
				results <- n
				return
			}
			n++
		}
	}()

	for _, entry := range entries {
		err := stream.Send(&proto.ExportRequest{
			Packet: &proto.ExportRequest_Entry{Entry: entry},
		})
		require.NoError(t, err, "send entry")
	}

	if upload != nil {
		upload()
	}

	require.NoError(t, stream.CloseSend())

	select {
	case <-results:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for the export to finish")
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestExporterServerInlinePayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("data")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := &fakeExporter{}
			client := dialPlugin(t, exp)

			entry := entryOf("/f", int64(len(tc.body)))
			entry.Chunk = tc.body

			exportEntries(t, client, []*proto.ExportEntry{entry}, nil)

			got := exp.records()
			require.Len(t, got, 1)
			require.Equal(t, tc.body, got[0].payload)
		})
	}
}

// The record and its body travel on different streams, and gRPC does
// not order them: both arrival orders have to work.
func TestExporterServerStreamedPayload(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 128*1024)

	for _, tc := range []struct {
		name        string
		uploadFirst bool
	}{
		{"entry first", false},
		{"payload first", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := &fakeExporter{}
			client := dialPlugin(t, exp)

			entry := entryOf("/f", int64(len(body)))
			entry.Id = 1

			upload := func() { putData(t, client, 1, body, nil) }
			if tc.uploadFirst {
				done := make(chan struct{})
				go func() {
					defer close(done)
					putData(t, client, 1, body, nil)
				}()
				// the upload blocks until the record shows up
				exportEntries(t, client, []*proto.ExportEntry{entry}, nil)
				<-done
			} else {
				exportEntries(t, client, []*proto.ExportEntry{entry}, upload)
			}

			got := exp.records()
			require.Len(t, got, 1)
			require.NoError(t, got[0].err)
			require.True(t, bytes.Equal(body, got[0].payload), "payload: got %d bytes, want %d", len(got[0].payload), len(body))
		})
	}
}

// A payload the client could not read reaches the exporter as a read
// error on that record, and that record only.
func TestExporterServerPayloadError(t *testing.T) {
	exp := &fakeExporter{}
	client := dialPlugin(t, exp)

	entry := entryOf("/f", 4096)
	entry.Id = 1

	exportEntries(t, client, []*proto.ExportEntry{entry}, func() {
		putData(t, client, 1, nil, errors.New("boom"))
	})

	got := exp.records()
	require.Len(t, got, 1)
	require.ErrorContains(t, got[0].err, "boom")
}

// An exporter that stops reading a record early says so through the
// response, so that the client can tell it from a broken stream.
func TestExporterServerAbortedPayload(t *testing.T) {
	exp := &fakeExporter{readLimit: 16}
	client := dialPlugin(t, exp)

	entry := entryOf("/f", 1<<20)
	entry.Id = 1

	var res *proto.PutDataResponse
	exportEntries(t, client, []*proto.ExportEntry{entry}, func() {
		res = putData(t, client, 1, bytes.Repeat([]byte("x"), 1<<20), nil)
	})

	require.NotNil(t, res)
	require.True(t, res.Aborted, "PutData response %v, want aborted", res)
}

// The whole point: the receive goroutine no longer waits for one
// payload before taking the next record.
func TestExporterServerRecordsDoNotWaitOnPayloads(t *testing.T) {
	const count = 4

	exp := &fakeExporter{}
	client := dialPlugin(t, exp)

	body := bytes.Repeat([]byte("x"), 64*1024)

	entries := make([]*proto.ExportEntry, count)
	for i := range entries {
		entries[i] = entryOf("/f", int64(len(body)))
		entries[i].Id = uint64(i + 1)
	}

	// Every entry is sent before any payload is uploaded, which the
	// legacy protocol could not do: it would deadlock on the first.
	exportEntries(t, client, entries, func() {
		var wg sync.WaitGroup
		for i := range entries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				putData(t, client, uint64(i+1), body, nil)
			}()
		}
		wg.Wait()
	})

	got := exp.records()
	require.Len(t, got, count)
	for i, rec := range got {
		require.NoError(t, rec.err, "record %d", i)
		require.True(t, bytes.Equal(body, rec.payload), "record %d: payload is %d bytes, want %d", i, len(rec.payload), len(body))
	}
}

// The old protocol, where the payload follows the record on the Export
// stream itself, has to keep working untouched.
func TestExporterServerLegacyProtocol(t *testing.T) {
	for _, tc := range []struct {
		name string
		exp  *fakeExporter
	}{
		{"read to the end", &fakeExporter{}},
		{"closed before the ack", &fakeExporter{closeEarly: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyExport(t, tc.exp)
		})
	}
}

// legacyExport runs an Export session with the old protocol and checks
// the exporter got every record and payload.
func legacyExport(t *testing.T, exp *fakeExporter) {
	t.Helper()

	body := bytes.Repeat([]byte("x"), 32*1024)

	client := dialPlugin(t, exp)

	stream, err := client.Export(t.Context())
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()

	send := func(packet *proto.ExportRequest) {
		t.Helper()
		require.NoError(t, stream.Send(packet))
	}

	for _, pathname := range []string{"/a", "/b"} {
		send(&proto.ExportRequest{
			Packet: &proto.ExportRequest_Record{
				Record: recordOf(pathname, int64(len(body))),
			},
		})
		for chunk := range chunked(body, 4096) {
			pkt := &proto.ExportRequest_Chunk{Chunk: chunk}
			send(&proto.ExportRequest{Packet: pkt})
		}
		// the terminator telling the reader it reached the end
		send(&proto.ExportRequest{
			Packet: &proto.ExportRequest_Chunk{Chunk: nil},
		})
	}

	require.NoError(t, stream.CloseSend())

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for the export to finish")
	}

	got := exp.records()
	require.Len(t, got, 2)
	for i, rec := range got {
		require.NoError(t, rec.err, "record %d", i)
		require.True(t, bytes.Equal(body, rec.payload), "record %d: payload is %d bytes, want %d", i, len(rec.payload), len(body))
	}
}

// the exporter should terminate on Close even if it is holding a
// payload that was never picked up.
func TestExporterServerReleasesPayloadsOnClose(t *testing.T) {
	exp := &fakeExporter{
		reading: make(chan struct{}, 1),
		seen:    make(chan gotRecord, 1),
	}
	client := dialPlugin(t, exp)

	stream, err := client.Export(t.Context())
	require.NoError(t, err)

	entry := entryOf("/f", 1<<20)
	entry.Id = 1
	err = stream.Send(&proto.ExportRequest{
		Packet: &proto.ExportRequest_Entry{Entry: entry},
	})
	require.NoError(t, err, "send entry")

	// Wait until the exporter holds the record, so that we know it
	// is about to block on a payload we are never going to upload.
	select {
	case <-exp.reading:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for the exporter to get the record")
	}

	_, err = client.Close(t.Context(), &proto.CloseRequest{})
	require.NoError(t, err)

	select {
	case got := <-exp.seen:
		require.Error(t, got.err, "the payload read succeeded, want the session error")
	case <-time.After(5 * time.Second):
		require.Fail(t, "the exporter is still blocked on a payload nobody will send")
	}
}

// a shutdown has to release an exporter parked on a payload, so that
// it gets to wind down instead of staying blocked.
func TestRunExporterOnReleasesPayloadsOnShutdown(t *testing.T) {
	exp := &fakeExporter{
		reading: make(chan struct{}, 1),
		seen:    make(chan gotRecord, 1),
	}

	lis := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(t.Context())

	served := make(chan error, 1)
	go func() {
		served <- RunExporterOn(ctx, func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return exp, nil
		}, lis)
	}()

	client := dialOn(t, lis)

	stream, err := client.Export(t.Context())
	require.NoError(t, err)

	entry := entryOf("/f", 1<<20)
	entry.Id = 1
	err = stream.Send(&proto.ExportRequest{
		Packet: &proto.ExportRequest_Entry{Entry: entry},
	})
	require.NoError(t, err, "send entry")

	select {
	case <-exp.reading:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for the exporter to get the record")
	}

	cancel()

	select {
	case got := <-exp.seen:
		require.Error(t, got.err, "the payload read succeeded, want the shutdown error")
	case <-time.After(5 * time.Second):
		require.Fail(t, "the exporter is still blocked on a payload nobody will send")
	}

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "RunExporterOn did not return")
	}
}

// the handshake is what puts a client on the entry protocol.
func TestExporterServerAnnouncesPutData(t *testing.T) {
	plugin := &exporterPluginServer{
		constructor: func(context.Context, *connectors.Options, string, map[string]string) (exporter.Exporter, error) {
			return &fakeExporter{}, nil
		},
	}

	res, err := plugin.Init(t.Context(), &proto.InitRequest{
		Options: &proto.Options{Maxconcurrency: 4},
		Proto:   "p",
	})
	require.NoError(t, err)
	require.True(t, res.HasPutData, "the handshake does not announce PutData support")
}
