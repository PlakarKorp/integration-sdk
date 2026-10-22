package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// Fake server
// ---------------------------------------------------------------------------

type fakeExporter struct {
	proto.UnimplementedExporterServer

	mu       sync.Mutex
	received []receivedFile

	respond []*connectors.Result

	// earlyExportErr, when set, makes the Export handler abort after its first
	// Recv() and return this error, which grpc turns into a non-OK status
	// in the stream trailer.
	earlyExportErr error

	// sendNilResult makes the Export handler emit an ExportResponse with no
	// Result at all before anything else.
	// Takes precedence over earlyExportErr
	sendNilResult bool

	// hasPutData advertises the entry protocol in Init, putting the
	// client on the PutData path.
	hasPutData bool

	// putDataErr makes every PutData handler fail with it.
	putDataErr error

	// failFirstPutData fails the first PutData stream and parks the
	// others until they are cancelled, so that the failure has
	// in-flight payloads to take down with it.
	failFirstPutData bool

	// abortPayloads makes the PutData handlers stop reading after the
	// first chunk and report the payload as deliberately dropped.
	abortPayloads bool

	// putDataBarrier, when set, blocks every PutData handler until it
	// is closed, after signalling putDataArrived.  Used to observe how
	// many payloads are in flight at once.
	putDataBarrier chan struct{}
	putDataArrived chan struct{}

	putDataCalls int

	// entries and payloads are joined into received as the two halves
	// of a record arrive, in either order.
	entries  map[uint64]*conn.Record
	payloads map[uint64][]byte
	// payloadErrs holds what the client reported it could not read.
	payloadErrs map[uint64]string

	// ids collects every entry id the server saw, in arrival order.
	ids []uint64
}

// join moves a record into received once both its entry and its
// payload have arrived.  Called with mu held.
func (f *fakeExporter) join(id uint64) {
	record, ok := f.entries[id]
	if !ok {
		return
	}
	payload, ok := f.payloads[id]
	if !ok {
		return
	}
	delete(f.entries, id)
	delete(f.payloads, id)
	f.received = append(f.received, receivedFile{record: record, payload: payload})
}

func (f *fakeExporter) PutData(stream grpc.ClientStreamingServer[proto.PutDataRequest, proto.PutDataResponse]) error {
	if f.putDataErr != nil {
		return f.putDataErr
	}

	f.mu.Lock()
	f.putDataCalls++
	first := f.putDataCalls == 1
	f.mu.Unlock()

	if f.failFirstPutData {
		if first {
			return status.Error(codes.Internal, "nope")
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}

	if f.putDataBarrier != nil {
		f.putDataArrived <- struct{}{}
		<-f.putDataBarrier
	}

	var (
		id     uint64
		seen   bool
		failed bool
		buf    bytes.Buffer
	)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch p := req.Packet.(type) {
		case *proto.PutDataRequest_Id:
			id, seen = p.Id, true
		case *proto.PutDataRequest_Chunk:
			if !seen {
				return status.Error(codes.InvalidArgument, "chunk before id")
			}
			buf.Write(p.Chunk)
			if f.abortPayloads {
				f.mu.Lock()
				f.payloadErrs[id] = "aborted"
				f.mu.Unlock()
				return stream.SendAndClose(&proto.PutDataResponse{Aborted: true})
			}
		case *proto.PutDataRequest_Error:
			if !seen {
				return status.Error(codes.InvalidArgument, "error before id")
			}
			failed = true
			f.mu.Lock()
			f.payloadErrs[id] = p.Error
			f.mu.Unlock()
		}
	}

	if !seen {
		return status.Error(codes.InvalidArgument, "no id")
	}

	if !failed {
		f.mu.Lock()
		f.payloads[id] = buf.Bytes()
		f.join(id)
		f.mu.Unlock()
	}

	return stream.SendAndClose(&proto.PutDataResponse{})
}

type receivedFile struct {
	record  *conn.Record
	payload []byte
}

func (f *fakeExporter) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	return &proto.InitResponse{Origin: "o", Type: "t", Root: "/", Flags: 0, HasPutData: f.hasPutData}, nil
}
func (f *fakeExporter) Ping(ctx context.Context, _ *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{}, nil
}
func (f *fakeExporter) Close(ctx context.Context, _ *proto.CloseRequest) (*proto.CloseResponse, error) {
	return &proto.CloseResponse{}, nil
}

func (f *fakeExporter) Export(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse]) error {
	if f.sendNilResult {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return stream.Send(&proto.ExportResponse{})
	}

	if f.earlyExportErr != nil {
		// Mirrors a connector whose Export() fails: the handler returns
		// while the client may still be sending records.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return f.earlyExportErr
	}

	var current *conn.Record
	var buf bytes.Buffer

	// Pre-queue any responses to send after we see at least one record.
	pending := append([]*connectors.Result(nil), f.respond...)

	flush := func() {
		f.mu.Lock()
		f.received = append(f.received, receivedFile{
			record:  current,
			payload: append([]byte(nil), buf.Bytes()...),
		})
		f.mu.Unlock()
		current = nil
		buf.Reset()
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch p := req.Packet.(type) {
		case *proto.ExportRequest_Record:
			if current != nil {
				flush()
			}
			current = p.Record
			if !p.Record.HasReader {
				flush()
			}
		case *proto.ExportRequest_Entry:
			f.mu.Lock()
			switch {
			case p.Entry.Id != 0:
				f.ids = append(f.ids, p.Entry.Id)
				f.entries[p.Entry.Id] = p.Entry.Record
				f.join(p.Entry.Id)
			default:
				f.received = append(f.received, receivedFile{
					record:  p.Entry.Record,
					payload: p.Entry.Chunk,
				})
			}
			f.mu.Unlock()
		case *proto.ExportRequest_Chunk:
			if len(p.Chunk) == 0 {
				// terminator chunk (client sends a zero-length chunk
				// after each file; protobuf may unmarshal nil as []byte{})
				flush()
			} else {
				buf.Write(p.Chunk)
			}
		}

		// echo back a result for each completed file, draining the
		// `pending` list (if any was supplied).
		f.mu.Lock()
		for len(pending) > 0 && len(f.received) > 0 {
			r := pending[0]
			pending = pending[1:]
			if err := stream.Send(&proto.ExportResponse{Result: conn.ResultToProto(r)}); err != nil {
				f.mu.Unlock()
				return err
			}
		}
		f.mu.Unlock()
	}
	if current != nil {
		flush()
	}
	return nil
}

// ---------------------------------------------------------------------------
// kloset mocking
// ---------------------------------------------------------------------------

// runExport is basically a no-op kloset restore: it feeds n records into the
// channel from a producer goroutine and consumes the results, returning the
// error Export() reported. producerDone reports whether the producer managed
// to finish; a false means it was left blocked on an undrained channel.
func runExport(t *testing.T, exp *Exporter, n int) (err error, producerFinished bool) {
	t.Helper()

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 64)

	producer := make(chan struct{})
	go func() {
		defer close(producer)
		for range n {
			records <- exportableRecord()
		}
		close(records)
	}()

	consumer := make(chan struct{})
	go func() {
		defer close(consumer)
		for range results {
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- exp.Export(t.Context(), records, results) }()

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for Export to return")
	}

	select {
	case <-producer:
		producerFinished = true
	case <-time.After(2 * time.Second):
		// Not fatal: the caller asserts on it, and a blocked producer is
		// exactly the regression this reports.
	}

	select {
	case <-consumer:
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for the result consumer to exit: results was never closed")
	}

	return err, producerFinished
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialExporter(t *testing.T, srv *fakeExporter) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	proto.RegisterExporterServer(s, srv)
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return conn, func() {
		_ = conn.Close()
		s.Stop()
	}
}

func exportableRecord() *connectors.Record {
	rec := &connectors.Record{
		Pathname: "/f.txt",
		FileInfo: objects.FileInfo{Lname: "f.txt", Lsize: 4, Lmode: 0o644},
	}
	rec.Reader = io.NopCloser(bytes.NewReader([]byte("data")))
	return rec
}

func dialFailing(t *testing.T, srv *fakeExporter) *Exporter {
	t.Helper()
	conn, cleanup := dialExporter(t, srv)
	t.Cleanup(cleanup)

	exp, err := NewExporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)
	return exp.(*Exporter)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestExporter_InitAndGetters(t *testing.T) {
	conn, cleanup := dialExporter(t, &fakeExporter{})
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)
	require.Equal(t, "o", exp.Origin())
	require.Equal(t, "t", exp.Type())
	require.Equal(t, "/", exp.Root())
}

func TestExporter_SendsRecordAndChunks(t *testing.T) {
	srv := &fakeExporter{
		respond: []*connectors.Result{
			{Record: connectors.Record{Pathname: "/a.txt"}},
		},
	}
	conn, cleanup := dialExporter(t, srv)
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("ab"), 100)
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{
			Lname: "a.txt",
			Lsize: int64(len(payload)),
			Lmode: 0o644,
		},
		Reader: io.NopCloser(bytes.NewReader(payload)),
	}

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 1)
	records <- rec
	close(records)

	done := make(chan error, 1)
	go func() { done <- exp.Export(context.Background(), records, results) }()

	// Drain results.
	gotResult := false
	for r := range results {
		require.NotNil(t, r)
		gotResult = true
	}
	require.True(t, gotResult, "expected at least one result from server")

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for Export")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	require.Len(t, srv.received, 1)
	require.Equal(t, payload, srv.received[0].payload)
}

func TestExporter_DirectoryRecordSkipsChunks(t *testing.T) {
	srv := &fakeExporter{}
	conn, cleanup := dialExporter(t, srv)
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	rec := &connectors.Record{
		Pathname: "/d",
		FileInfo: objects.FileInfo{
			Lname: "d",
			Lmode: 0o755 | 0x80000000, // dir bit
		},
	}

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 1)
	records <- rec
	close(records)

	done := make(chan error, 1)
	go func() { done <- exp.Export(context.Background(), records, results) }()

	for range results {
	}
	<-done

	srv.mu.Lock()
	defer srv.mu.Unlock()
	require.Len(t, srv.received, 1)
	require.Empty(t, srv.received[0].payload)
}

func TestExporter_ExportErrorIsReported(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{
		earlyExportErr: errors.New("simulated export tool failure"),
	})

	err, _ := runExport(t, exp, 50)

	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF, "a bare io.EOF from the send path, want the plugin's error")
	require.ErrorContains(t, err, "simulated export tool failure")
}

func TestExporter_ExportErrorDrainsRecords(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{
		earlyExportErr: errors.New("simulated export tool failure"),
	})

	_, producerFinished := runExport(t, exp, 50)

	require.True(t, producerFinished, "the producer is still blocked writing to records: the channel was not drained")
}

func TestExporter_ExportCanceledIsUnwrapped(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{
		earlyExportErr: status.Error(codes.Canceled, "ctx"),
	})

	err, _ := runExport(t, exp, 50)

	require.ErrorIs(t, err, context.Canceled)
}

func TestExporter_ExportUnavailableIsUnwrapped(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{
		earlyExportErr: status.Error(codes.Unavailable, "down"),
	})

	err, _ := runExport(t, exp, 50)

	require.ErrorContains(t, err, "I/O error")
}

func TestExporter_NilResultIsRejected(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{sendNilResult: true})

	err, producerFinished := runExport(t, exp, 5)

	require.ErrorContains(t, err, "expected a result")
	require.True(t, producerFinished, "the producer is still blocked writing to records")
}

func TestExporter_ExportSucceeds(t *testing.T) {
	exp := dialFailing(t, &fakeExporter{})

	err, producerFinished := runExport(t, exp, 5)

	require.NoError(t, err)
	require.True(t, producerFinished, "the producer is still blocked writing to records")
}

// TestExporter_TransmitRecordsCollapsesEOF tests transmitRecords directly,
// because the collapse cannot be observed through Export(): receiveResults is
// already parked in Recv() when the handler dies, so it always reports the
// real status before transmitRecords notices its Send failed. The collapse is
// what keeps that guarantee from resting on scheduling -- errgroup surfaces
// whichever goroutine reports first, and a bare io.EOF would win any race it
// did happen to win.
func TestExporter_TransmitRecordsCollapsesEOF(t *testing.T) {
	srv := &fakeExporter{earlyExportErr: errors.New("simulated export tool failure")}
	conn, cleanup := dialExporter(t, srv)
	defer cleanup()

	exp, err := NewExporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)
	g := exp.(*Exporter)

	stream, err := g.client.Export(t.Context())
	require.NoError(t, err)

	records := make(chan *connectors.Record, 1)
	producer := make(chan struct{})
	go func() {
		defer close(producer)
		for i := 0; i < 50; i++ {
			records <- exportableRecord()
		}
		close(records)
	}()

	// The first Send unblocks the handler's single Recv, after which it
	// returns its error and every later Send fails with io.EOF.
	require.NoError(t, g.transmitRecords(stream, records), "transmitRecords leaked a transport error")

	select {
	case <-producer:
	case <-time.After(2 * time.Second):
		require.Fail(t, "the producer is still blocked writing to records")
	}
}

// ---------------------------------------------------------------------------
// Entry protocol (PutData)
// ---------------------------------------------------------------------------

func newEntryServer() *fakeExporter {
	return &fakeExporter{
		hasPutData:  true,
		entries:     make(map[uint64]*conn.Record),
		payloads:    make(map[uint64][]byte),
		payloadErrs: make(map[uint64]string),
	}
}

func dialEntryServer(t *testing.T, srv *fakeExporter, maxConcurrency int) *Exporter {
	t.Helper()
	conn, cleanup := dialExporter(t, srv)
	t.Cleanup(cleanup)

	opts := &connectors.Options{MaxConcurrency: maxConcurrency}
	exp, err := NewExporter(t.Context(), conn, opts, "p", nil)
	require.NoError(t, err)
	require.True(t, exp.(*Exporter).hasPutData, "client did not pick up has_put_data")
	return exp.(*Exporter)
}

// recordOf builds a record whose reader serves payload, optionally
// failing with readErr once payload is exhausted.
func recordOf(name string, payload []byte, readErr error) *connectors.Record {
	var rd io.Reader = bytes.NewReader(payload)
	if readErr != nil {
		rd = io.MultiReader(rd, errReader{readErr})
	}
	return &connectors.Record{
		Pathname: "/" + name,
		FileInfo: objects.FileInfo{Lname: name, Lsize: int64(len(payload)), Lmode: 0o644},
		Reader:   io.NopCloser(rd),
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// exportRecords runs a whole Export over recs and returns its error.
func exportRecords(t *testing.T, exp *Exporter, recs ...*connectors.Record) error {
	t.Helper()

	records := make(chan *connectors.Record, len(recs))
	for _, rec := range recs {
		records <- rec
	}
	close(records)

	results := make(chan *connectors.Result, len(recs)+1)
	done := make(chan error, 1)
	go func() { done <- exp.Export(t.Context(), records, results) }()

	for range results {
	}

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for Export to return")
		return nil
	}
}

func TestExporter_EntryInlinesSmallPayloads(t *testing.T) {
	big := bytes.Repeat([]byte("x"), inlineThreshold*3)

	for _, tc := range []struct {
		name     string
		payload  []byte
		putDatas int
	}{
		{"empty", nil, 0},
		{"small", []byte("data"), 0},
		{"threshold", bytes.Repeat([]byte("y"), inlineThreshold-1), 0},
		{"large", big, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newEntryServer()
			exp := dialEntryServer(t, srv, 4)

			require.NoError(t, exportRecords(t, exp, recordOf(tc.name, tc.payload, nil)))

			srv.mu.Lock()
			defer srv.mu.Unlock()

			require.Len(t, srv.received, 1)
			// bytes.Equal, not require.Equal: an empty payload may
			// arrive as nil or as an empty slice.
			require.True(t, bytes.Equal(tc.payload, srv.received[0].payload),
				"payload: got %d bytes, want %d", len(srv.received[0].payload), len(tc.payload))
			require.Equal(t, tc.putDatas, srv.putDataCalls, "PutData calls")
		})
	}
}

func TestExporter_EntrySendsDirectoriesWithoutPayload(t *testing.T) {
	srv := newEntryServer()
	exp := dialEntryServer(t, srv, 4)

	dir := &connectors.Record{
		Pathname: "/d",
		FileInfo: objects.FileInfo{Lname: "d", Lmode: 0o755 | os.ModeDir},
	}
	require.NoError(t, exportRecords(t, exp, dir))

	srv.mu.Lock()
	defer srv.mu.Unlock()

	require.Len(t, srv.received, 1)
	require.Nil(t, srv.received[0].payload, "directory carried a payload")
	require.Zero(t, srv.putDataCalls, "directory used PutData streams")
}

// A record we cannot read is the exporter's to report: the session
// carries on.
func TestExporter_EntryReadFailureIsPerRecord(t *testing.T) {
	boom := errors.New("boom")

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		// fails while probing for the inline threshold
		{"probing", []byte("short")},
		// fails once the payload is already on its way
		{"uploading", bytes.Repeat([]byte("x"), inlineThreshold*3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newEntryServer()
			exp := dialEntryServer(t, srv, 4)

			rec := recordOf(tc.name, tc.payload, boom)
			require.NoError(t, exportRecords(t, exp, rec, recordOf("ok", []byte("fine"), nil)))

			srv.mu.Lock()
			defer srv.mu.Unlock()

			require.Len(t, srv.payloadErrs, 1)
			for _, msg := range srv.payloadErrs {
				require.Contains(t, msg, boom.Error())
			}
			require.Len(t, srv.received, 1, "only the good record completes")
		})
	}
}

// An exporter that stops reading a payload says so in the response,
// which is not a failure of the session.
func TestExporter_EntryAbortedPayloadIsNotAnError(t *testing.T) {
	srv := newEntryServer()
	srv.abortPayloads = true
	exp := dialEntryServer(t, srv, 4)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	require.NoError(t, exportRecords(t, exp, recordOf("big", big, nil)))
}

// Anything else going wrong on a PutData stream is transport, and
// takes the session down rather than losing a record quietly.
func TestExporter_PutDataFailureFailsExport(t *testing.T) {
	srv := newEntryServer()
	srv.putDataErr = status.Error(codes.Internal, "nope")
	exp := dialEntryServer(t, srv, 4)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	err := exportRecords(t, exp, recordOf("big", big, nil), recordOf("big2", big, nil))
	require.ErrorContains(t, err, "nope", "want the PutData failure")
}

// The point of the whole exercise: payloads upload in parallel.
func TestExporter_EntryPayloadsUploadConcurrently(t *testing.T) {
	const concurrency = 4

	srv := newEntryServer()
	srv.putDataBarrier = make(chan struct{})
	srv.putDataArrived = make(chan struct{})
	exp := dialEntryServer(t, srv, concurrency)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	recs := make([]*connectors.Record, concurrency)
	for i := range recs {
		recs[i] = recordOf("big", big, nil)
	}

	done := make(chan error, 1)
	go func() { done <- exportRecords(t, exp, recs...) }()

	// Every handler blocks on the barrier, so seeing all of them
	// arrive means they are all open at the same time.
	for range concurrency {
		select {
		case <-srv.putDataArrived:
		case <-time.After(5 * time.Second):
			require.Fail(t, "timeout waiting for concurrent PutData streams")
		}
	}
	close(srv.putDataBarrier)

	require.NoError(t, <-done)

	srv.mu.Lock()
	defer srv.mu.Unlock()
	require.Len(t, srv.received, concurrency)
}

// A payload failure cancels the payloads still in flight; what comes
// back must be the failure, not the context.Canceled it caused.
func TestExporter_PutDataFailureBeatsTheCancellation(t *testing.T) {
	const concurrency = 4

	srv := newEntryServer()
	srv.failFirstPutData = true
	exp := dialEntryServer(t, srv, concurrency)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	recs := make([]*connectors.Record, concurrency)
	for i := range recs {
		recs[i] = recordOf("big", big, nil)
	}

	err := exportRecords(t, exp, recs...)
	require.ErrorContains(t, err, "nope", "want the PutData failure rather than its fallout")
}

type trackingReader struct {
	io.Reader
	closed atomic.Bool
}

func (r *trackingReader) Close() error {
	r.closed.Store(true)
	return nil
}

// Whatever ends an export, the records we took over and never sent
// still own an open reader until we close it.
func TestExporter_AbandonedRecordsAreClosed(t *testing.T) {
	const count = 50

	for _, tc := range []struct {
		name    string
		srv     *fakeExporter
		payload []byte
	}{{
		// the Export stream dies under the sender
		name:    "legacy",
		srv:     &fakeExporter{earlyExportErr: errors.New("simulated export tool failure")},
		payload: []byte("data"),
	}, {
		// a payload fails, which cancels the sender
		name:    "entry",
		srv:     newEntryServer(),
		payload: bytes.Repeat([]byte("x"), inlineThreshold*3),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tc.srv.putDataErr = status.Error(codes.Internal, "nope")

			conn, cleanup := dialExporter(t, tc.srv)
			t.Cleanup(cleanup)

			exp, err := NewExporter(t.Context(), conn, &connectors.Options{MaxConcurrency: 1}, "p", nil)
			require.NoError(t, err)

			readers := make([]*trackingReader, count)
			records := make(chan *connectors.Record)
			go func() {
				defer close(records)
				for i := range readers {
					readers[i] = &trackingReader{Reader: bytes.NewReader(tc.payload)}
					rec := recordOf("f", tc.payload, nil)
					rec.Reader = readers[i]
					records <- rec
				}
			}()

			results := make(chan *connectors.Result, count+1)
			done := make(chan error, 1)
			go func() { done <- exp.Export(t.Context(), records, results) }()

			for range results {
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				require.Fail(t, "timeout waiting for Export to return")
			}

			for i, rd := range readers {
				require.NotNil(t, rd, "the producer never got to record %d", i)
				require.True(t, rd.closed.Load(), "record %d was abandoned with its reader open", i)
			}
		})
	}
}

// TestExporter_PayloadIdsAreUniqueAcrossExports pins down what the
// plugin side relies on: an id names one payload for the whole life of
// the exporter, not just for one Export call.
func TestExporter_PayloadIdsAreUniqueAcrossExports(t *testing.T) {
	const (
		exports = 2
		perCall = 3
	)
	big := bytes.Repeat([]byte("x"), inlineThreshold*2)

	srv := newEntryServer()
	exp := dialEntryServer(t, srv, 4)

	errs := make(chan error, exports)
	for range exports {
		go func() {
			records := make(chan *connectors.Record, perCall)
			for range perCall {
				records <- recordOf("f", big, nil)
			}
			close(records)

			results := make(chan *connectors.Result, perCall+1)
			done := make(chan error, 1)
			go func() { done <- exp.Export(t.Context(), records, results) }()
			for range results {
			}
			errs <- <-done
		}()
	}

	for range exports {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.Fail(t, "timeout waiting for Export to return")
		}
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	seen := make(map[uint64]bool, len(srv.ids))
	for _, id := range srv.ids {
		require.False(t, seen[id], "payload id %d handed out twice", id)
		seen[id] = true
	}
	require.Len(t, srv.ids, exports*perCall)
}
