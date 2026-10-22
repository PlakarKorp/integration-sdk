package client

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/importer"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// fakeImporter implements ImporterServer in-process for tests.
// ---------------------------------------------------------------------------

type fakeImporter struct {
	proto.UnimplementedImporterServer

	initResp *proto.InitResponse
	initErr  error

	pingErr error

	// records to emit on Import (server-side)
	records []*conn.Record
	// chunks to emit on Open(record)
	openChunks map[string][][]byte
	openErr    error

	// Because kloset is async, we can choose if we want import to err before results are closed or after
	// Note: tests mostly use earlyImportErr because it is the case that is most likely to cause errors by design
	// (closing the grpc stream before closing results has been the most problematic case).
	earlyImportErr error
	importErr      error

	// captured Acks from the client
	acks    []*conn.Result
	ackDone chan struct{} // closed when Import returns server-side

	closeErr error
}

func (f *fakeImporter) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	if f.initErr != nil {
		return nil, f.initErr
	}
	return f.initResp, nil
}

func (f *fakeImporter) Ping(ctx context.Context, _ *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{}, f.pingErr
}

func (f *fakeImporter) Close(ctx context.Context, _ *proto.CloseRequest) (*proto.CloseResponse, error) {
	return &proto.CloseResponse{}, f.closeErr
}

func (f *fakeImporter) Import(stream grpc.BidiStreamingServer[proto.ImportRequest, proto.ImportResponse]) error {
	if f.ackDone != nil {
		defer close(f.ackDone)
	}
	for _, r := range f.records {
		if err := stream.Send(&proto.ImportResponse{Record: r}); err != nil {
			return err
		}
	}
	if err := stream.Send(&proto.ImportResponse{Finished: true}); err != nil {
		return err
	}

	if f.earlyImportErr != nil {
		return f.earlyImportErr
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if req.Result != nil {
			res, _ := conn.ResultFromProto(req.Result)
			f.acks = append(f.acks, &conn.Result{
				Header: conn.RecordToProto(&res.Record),
			})
		}
	}

	return f.importErr
}

func (f *fakeImporter) Open(req *proto.OpenRequest, stream grpc.ServerStreamingServer[proto.OpenResponse]) error {
	if f.openErr != nil {
		return f.openErr
	}
	chunks := f.openChunks[req.Record.Pathname]
	for _, c := range chunks {
		if err := stream.Send(&proto.OpenResponse{Chunk: c}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// kloset mocking
// ---------------------------------------------------------------------------

// runImport is basically a no-op kloset's snapshot.Builder.importSource.
// It consumes and acks records, returns seen records and error
func runImport(t *testing.T, imp *Importer) (error, []*connectors.Record) {
	t.Helper()

	records := make(chan *connectors.Record, 4)
	results := make(chan *connectors.Result, 4)

	var seen []*connectors.Record
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(results)
		for r := range records {
			seen = append(seen, r)
			results <- r.Ok()
		}
	}()

	importDone := make(chan error, 1)
	go func() { importDone <- imp.Import(t.Context(), records, results) }()

	var err error
	select {
	case err = <-importDone:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for Import to return")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second): // shorter timer here because adding records to a list should not take long.
		require.Fail(t, "timeout waiting for the record consumer to exit")
	}

	return err, seen
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialImporter(t *testing.T, srv *fakeImporter) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	proto.RegisterImporterServer(s, srv)
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "dial")
	return conn, func() {
		_ = conn.Close()
		s.Stop()
	}
}

func sampleFI() *conn.FileInfo {
	return &conn.FileInfo{
		Name:    "x.txt",
		Size:    3,
		Mode:    0o644,
		ModTime: nil, // ModTime is not nil-safe upstream; test uses helper below
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestImporter_InitAndGetters(t *testing.T) {
	srv := &fakeImporter{
		initResp: &proto.InitResponse{
			Origin: "o",
			Type:   "t",
			Root:   "/r",
			Flags:  0,
		},
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(context.Background(), conn, &connectors.Options{}, "proto", nil)
	require.NoError(t, err)
	require.Equal(t, "o", imp.Origin())
	require.Equal(t, "t", imp.Type())
	require.Equal(t, "/r", imp.Root())
	// FLAG_NEEDACK is always OR'd in.
	require.NotZero(t, imp.Flags(), "expected FLAG_NEEDACK to be set")
	require.NoError(t, imp.Ping(context.Background()))
	require.NoError(t, imp.Close(context.Background()))
}

func TestImporter_Init_UnwrapsErrors(t *testing.T) {
	srv := &fakeImporter{
		initErr: status.Error(codes.Unavailable, "down"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	_, err := NewImporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.ErrorContains(t, err, "I/O error", "expected Unavailable to be wrapped as I/O error")
}

func TestImporter_Init_CanceledMapsToContextCanceled(t *testing.T) {
	srv := &fakeImporter{
		initErr: status.Error(codes.Canceled, "ctx"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	_, err := NewImporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestImporter_ImportStreamsRecordsAndAcks(t *testing.T) {
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{
			Lname: "a.txt",
			Lsize: 5,
			Lmode: 0o644,
		},
	}
	pbrec := conn.RecordToProto(rec)

	srv := &fakeImporter{
		initResp:   &proto.InitResponse{Type: "t", Root: "/"},
		records:    []*conn.Record{pbrec},
		openChunks: map[string][][]byte{"/a.txt": {[]byte("hello")}},
		ackDone:    make(chan struct{}),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	records := make(chan *connectors.Record, 4)
	results := make(chan *connectors.Result, 4)

	importDone := make(chan error, 1)
	go func() { importDone <- imp.Import(context.Background(), records, results) }()

	var got *connectors.Record
	select {
	case got = <-records:
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for record")
	}
	require.NotNil(t, got)
	require.Equal(t, "/a.txt", got.Pathname)

	// Open via LazyReader
	data, err := io.ReadAll(got.Reader)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
	_ = got.Reader.Close()

	// ACK
	results <- &connectors.Result{Record: *got}
	close(results)

	require.NoError(t, <-importDone)

	// Wait for the server to finish its Import handler so it has actually
	// drained the ack stream — Import returns as soon as the client side
	// CloseSends, but the server may still be receiving.
	select {
	case <-srv.ackDone:
	case <-time.After(2 * time.Second):
		require.Fail(t, "timeout waiting for server import handler to exit")
	}
	require.Len(t, srv.acks, 1)
}

func TestImporter_OpenErrorPropagates(t *testing.T) {
	srv := &fakeImporter{
		initResp: &proto.InitResponse{},
		openErr:  status.Error(codes.NotFound, "nope"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	gi := imp.(*Importer)
	rd, err := gi.open(context.Background(), &connectors.Record{Pathname: "/missing"})
	require.NoError(t, err, "open returned err immediately")
	defer rd.Close()
	_, err = io.ReadAll(rd)
	require.Error(t, err, "expected read error from server")
}

func TestImporter_EarlyImportErrorAfterFinishedIsReported(t *testing.T) {
	srv := &fakeImporter{
		initResp:       &proto.InitResponse{Type: "t", Root: "/"},
		earlyImportErr: errors.New("import failed: simulated backup tool failure"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	earlyImportErr, seen := runImport(t, imp.(*Importer))

	require.Empty(t, seen)
	require.ErrorContains(t, earlyImportErr, "simulated backup tool failure")
}

func TestImporter_EarlyImportErrorAfterRecordsIsReported(t *testing.T) {
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{Lname: "a.txt", Lsize: 5, Lmode: 0o644},
	}

	srv := &fakeImporter{
		initResp:       &proto.InitResponse{Type: "t", Root: "/"},
		records:        []*conn.Record{conn.RecordToProto(rec)},
		earlyImportErr: errors.New("import failed: simulated backup tool failure"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	earlyImportErr, seen := runImport(t, imp.(*Importer))

	require.Len(t, seen, 1, "expected the record emitted before the failure")
	require.Equal(t, "/a.txt", seen[0].Pathname)
	require.ErrorContains(t, earlyImportErr, "simulated backup tool failure")
}

func TestImporter_ImportErrorAfterRecordsIsReported(t *testing.T) {
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{Lname: "a.txt", Lsize: 5, Lmode: 0o644},
	}

	srv := &fakeImporter{
		initResp:  &proto.InitResponse{Type: "t", Root: "/"},
		records:   []*conn.Record{conn.RecordToProto(rec)},
		importErr: errors.New("import failed: simulated backup tool failure"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	ImportErr, seen := runImport(t, imp.(*Importer))

	require.Len(t, seen, 1, "expected the record emitted before the failure")
	require.Equal(t, "/a.txt", seen[0].Pathname)
	require.ErrorContains(t, ImportErr, "simulated backup tool failure")
}

func TestImporter_EarlyImportCanceledIsUnwrapped(t *testing.T) {
	srv := &fakeImporter{
		initResp:       &proto.InitResponse{Type: "t", Root: "/"},
		earlyImportErr: status.Error(codes.Canceled, "ctx"),
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	earlyImportErr, _ := runImport(t, imp.(*Importer))

	require.ErrorIs(t, earlyImportErr, context.Canceled)
}

func TestImporter_EarlyImportSucceedsAfterFinished(t *testing.T) {
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{Lname: "a.txt", Lsize: 5, Lmode: 0o644},
	}

	srv := &fakeImporter{
		initResp: &proto.InitResponse{Type: "t", Root: "/"},
		records:  []*conn.Record{conn.RecordToProto(rec)},
	}
	conn, cleanup := dialImporter(t, srv)
	defer cleanup()

	imp, err := NewImporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	require.NoError(t, err)

	earlyImportErr, seen := runImport(t, imp.(*Importer))

	require.NoError(t, earlyImportErr)
	require.Len(t, seen, 1)
}
