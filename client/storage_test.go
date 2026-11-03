package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// fakeServer
// ---------------------------------------------------------------------------

type fakeServer struct {
	proto.UnimplementedStoreServer

	openConfig []byte
	macs       [][]byte
	getChunks  [][]byte
	getErr     error
	mode       int32

	putBytes int64
	putErr   error
	// putEarly makes Put answer as soon as it has the mac, without
	// waiting for the payload.
	putEarly bool

	// putEnd, when set, reports how the Put stream ended: nil when the
	// client closed it, the error otherwise.
	putEnd chan error

	receivedMac    []byte
	receivedType   proto.StorageResource
	receivedChunks [][]byte
}

func (f *fakeServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	return &proto.InitResponse{Origin: "o", Type: "t", Root: "/"}, nil
}
func (f *fakeServer) Open(ctx context.Context, _ *proto.OpenRequest) (*proto.OpenResponse, error) {
	return &proto.OpenResponse{Config: f.openConfig}, nil
}
func (f *fakeServer) Create(ctx context.Context, _ *proto.CreateRequest) (*proto.CreateResponse, error) {
	return &proto.CreateResponse{}, nil
}
func (f *fakeServer) Ping(ctx context.Context, _ *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{}, nil
}
func (f *fakeServer) Close(ctx context.Context, _ *proto.CloseRequest) (*proto.CloseResponse, error) {
	return &proto.CloseResponse{}, nil
}
func (f *fakeServer) Mode(ctx context.Context, _ *proto.ModeRequest) (*proto.ModeResponse, error) {
	return &proto.ModeResponse{Mode: f.mode}, nil
}
func (f *fakeServer) Size(ctx context.Context, _ *proto.SizeRequest) (*proto.SizeResponse, error) {
	return &proto.SizeResponse{Size: 42}, nil
}
func (f *fakeServer) List(ctx context.Context, _ *proto.ListRequest) (*proto.ListResponse, error) {
	return &proto.ListResponse{Macs: f.macs}, nil
}
func (f *fakeServer) Delete(ctx context.Context, _ *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	return &proto.DeleteResponse{}, nil
}

func (f *fakeServer) Put(stream grpc.ClientStreamingServer[proto.PutRequest, proto.PutResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if f.putEnd != nil {
				f.putEnd <- err
			}
			return err
		}
		if len(req.Mac) > 0 {
			f.receivedMac = append([]byte(nil), req.Mac...)
			f.receivedType = req.Type
		}
		if f.putEarly {
			break
		}
		if len(req.Chunk) > 0 {
			f.receivedChunks = append(f.receivedChunks, append([]byte(nil), req.Chunk...))
		}
	}
	if f.putEnd != nil {
		f.putEnd <- nil
	}
	if f.putErr != nil {
		return f.putErr
	}
	return stream.SendAndClose(&proto.PutResponse{BytesWritten: f.putBytes})
}

func (f *fakeServer) Get(_ *proto.GetRequest, stream grpc.ServerStreamingServer[proto.GetResponse]) error {
	if f.getErr != nil {
		return f.getErr
	}
	for _, c := range f.getChunks {
		if err := stream.Send(&proto.GetResponse{Chunk: c}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialTestServer(t *testing.T, srv *fakeServer) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	proto.RegisterStoreServer(s, srv)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestStore_BasicLifecycle(t *testing.T) {
	srv := &fakeServer{openConfig: []byte("cfg")}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, err := NewStorage(context.Background(), conn, "p", nil)
	require.NoError(t, err)

	cfg, err := s.Open(context.Background())
	require.NoError(t, err, "Open")
	require.Equal(t, "cfg", string(cfg))
	require.NoError(t, s.Create(context.Background(), []byte("x")), "Create")
	require.NoError(t, s.Ping(context.Background()), "Ping")
	size, err := s.Size(context.Background())
	require.NoError(t, err, "Size")
	require.Equal(t, int64(42), size)
	require.NoError(t, s.Close(context.Background()), "Close")
}

func TestStore_List_ValidatesMacLength(t *testing.T) {
	srv := &fakeServer{
		macs: [][]byte{make([]byte, 16)},
	}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, _ := NewStorage(context.Background(), conn, "p", nil)
	var itererr error
	for _, err := range s.List(context.Background(), integration.StorageResourcePackfile, 0) {
		itererr = err
	}
	require.Error(t, itererr, "expected validation error for short MAC")
}

func TestStore_List_RoundTripsMacs(t *testing.T) {
	full := bytes.Repeat([]byte{0x42}, 32)
	srv := &fakeServer{macs: [][]byte{full}}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, _ := NewStorage(context.Background(), conn, "p", nil)
	var count int
	for got, err := range s.List(context.Background(), integration.StorageResourcePackfile, 0) {
		count++
		require.NoError(t, err)
		require.Equal(t, integration.Resource{MAC: integration.MAC(full)}, got)
	}
	require.Equal(t, 1, count)
}

func TestStore_PutStreamsChunks(t *testing.T) {
	srv := &fakeServer{putBytes: 11}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, _ := NewStorage(context.Background(), conn, "p", nil)

	var mac integration.MAC
	for i := range mac {
		mac[i] = byte(i)
	}
	payload := []byte("hello world")
	n, err := s.Put(context.Background(), integration.StorageResourcePackfile, 0, mac, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, int64(11), n, "BytesWritten")
	require.Equal(t, mac[:], srv.receivedMac)
	var glued []byte
	for _, c := range srv.receivedChunks {
		glued = append(glued, c...)
	}
	require.Equal(t, payload, glued)
}

// endless is a payload that never ends: only the server can stop Put.
type endless struct{}

func (endless) Read(p []byte) (int, error) { return len(p), nil }

// A server answering before it has the whole payload fails the Put,
// with its own error when it has one.
func TestStore_PutCutShort(t *testing.T) {
	suite := []struct {
		name string
		srv  *fakeServer
		want string
	}{
		{"with an error", &fakeServer{putEarly: true, putErr: status.Error(codes.Internal, "disk full")}, "disk full"},
		{"without an error", &fakeServer{putEarly: true}, "stopped reading"},
	}

	for _, tc := range suite {
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup := dialTestServer(t, tc.srv)
			defer cleanup()

			s, err := NewStorage(t.Context(), conn, "p", nil)
			require.NoError(t, err)

			_, err = s.Put(t.Context(), integration.StorageResourcePackfile, 0, integration.MAC{}, endless{})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// A payload we fail to read must not reach the server as a complete,
// shorter one.

// If the reader passed to Put() fails, we must cancel the operation
// (and not leak nor just close it, which would truncate the payload.)
func TestStore_PutReadErrorCancelsTheStream(t *testing.T) {
	srv := &fakeServer{putEnd: make(chan error, 1)}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, err := NewStorage(t.Context(), conn, "p", nil)
	require.NoError(t, err)

	sentinel := errors.New("disk-on-fire")

	rd := io.MultiReader(bytes.NewReader([]byte("hello")), &errReader{err: sentinel})
	_, err = s.Put(t.Context(), integration.StorageResourcePackfile, 0, integration.MAC{}, rd)
	require.ErrorContains(t, err, sentinel.Error())

	select {
	case err := <-srv.putEnd:
		require.Equal(t, codes.Canceled, status.Code(err), "the server saw the stream end with %v", err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the Put stream is still open on the server")
	}
}

func TestStore_GetStreamsChunks(t *testing.T) {
	srv := &fakeServer{
		getChunks: [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
	}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, _ := NewStorage(context.Background(), conn, "p", nil)
	rd, err := s.Get(context.Background(), integration.StorageResourcePackfile, 0, integration.MAC{}, nil)
	require.NoError(t, err)
	defer rd.Close()
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "foobarbaz", string(data))
}

func TestStore_Get_UnavailableMapsToIOError(t *testing.T) {
	srv := &fakeServer{
		getErr: status.Error(codes.Unavailable, "down"),
	}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	s, _ := NewStorage(context.Background(), conn, "p", nil)
	rd, err := s.Get(context.Background(), integration.StorageResourcePackfile, 0, integration.MAC{}, nil)
	require.NoError(t, err)
	defer rd.Close()

	_, err = io.ReadAll(rd)
	require.Error(t, err, "expected error reading from server that returned Unavailable")
}

func TestSendChunks_PropagatesReadError(t *testing.T) {
	rd := io.NopCloser(&errReader{err: errors.New("disk-on-fire")})
	n, err := SendChunks(rd, func([]byte) error { return nil })
	require.Error(t, err, "expected read error")
	require.Zero(t, n)
}

func TestSendChunks_PropagatesSendError(t *testing.T) {
	rd := io.NopCloser(bytes.NewReader([]byte("xxx")))
	sendErr := errors.New("net dead")
	_, err := SendChunks(rd, func([]byte) error { return sendErr })
	require.Error(t, err, "expected send error")
}

func TestReceiveChunks_ReassemblesAcrossChunkBoundaries(t *testing.T) {
	chunks := [][]byte{[]byte("ab"), []byte("cde"), []byte("fghij")}
	i := 0
	rd := ReceiveChunks(func() ([]byte, error) {
		if i >= len(chunks) {
			return nil, io.EOF
		}
		c := chunks[i]
		i++
		return c, nil
	})

	// Read exactly 5 bytes at a time to force the buffer logic to span chunks.
	buf := make([]byte, 5)
	var out bytes.Buffer
	for {
		n, err := rd.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Read")
	}
	require.Equal(t, "abcdefghij", out.String())
}
