package server

import (
	"context"
	"io"
	"iter"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/location"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// A store the handlers must refuse the request before reaching.
type unreachedStore struct{ t *testing.T }

func (s *unreachedStore) Create(context.Context, []byte) error           { return nil }
func (s *unreachedStore) Open(context.Context) ([]byte, error)           { return nil, nil }
func (s *unreachedStore) Ping(context.Context) error                     { return nil }
func (s *unreachedStore) Origin() string                                 { return "o" }
func (s *unreachedStore) Type() string                                   { return "t" }
func (s *unreachedStore) Root() string                                   { return "/" }
func (s *unreachedStore) Flags() location.Flags                          { return 0 }
func (s *unreachedStore) Mode(context.Context) (integration.Mode, error) { return 0, nil }
func (s *unreachedStore) Size(context.Context) (int64, error)            { return 0, nil }
func (s *unreachedStore) Close(context.Context) error                    { return nil }

func (s *unreachedStore) List(context.Context, integration.StorageResource, integration.StorageFlag) iter.Seq2[integration.Resource, error] {
	return func(func(integration.Resource, error) bool) {}
}

func (s *unreachedStore) Put(context.Context, integration.StorageResource, integration.StorageFlag, integration.MAC, io.Reader) (int64, error) {
	s.t.Error("Put reached the store")
	return 0, nil
}

func (s *unreachedStore) Get(context.Context, integration.StorageResource, integration.StorageFlag, integration.MAC, *integration.Range) (io.ReadCloser, error) {
	s.t.Error("Get reached the store")
	return nil, nil
}

func (s *unreachedStore) Delete(context.Context, integration.StorageResource, integration.StorageFlag, integration.MAC) error {
	s.t.Error("Delete reached the store")
	return nil
}

func dialStore(t *testing.T, st integration.Store) proto.StoreClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	proto.RegisterStoreServer(srv, &storagePluginServer{
		constructor: func(context.Context, string, map[string]string) (integration.Store, error) { return st, nil },
	})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := proto.NewStoreClient(conn)
	_, err = client.Init(context.Background(), &proto.InitRequest{})
	require.NoError(t, err)
	return client
}

// A MAC of the wrong length is refused as a bad argument instead of
// panicking the plugin on the array conversion.
func TestStorageRejectsWrongMACLength(t *testing.T) {
	client := dialStore(t, &unreachedStore{t: t})
	ctx := context.Background()
	mac := make([]byte, 31)

	_, err := client.Delete(ctx, &proto.DeleteRequest{Mac: mac})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	get, err := client.Get(ctx, &proto.GetRequest{Mac: mac})
	require.NoError(t, err)
	_, err = get.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	put, err := client.Put(ctx)
	require.NoError(t, err)
	if err := put.Send(&proto.PutRequest{Mac: mac}); err != nil {
		// the server may already have hung up; the status comes next
		require.ErrorIs(t, err, io.EOF)
	}
	_, err = put.CloseAndRecv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
