package client

import (
	"context"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	proto "github.com/PlakarKorp/integration-sdk/proto/inventory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeInventoryServer sends one entry carrying country as-is, standing in for
// a plugin that does not normalize.
type fakeInventoryServer struct {
	proto.UnimplementedInventoryServer

	country string
}

func (f *fakeInventoryServer) Init(context.Context, *proto.InitRequest) (*proto.InitResponse, error) {
	return &proto.InitResponse{}, nil
}

func (f *fakeInventoryServer) List(_ *proto.ListRequest, stream grpc.ServerStreamingServer[proto.ListResponse]) error {
	return stream.Send(&proto.ListResponse{Entry: &proto.InventoryEntry{
		Class:   "Database",
		Urn:     "urn:test:one",
		Name:    "one",
		Region:  "nl-ams",
		Country: f.country,
	}})
}

func dialInventory(t *testing.T, srv *fakeInventoryServer) integration.Inventory {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	proto.RegisterInventoryServer(s, srv)

	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	inv, err := NewInventory(context.Background(), conn, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	return inv
}

// The client normalizes on arrival, so a plugin built against an older SDK
// cannot put a bad code into the control plane.
func TestInventoryListNormalizesCountry(t *testing.T) {
	tests := []struct {
		name    string
		country string
		want    integration.Country
	}{
		{"valid", "NL", "NL"},
		{"lowercase", "nl", "NL"},
		{"none", "", ""},
		{"bad code", "FRANCE", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inv := dialInventory(t, &fakeInventoryServer{country: tt.country})

			c := make(chan *integration.InventoryEntry, 4)
			if err := inv.List(context.Background(), c); err != nil {
				t.Fatalf("list: %v", err)
			}
			entry := <-c
			if entry == nil {
				t.Fatal("expected an entry")
			}
			if entry.Country != tt.want {
				t.Fatalf("entry country = %q, want %q", entry.Country, tt.want)
			}
		})
	}
}
