package server

import (
	"context"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	proto "github.com/PlakarKorp/integration-sdk/proto/inventory"
	"github.com/PlakarKorp/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeInventory reports a country on the one entry it lists.
type fakeInventory struct {
	country integration.Country
}

func (f *fakeInventory) List(ctx context.Context, c chan<- *integration.InventoryEntry) error {
	defer close(c)
	c <- &integration.InventoryEntry{
		Class:   pkg.ResourceClass("Database"),
		URN:     "urn:test:one",
		Name:    "one",
		Region:  "nl-ams",
		Country: f.country,
	}
	return nil
}

func (f *fakeInventory) Close(context.Context) error { return nil }

func dialInventory(t *testing.T, inv integration.Inventory) proto.InventoryClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	proto.RegisterInventoryServer(srv, &grpcInventoryServer{
		constructor: func(context.Context, map[string]string) (integration.Inventory, error) { return inv, nil },
	})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

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

	client := proto.NewInventoryClient(conn)
	if _, err := client.Init(context.Background(), &proto.InitRequest{}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return client
}

// The server normalizes what it sends, so an integration that skips CountryOf
// cannot put a region string or a country name on the wire.
func TestInventoryListSendsNormalizedCountry(t *testing.T) {
	tests := []struct {
		name    string
		country integration.Country
		want    string
	}{
		{"valid", "NL", "NL"},
		{"lowercase", "nl", "NL"},
		{"none", "", ""},
		{"bad code", "FRANCE", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := dialInventory(t, &fakeInventory{country: tt.country})

			stream, err := client.List(context.Background(), &proto.ListRequest{})
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			res, err := stream.Recv()
			if err != nil {
				t.Fatalf("recv: %v", err)
			}
			if got := res.GetEntry().GetCountry(); got != tt.want {
				t.Fatalf("wire country = %q, want %q", got, tt.want)
			}
		})
	}
}
