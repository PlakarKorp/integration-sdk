package sdk_test

import (
	"context"
	"net"
	"testing"

	"github.com/PlakarKorp/go-inventory-sdk/inventory"
	"github.com/PlakarKorp/go-inventory-sdk/sdk"
	sdkclient "github.com/PlakarKorp/go-inventory-sdk/sdk/client"
	sdkserver "github.com/PlakarKorp/go-inventory-sdk/sdk/server"
	"github.com/PlakarKorp/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeInventory reports a country on the one entry it lists. Only resources
// have a country -- an inventory is a configuration and holds no bytes.
type fakeInventory struct {
	entry inventory.Country
}

func (f *fakeInventory) List(ctx context.Context, c chan<- *inventory.InventoryEntry) error {
	defer close(c)
	c <- &inventory.InventoryEntry{
		Class:   pkg.ResourceClass("Database"),
		URN:     "urn:test:one",
		Name:    "one",
		Region:  "nl-ams",
		Country: f.entry,
	}
	return nil
}

func (f *fakeInventory) Close(context.Context) error { return nil }

// dial stands a real server and client up over an in-memory connection, so the
// generated marshalling is exercised rather than stubbed.
func dial(t *testing.T, inv inventory.Inventory) inventory.Inventory {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	sdk.RegisterInventoryServer(srv, sdkserver.NewGrpcInventoryServer(
		func(context.Context, map[string]string) (inventory.Inventory, error) { return inv, nil },
	))

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

	client, err := sdkclient.NewGrpcInventory(context.Background(), conn, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	return client
}

func TestCountryCrossesTheWire(t *testing.T) {
	client := dial(t, &fakeInventory{entry: "NL"})

	c := make(chan *inventory.InventoryEntry, 4)
	if err := client.List(context.Background(), c); err != nil {
		t.Fatalf("list: %v", err)
	}
	entry := <-c
	if entry == nil {
		t.Fatal("expected an entry")
	}
	if entry.Country != "NL" {
		t.Fatalf("entry country = %q, want NL", entry.Country)
	}
}

// Reporting no country is not an error -- the path every existing integration
// takes.
func TestNoCountryReportedIsNotAnError(t *testing.T) {
	client := dial(t, &fakeInventory{})

	c := make(chan *inventory.InventoryEntry, 4)
	if err := client.List(context.Background(), c); err != nil {
		t.Fatalf("list: %v", err)
	}
	entry := <-c
	if entry == nil {
		t.Fatal("expected an entry")
	}
	if entry.Country != "" {
		t.Fatalf("entry country = %q, want empty", entry.Country)
	}
}

// A bad code is normalized away on arrival, so an integration that skips
// CountryOf cannot put a region string or a country name into the control
// plane.
func TestBadCodeIsDroppedOnArrival(t *testing.T) {
	client := dial(t, &fakeInventory{entry: "FRANCE"})

	c := make(chan *inventory.InventoryEntry, 4)
	if err := client.List(context.Background(), c); err != nil {
		t.Fatalf("list: %v", err)
	}
	if entry := <-c; entry.Country != "" {
		t.Fatalf("entry country = %q, want empty", entry.Country)
	}
}
