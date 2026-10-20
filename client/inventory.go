package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/inventory"
	"github.com/PlakarKorp/pkg"
	"google.golang.org/grpc"
)

type grpcInventoryClient struct {
	client inventory.InventoryClient
}

func NewInventory(ctx context.Context, client grpc.ClientConnInterface, config map[string]string) (integration.Inventory, error) {
	inv := &grpcInventoryClient{
		client: inventory.NewInventoryClient(client),
	}

	if err := inv.Init(ctx, config); err != nil {
		return nil, err
	}

	return inv, nil
}

func (inv *grpcInventoryClient) Init(ctx context.Context, config map[string]string) error {
	_, err := inv.client.Init(ctx, &inventory.InitRequest{
		Config: config,
	})

	if err != nil {
		return unwrap(err)
	}

	return nil
}

func (inv *grpcInventoryClient) Close(ctx context.Context) error {
	_, err := inv.client.Close(ctx, &inventory.CloseRequest{})
	if err != nil {
		return unwrap(err)
	}

	return nil
}

func (inv *grpcInventoryClient) List(ctx context.Context, entries chan<- *integration.InventoryEntry) error {
	defer close(entries)

	stream, err := inv.client.List(ctx, &inventory.ListRequest{})
	if err != nil {
		return unwrap(err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return unwrap(err)
		}

		if res == nil || res.Entry == nil {
			return fmt.Errorf("expected a record")
		}

		entry := &integration.InventoryEntry{
			Class:    pkg.ResourceClass(res.Entry.Class),
			SubClass: pkg.ResourceSubClass(res.Entry.Subclass),
			Tags:     res.Entry.Tags,
			URN:      res.Entry.Urn,
			Name:     res.Entry.Name,
			Region:   res.Entry.Region,
			Service:  res.Entry.Service,
			Resource: res.Entry.Resource,
			Details:  res.Entry.Details,
			Country:  integration.CountryOf(res.Entry.GetCountry()),
		}

		for _, e := range res.Entry.GetEndpoints() {
			entry.Endpoints = append(entry.Endpoints, integration.HostEndpoint{
				Type:       integration.EndpointType(e.Type),
				Endpoint:   e.Endpoint,
				Attributes: e.Attributes,
			})
		}

		entries <- entry
	}
}
