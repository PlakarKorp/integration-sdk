package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/inventory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcInventoryClient struct {
	config map[string]string
	client inventory.InventoryClient
}

func NewInventory(ctx context.Context, client grpc.ClientConnInterface, config map[string]string) (integration.Inventory, error) {
	inv := &grpcInventoryClient{
		config: config,
		client: inventory.NewInventoryClient(client),
	}

	return inv, nil
}

func (inv *grpcInventoryClient) Close(ctx context.Context) error {
	_, err := inv.client.Close(ctx, &inventory.CloseRequest{})
	if err != nil {
		return unwrap(err)
	}

	return nil
}

func (inv *grpcInventoryClient) List(ctx context.Context, entries chan<- *integration.InventoryEntry) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := inv.client.StatelessList(ctx, &inventory.StatelessListRequest{
		Config: inv.config,
	})
	if err != nil {
		return unwrap(err)
	}

	first := true
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if first && status.Code(err) == codes.Unimplemented {
			return inv.legacylist(ctx, entries)
		}
		if err != nil {
			return err
		}

		if res == nil || res.Entry == nil {
			return fmt.Errorf("expected a record")
		}

		entry := &integration.InventoryEntry{
			Class:    integration.ResourceClass(res.Entry.Class),
			SubClass: integration.ResourceSubClass(res.Entry.Subclass),
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

func (inv *grpcInventoryClient) legacylist(ctx context.Context, entries chan<- *integration.InventoryEntry) error {
	defer close(entries)

	_, err := inv.client.Init(ctx, &inventory.InitRequest{
		Config: inv.config,
	})
	if err != nil {
		return unwrap(err)
	}

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
			Class:    integration.ResourceClass(res.Entry.Class),
			SubClass: integration.ResourceSubClass(res.Entry.Subclass),
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
