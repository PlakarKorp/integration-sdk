package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/PlakarKorp/go-inventory-sdk/inventory"
	"github.com/PlakarKorp/go-inventory-sdk/sdk"
	"github.com/PlakarKorp/pkg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func unwrap(err error) error {
	if err == nil {
		return nil
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Canceled:
		return context.Canceled
	case codes.Unavailable:
		return fmt.Errorf("I/O error communicating with the integration (%s)", status.Message())
	default:
		return fmt.Errorf("%s", status.Message())
	}
}

type grpcInventoryClient struct {
	client sdk.InventoryClient
}

func NewGrpcInventory(ctx context.Context, client grpc.ClientConnInterface, config map[string]string) (inventory.Inventory, error) {
	inv := &grpcInventoryClient{
		client: sdk.NewInventoryClient(client),
	}

	if err := inv.Init(ctx, config); err != nil {
		return nil, err
	}

	return inv, nil
}

func (inv *grpcInventoryClient) Init(ctx context.Context, config map[string]string) error {
	_, err := inv.client.Init(ctx, &sdk.InitRequest{
		Config: config,
	})

	if err != nil {
		return unwrap(err)
	}

	return nil
}

func (inv *grpcInventoryClient) Close(ctx context.Context) error {
	_, err := inv.client.Close(ctx, &sdk.CloseRequest{})
	if err != nil {
		return unwrap(err)
	}

	return nil
}

func (inv *grpcInventoryClient) List(ctx context.Context, entries chan<- *inventory.InventoryEntry) error {
	defer close(entries)

	stream, err := inv.client.List(ctx, &sdk.ListRequest{})
	if err != nil {
		return unwrap(err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		if res == nil || res.Entry == nil {
			return fmt.Errorf("expected a record")
		}

		entry := &inventory.InventoryEntry{
			Class:    pkg.ResourceClass(res.Entry.Class),
			SubClass: pkg.ResourceSubClass(res.Entry.Subclass),
			Tags:     res.Entry.Tags,
			URN:      res.Entry.Urn,
			Name:     res.Entry.Name,
			Region:   res.Entry.Region,
			Service:  res.Entry.Service,
			Resource: res.Entry.Resource,
			Details:  res.Entry.Details,
			Country:  inventory.CountryOf(res.Entry.GetCountry()),
		}

		for _, e := range res.Entry.GetEndpoints() {
			entry.Endpoints = append(entry.Endpoints, inventory.HostEndpoint{
				Type:       inventory.EndpointType(e.Type),
				Endpoint:   e.Endpoint,
				Attributes: e.Attributes,
			})
		}

		entries <- entry
	}
}
