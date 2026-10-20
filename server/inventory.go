package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/inventory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type InventoryFn func(context.Context, map[string]string) (integration.Inventory, error)

type grpcInventoryServer struct {
	inventory.UnimplementedInventoryServer

	maxconcurrency int
	instance       instance[integration.Inventory]
	constructor    InventoryFn
}

func (g *grpcInventoryServer) Init(ctx context.Context, req *inventory.InitRequest) (*inventory.InitResponse, error) {
	_, err := g.instance.init(func() (integration.Inventory, error) {
		return g.constructor(ctx, req.Config)
	})
	if err != nil {
		return nil, err
	}
	return &inventory.InitResponse{}, nil
}

func (g *grpcInventoryServer) List(req *inventory.ListRequest, stream grpc.ServerStreamingServer[inventory.ListResponse]) error {
	inv, err := g.instance.get()
	if err != nil {
		return err
	}

	entries := make(chan *integration.InventoryEntry, g.maxconcurrency)
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(nil) // needed?

	senderr := errors.New("send error")

	var wg errgroup.Group

	wg.Go(func() error {
		for res := range entries {
			entry := &inventory.InventoryEntry{
				Class:    string(res.Class),
				Subclass: string(res.SubClass),
				Tags:     res.Tags,
				Urn:      res.URN,
				Name:     res.Name,
				Region:   res.Region,
				Service:  res.Service,
				Resource: res.Resource,
				Details:  res.Details,
				// Re-normalized: an integration may skip CountryOf.
				Country: string(integration.CountryOf(string(res.Country))),
			}

			for _, e := range res.Endpoints {
				entry.Endpoints = append(entry.Endpoints, &inventory.HostEndpoint{
					Type:       string(e.Type),
					Endpoint:   e.Endpoint,
					Attributes: e.Attributes,
				})
			}

			if err := stream.Send(&inventory.ListResponse{Entry: entry}); err != nil {
				cancel(senderr)
				for range entries {
				}
				return err
			}
		}

		return nil
	})

	err = inv.List(ctx, entries)
	wgerr := wg.Wait()

	if err != nil {
		if ctx.Err() == context.Canceled && context.Cause(ctx) == senderr {
			return wgerr
		}
		return err
	}

	return nil
}

func (g *grpcInventoryServer) Close(ctx context.Context, _ *inventory.CloseRequest) (*inventory.CloseResponse, error) {
	inv, err := g.instance.take()
	if err != nil {
		return nil, err
	}

	if err := inv.Close(ctx); err != nil {
		return nil, err
	}
	return &inventory.CloseResponse{}, nil
}

func RunInventory(ctx context.Context, constructor InventoryFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunInventoryOn(ctx, constructor, listener)
}

func RunInventoryOn(ctx context.Context, constructor InventoryFn, listener net.Listener) error {
	server := grpc.NewServer()
	inventory.RegisterInventoryServer(server, &grpcInventoryServer{
		constructor: constructor,
	})

	return serve(ctx, server, listener)
}
