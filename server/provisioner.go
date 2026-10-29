package server

import (
	"context"
	"fmt"
	"net"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/provisioner"
	"google.golang.org/grpc"
)

type ProvisionerFn func(context.Context, map[string]string) (integration.Provisioner, error)

type provisionerServer struct {
	provisioner.UnimplementedProvisionerServer
	instance    instance[integration.Provisioner]
	constructor ProvisionerFn
}

func (g *provisionerServer) Init(ctx context.Context, req *provisioner.InitRequest) (*provisioner.InitResponse, error) {
	_, err := g.instance.init(func() (integration.Provisioner, error) {
		return g.constructor(ctx, req.Config)
	})
	if err != nil {
		return nil, err
	}
	return &provisioner.InitResponse{}, nil
}

func (g *provisionerServer) Ping(ctx context.Context, req *provisioner.PingRequest) (*provisioner.PingResponse, error) {
	p, err := g.instance.get()
	if err != nil {
		return nil, err
	}
	if err := p.Ping(ctx); err != nil {
		return nil, err
	}
	return &provisioner.PingResponse{}, nil
}

func (g *provisionerServer) Provision(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResponse, error) {
	p, err := g.instance.get()
	if err != nil {
		return nil, err
	}
	res, err := p.Provision(ctx, integration.ProvisionRequest{Labels: req.Labels})
	if err != nil {
		return nil, err
	}
	return &provisioner.ProvisionResponse{
		Handle:   res.Handle,
		Protocol: res.Protocol,
		Target:   res.Target,
	}, nil
}

func (g *provisionerServer) Deprovision(ctx context.Context, req *provisioner.DeprovisionRequest) (*provisioner.DeprovisionResponse, error) {
	p, err := g.instance.get()
	if err != nil {
		return nil, err
	}
	if err := p.Deprovision(ctx, req.Handle); err != nil {
		return nil, err
	}
	return &provisioner.DeprovisionResponse{}, nil
}

func (g *provisionerServer) Close(ctx context.Context, req *provisioner.CloseRequest) (*provisioner.CloseResponse, error) {
	p, err := g.instance.take()
	if err != nil {
		return nil, err
	}
	if err := p.Close(ctx); err != nil {
		return nil, err
	}
	return &provisioner.CloseResponse{}, nil
}

// RunProvisioner serves a provisioner, and its resolver when it has one.
func RunProvisioner(constructor ProvisionerFn, resolver integration.Resolver) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()
	return RunProvisionerOn(constructor, resolver, listener)
}

func RunProvisionerOn(constructor ProvisionerFn, resolver integration.Resolver, listener net.Listener) error {
	server := grpc.NewServer()
	provisioner.RegisterProvisionerServer(server, &provisionerServer{constructor: constructor})
	if resolver != nil {
		RegisterResolver(server, resolver)
	}
	return server.Serve(listener)
}
