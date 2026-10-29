package client

import (
	"context"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/provisioner"
	"google.golang.org/grpc"
)

type provisionerClient struct {
	client provisioner.ProvisionerClient
}

func NewProvisioner(ctx context.Context, conn grpc.ClientConnInterface, config map[string]string) (integration.Provisioner, error) {
	client := provisioner.NewProvisionerClient(conn)
	if _, err := client.Init(ctx, &provisioner.InitRequest{Config: config}); err != nil {
		return nil, unwrap(err)
	}
	return &provisionerClient{client: client}, nil
}

func (g *provisionerClient) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &provisioner.PingRequest{})
	return unwrap(err)
}

func (g *provisionerClient) Provision(ctx context.Context, req integration.ProvisionRequest) (*integration.ProvisionResponse, error) {
	res, err := g.client.Provision(ctx, &provisioner.ProvisionRequest{Labels: req.Labels})
	if err != nil {
		return nil, unwrap(err)
	}
	return &integration.ProvisionResponse{
		Handle:   res.Handle,
		Protocol: res.Protocol,
		Target:   res.Target,
	}, nil
}

func (g *provisionerClient) Deprovision(ctx context.Context, handle string) error {
	_, err := g.client.Deprovision(ctx, &provisioner.DeprovisionRequest{Handle: handle})
	return unwrap(err)
}

func (g *provisionerClient) Close(ctx context.Context) error {
	_, err := g.client.Close(ctx, &provisioner.CloseRequest{})
	return unwrap(err)
}
