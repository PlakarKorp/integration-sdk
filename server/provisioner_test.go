package server

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fakeProvisioner struct {
	config        map[string]string
	deprovisioned string
}

func (f *fakeProvisioner) Ping(context.Context) error { return nil }

func (f *fakeProvisioner) Provision(_ context.Context, req integration.ProvisionRequest) (*integration.ProvisionResponse, error) {
	return &integration.ProvisionResponse{
		Handle:   "h-" + req.Labels["plakman.job"],
		Protocol: "fs",
		Target:   map[string]string{"location": "fs:///tmp/" + f.config["name"]},
	}, nil
}

func (f *fakeProvisioner) Deprovision(_ context.Context, handle string) error {
	f.deprovisioned = handle
	return nil
}

func (f *fakeProvisioner) Close(context.Context) error { return nil }

type fakeResolver struct{}

func (fakeResolver) Resolve(_ context.Context, req integration.ResolveRequest) (*integration.ResolveResponse, error) {
	if req.Resolver != "zones" {
		return nil, errors.New("unknown resolver")
	}
	return &integration.ResolveResponse{
		Search:  integration.SearchLocal,
		Columns: []integration.Column{{Key: "country", Type: integration.ColumnBadge, Tones: map[string]string{"FR": "neutral"}}},
		Options: []integration.Option{{
			Value: "fr-par-1", Label: "Paris 1", State: integration.OptionAvailable,
			Attributes: map[string]string{"country": "FR", "region": req.Params["region"]},
		}},
		Filters: []integration.ResolveFilter{{Key: "country", Values: []string{"FR", "NL"}}},
	}, nil
}

func TestProvisionerRoundTrip(t *testing.T) {
	ctx := context.Background()
	listener := bufconn.Listen(1 << 20)
	fake := &fakeProvisioner{}
	go func() {
		_ = RunProvisionerOn(func(_ context.Context, config map[string]string) (integration.Provisioner, error) {
			fake.config = config
			return fake, nil
		}, fakeResolver{}, listener)
	}()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	p, err := client.NewProvisioner(ctx, conn, map[string]string{"name": "x"})
	require.NoError(t, err)
	require.NoError(t, p.Ping(ctx))

	res, err := p.Provision(ctx, integration.ProvisionRequest{Labels: map[string]string{"plakman.job": "42"}})
	require.NoError(t, err)
	require.Equal(t, "h-42", res.Handle)
	require.Equal(t, "fs", res.Protocol)
	require.Equal(t, map[string]string{"location": "fs:///tmp/x"}, res.Target)

	require.NoError(t, p.Deprovision(ctx, res.Handle))
	require.Equal(t, "h-42", fake.deprovisioned)

	r := client.NewResolver(conn)
	opts, err := r.Resolve(ctx, integration.ResolveRequest{
		Resolver: "zones",
		Params:   map[string]string{"region": "fr-par"},
		Filters:  map[string][]string{"country": {"FR"}},
	})
	require.NoError(t, err)
	require.Equal(t, integration.SearchLocal, opts.Search)
	require.Len(t, opts.Options, 1)
	require.Equal(t, "fr-par", opts.Options[0].Attributes["region"])
	require.Equal(t, "neutral", opts.Columns[0].Tones["FR"])
	require.Equal(t, []string{"FR", "NL"}, opts.Filters[0].Values)

	_, err = r.Resolve(ctx, integration.ResolveRequest{Resolver: "nope"})
	require.Error(t, err)

	require.NoError(t, p.Close(ctx))
}
