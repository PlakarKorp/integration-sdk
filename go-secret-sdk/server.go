package sdk

import "context"

type secretServer struct {
	UnimplementedSecretProviderServer

	provider    Provider
	constructor ProviderFn
}

func (g *secretServer) Init(ctx context.Context, req *InitRequest) (*InitResponse, error) {
	provider, err := g.constructor(ctx, req.Config)
	if err != nil {
		return nil, err
	}

	g.provider = provider
	return &InitResponse{}, nil
}

func (g *secretServer) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	if err := g.provider.Ping(ctx); err != nil {
		return nil, err
	}
	return &PingResponse{}, nil
}

func (g *secretServer) Resolve(ctx context.Context, req *ResolveRequest) (*ResolveResponse, error) {
	secret, err := g.provider.Resolve(ctx, req.Handle)
	if err != nil {
		return nil, err
	}
	return &ResolveResponse{Secret: secret}, nil
}

func (g *secretServer) Close(ctx context.Context, req *CloseRequest) (*CloseResponse, error) {
	if err := g.provider.Close(ctx); err != nil {
		return nil, err
	}
	return &CloseResponse{}, nil
}
