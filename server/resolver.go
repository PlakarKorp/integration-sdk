package server

import (
	"context"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/resolver"
	"google.golang.org/grpc"
)

type resolverServer struct {
	resolver.UnimplementedResolverServer
	resolver integration.Resolver
}

// RegisterResolver serves a resolver next to a connector's own service.
func RegisterResolver(server *grpc.Server, r integration.Resolver) {
	resolver.RegisterResolverServer(server, &resolverServer{resolver: r})
}

func (g *resolverServer) Resolve(ctx context.Context, req *resolver.ResolveRequest) (*resolver.ResolveResponse, error) {
	filters := make(map[string][]string, len(req.Filters))
	for k, v := range req.Filters {
		filters[k] = v.GetValues()
	}
	res, err := g.resolver.Resolve(ctx, integration.ResolveRequest{
		Resolver: req.Resolver,
		Params:   req.Params,
		Search:   req.Search,
		Filters:  filters,
		Values:   req.Values,
		Cursor:   req.Cursor,
		Limit:    int(req.Limit),
	})
	if err != nil {
		return nil, err
	}
	out := &resolver.ResolveResponse{
		NextCursor: res.NextCursor,
		Search:     string(res.Search),
	}
	for _, c := range res.Columns {
		out.Columns = append(out.Columns, &resolver.Column{
			Key:       c.Key,
			Label:     c.Label,
			Type:      string(c.Type),
			Unit:      c.Unit,
			Currency:  c.Currency,
			Per:       c.Per,
			Secondary: c.Secondary,
			Hidden:    c.Hidden,
			Tones:     c.Tones,
		})
	}
	for _, o := range res.Options {
		out.Options = append(out.Options, &resolver.Option{
			Value:       o.Value,
			Label:       o.Label,
			Description: o.Description,
			State:       string(o.State),
			Reason:      o.Reason,
			Attributes:  o.Attributes,
		})
	}
	for _, f := range res.Filters {
		out.Filters = append(out.Filters, &resolver.Filter{Key: f.Key, Values: f.Values})
	}
	return out, nil
}
