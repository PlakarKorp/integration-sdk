package client

import (
	"context"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/proto/resolver"
	"google.golang.org/grpc"
)

type resolverClient struct {
	client resolver.ResolverClient
}

// NewResolver talks to the resolver a connector serves next to its own
// service. No Init: a resolution carries everything it needs.
func NewResolver(conn grpc.ClientConnInterface) integration.Resolver {
	return &resolverClient{client: resolver.NewResolverClient(conn)}
}

func (g *resolverClient) Resolve(ctx context.Context, req integration.ResolveRequest) (*integration.ResolveResponse, error) {
	filters := make(map[string]*resolver.StringList, len(req.Filters))
	for k, v := range req.Filters {
		filters[k] = &resolver.StringList{Values: v}
	}
	res, err := g.client.Resolve(ctx, &resolver.ResolveRequest{
		Resolver: req.Resolver,
		Params:   req.Params,
		Search:   req.Search,
		Filters:  filters,
		Values:   req.Values,
		Cursor:   req.Cursor,
		Limit:    int32(req.Limit),
	})
	if err != nil {
		return nil, unwrap(err)
	}
	out := &integration.ResolveResponse{
		NextCursor: res.NextCursor,
		Search:     integration.SearchMode(res.Search),
	}
	for _, c := range res.Columns {
		out.Columns = append(out.Columns, integration.Column{
			Key:       c.Key,
			Label:     c.Label,
			Type:      integration.ColumnType(c.Type),
			Unit:      c.Unit,
			Currency:  c.Currency,
			Per:       c.Per,
			Secondary: c.Secondary,
			Hidden:    c.Hidden,
			Tones:     c.Tones,
		})
	}
	for _, o := range res.Options {
		out.Options = append(out.Options, integration.Option{
			Value:       o.Value,
			Label:       o.Label,
			Description: o.Description,
			State:       integration.OptionState(o.State),
			Reason:      o.Reason,
			Attributes:  o.Attributes,
		})
	}
	for _, f := range res.Filters {
		out.Filters = append(out.Filters, integration.ResolveFilter{Key: f.Key, Values: f.Values})
	}
	return out, nil
}
