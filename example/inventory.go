package example

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/integration-sdk"
	"golang.org/x/sync/errgroup"
)

type example struct {
	// ...
}

func NewInventory(ctx context.Context, params map[string]string) (integration.Inventory, error) {
	var ex example

	// inspect and consume params
	for k, v := range params {
		_ = v
		switch k {
		case "example_param1":
			// validate v
		case "example_param2":
			// ditto
		}
	}

	return &ex, nil
}

func (ex *example) List(ctx context.Context, resources chan<- *integration.InventoryEntry) error {
	defer close(resources)

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error { return ex.listFoos(ctx, resources) })
	wg.Go(func() error { return ex.listBars(ctx, resources) })

	return wg.Wait()
}

func (ex *example) listFoos(ctx context.Context, resources chan<- *integration.InventoryEntry) error {
	for i := range 3 {
		resources <- &integration.InventoryEntry{
			Class:    integration.ResourceClassCompute,
			SubClass: integration.ResourceSubClassUnknown,
			Tags:     []string{"foo", "bar", "baz"},
			URN:      fmt.Sprintf("example:foo:foo-%d", i),
			Name:     fmt.Sprintf("example-foo-%d", i),
			Region:   "eu",
			Service:  "foo",
			Country:  integration.Country("FR"),
			Endpoints: []integration.HostEndpoint{{
				Type:     integration.EndpointIdentifier,
				Endpoint: fmt.Sprintf("foo-%d", i),
			}},
		}
	}
	return nil
}

func (ex *example) listBars(ctx context.Context, resources chan<- *integration.InventoryEntry) error {
	// TODO
	return nil
}

func (ex *example) Close(ctx context.Context) error {
	return nil
}
