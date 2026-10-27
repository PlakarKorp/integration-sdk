package integration

import (
	"context"

	"github.com/PlakarKorp/integration-sdk/location"
)

type Importer interface {
	Origin() string
	Type() string
	Root() string
	Flags() location.Flags
	Ping(context.Context) error
	Import(context.Context, chan<- *Record, <-chan *Result) error
	Close(context.Context) error
}

type ImporterFn func(context.Context, *Options, string, map[string]string) (Importer, error)
