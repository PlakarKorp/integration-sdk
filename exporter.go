package integration

import (
	"context"

	"github.com/PlakarKorp/integration-sdk/location"
)

type Exporter interface {
	Origin() string
	Type() string
	Root() string
	Flags() location.Flags
	Ping(context.Context) error
	Export(context.Context, map[string]string, <-chan *Record, chan<- *Result) error
	Close(context.Context) error
}

type ExporterFn func(context.Context, *Options, string, map[string]string) (Exporter, error)
