package integration

import (
	"context"
	"fmt"

	"github.com/PlakarKorp/integration-sdk/location"
)

type Exporter interface {
	Origin() string
	Type() string
	Root() string
	Flags() location.Flags
	Ping(context.Context) error
	Export(context.Context, <-chan *Record, chan<- *Result) error
	Close(context.Context) error
}

type ExporterFn func(context.Context, *Options, string, map[string]string) (Exporter, error)

var exporterBackends = location.New[ExporterFn]("fs")

func RegisterExporter(name string, flags location.Flags, backend ExporterFn) error {
	if !exporterBackends.Register(name, backend, flags) {
		return fmt.Errorf("exporter backend '%s' already registered", name)
	}
	return nil
}

func UnregisterExporter(name string) error {
	if !exporterBackends.Unregister(name) {
		return fmt.Errorf("exporter backend '%s' not registered", name)
	}
	return nil
}

func ExporterBackends() []string {
	return exporterBackends.Names()
}
