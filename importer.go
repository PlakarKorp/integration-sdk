package integration

import (
	"context"
	"fmt"

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

var importerBackends = location.New[ImporterFn]("fs")

func RegisterImporter(name string, flags location.Flags, backend ImporterFn) error {
	if !importerBackends.Register(name, backend, flags) {
		return fmt.Errorf("importer backend '%s' already registered", name)
	}
	return nil
}

func UnregisterImporter(name string) error {
	if !importerBackends.Unregister(name) {
		return fmt.Errorf("importer backend '%s' not registered", name)
	}
	return nil
}

func ImporterBackends() []string {
	return importerBackends.Names()
}
