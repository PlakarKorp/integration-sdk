package integration

import (
	"context"
	"io"

	"github.com/PlakarKorp/integration-sdk/location"
)

type Mode uint32

const (
	ModeWrite Mode = 1 << 1
	ModeRead  Mode = 1 << 2
)

type StorageResource uint32

const (
	StorageResourceUndefined   StorageResource = 0
	StorageResourcePackfile    StorageResource = 1
	StorageResourceState       StorageResource = 2
	StorageResourceLock        StorageResource = 3
	StorageResourceECCPackfile StorageResource = 4
	StorageResourceECCState    StorageResource = 5
)

func (s StorageResource) String() string {
	switch s {
	case StorageResourceUndefined:
		return "undefined"
	case StorageResourcePackfile:
		return "packfile"
	case StorageResourceState:
		return "state"
	case StorageResourceLock:
		return "lock"
	case StorageResourceECCPackfile:
		return "ECC packfile"
	case StorageResourceECCState:
		return "ECC state"
	default:
		return "unknown"
	}
}

// Pass a flag to the storage, (ab)using the context. When we go for a new
// version of the protocol roll this out as a parameter.
type flagCtxKey struct{}

type StorageFlag uint32

const (
	StorageHot StorageFlag = 1 << iota
)

func WithFlag(ctx context.Context, flag StorageFlag) context.Context {
	return context.WithValue(ctx, flagCtxKey{}, flag)
}

func Flag(ctx context.Context) StorageFlag {
	fl, _ := ctx.Value(flagCtxKey{}).(StorageFlag)
	return fl
}

type Range struct {
	Offset uint64
	Length uint32
}

type Store interface {
	Create(context.Context, []byte) error
	Open(context.Context) ([]byte, error)
	Ping(context.Context) error

	Origin() string
	Type() string
	Root() string
	Flags() location.Flags

	Mode(context.Context) (Mode, error)
	Size(context.Context) (int64, error) // this can be costly, call with caution
	List(context.Context, StorageResource) ([]MAC, error)
	Put(context.Context, StorageResource, MAC, io.Reader) (int64, error)
	Get(context.Context, StorageResource, MAC, *Range) (io.ReadCloser, error)
	Delete(context.Context, StorageResource, MAC) error

	Close(ctx context.Context) error
}

type StoreFn func(context.Context, string, map[string]string) (Store, error)
