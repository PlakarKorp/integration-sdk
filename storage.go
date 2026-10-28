package integration

import (
	"context"
	"io"
	"iter"
	"time"

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

type StorageFlag uint32

const (
	StorageHot StorageFlag = 1 << iota
)

type Range struct {
	Offset uint64
	Length uint32
}

type Resource struct {
	MAC   MAC
	Size  int64     // -1 if unknown
	MTime time.Time // zero if not known
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
	Size(context.Context) (int64, error) // this can be costly, call with caution // XXX remove?
	List(context.Context, StorageResource, StorageFlag) iter.Seq2[Resource, error]
	Put(context.Context, StorageResource, StorageFlag, MAC, io.Reader) (int64, error)
	Get(context.Context, StorageResource, StorageFlag, MAC, *Range) (io.ReadCloser, error)
	Delete(context.Context, StorageResource, StorageFlag, MAC) error

	Close(ctx context.Context) error
}

type StoreFn func(context.Context, string, map[string]string) (Store, error)
