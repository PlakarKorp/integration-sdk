package proto

import (
	"io"
	"sync"

	"github.com/PlakarKorp/integration-sdk"
)

type HoldingReaders struct {
	mu      sync.Mutex
	readers map[string]io.ReadCloser
}

func NewHoldingReaders() *HoldingReaders {
	return &HoldingReaders{
		readers: make(map[string]io.ReadCloser),
	}
}

func recordToPath(record *integration.Record) string {
	pathname := record.Pathname

	if record.IsXattr {
		sep := ":"
		if record.XattrType == integration.AttributeADS {
			sep = "@"
		}
		if record.XattrType == integration.AttributeCustom {
			sep = "%"
		}
		pathname = "xattr:" + pathname + sep + record.XattrName
	}

	return pathname
}

func (h *HoldingReaders) Track(record *integration.Record) {
	pathname := recordToPath(record)
	h.mu.Lock()
	h.readers[pathname] = record.Reader
	h.mu.Unlock()
}

func (h *HoldingReaders) Get(record *integration.Record) io.ReadCloser {
	pathname := recordToPath(record)

	h.mu.Lock()
	rd, ok := h.readers[pathname]
	if ok {
		delete(h.readers, pathname)
	}
	h.mu.Unlock()
	return rd
}

func (h *HoldingReaders) Close() (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, rd := range h.readers {
		if cerr := rd.Close(); err == nil {
			err = cerr
		}
	}

	return err
}
