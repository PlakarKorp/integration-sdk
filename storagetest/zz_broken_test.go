//go:build broken

// Deliberately broken stores: each mode must make the suite fail, on the
// subtests listed. Excluded from normal runs (it fails by design); run with:
//
//	go test -tags broken -run TestBroken ./storagetest
//
//	noreplace -> PutIdempotent, OverwriteLock, OverwriteLockWhileOpen
//	inplace   -> NoPartialPut, NoTornRead
//	plainerr  -> GetMissing, Visibility, NoPartialPut, NoTornRead, DeleteWhileReading
//	delerr    -> DeleteMissing
package storagetest

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
)

type brokenStore struct {
	*memStore
	mode string
}

func (s *brokenStore) Put(ctx context.Context, res integration.StorageResource, f integration.StorageFlag, mac integration.MAC, rd io.Reader) (int64, error) {
	switch s.mode {
	case "noreplace":
		// SMB rename without ReplaceIfExists.
		s.repo.mu.Lock()
		_, ok := s.repo.data[res][mac]
		s.repo.mu.Unlock()
		if ok {
			return 0, errors.New("exists")
		}
	case "inplace":
		// Writes straight to the final name, no tmp+rename.
		s.repo.mu.Lock()
		if s.repo.data[res] == nil {
			s.repo.data[res] = map[integration.MAC][]byte{}
		}
		s.repo.data[res][mac] = nil
		s.repo.mu.Unlock()
		buf := make([]byte, 64<<10)
		var n int64
		for {
			k, err := rd.Read(buf)
			s.repo.mu.Lock()
			s.repo.data[res][mac] = append(append([]byte(nil), s.repo.data[res][mac]...), buf[:k]...)
			s.repo.mu.Unlock()
			n += int64(k)
			if err == io.EOF {
				return n, nil
			}
			if err != nil {
				return n, err
			}
		}
	}
	return s.memStore.Put(ctx, res, f, mac, rd)
}

func (s *brokenStore) Get(ctx context.Context, res integration.StorageResource, f integration.StorageFlag, mac integration.MAC, rg *integration.Range) (io.ReadCloser, error) {
	rd, err := s.memStore.Get(ctx, res, f, mac, rg)
	if err != nil && s.mode == "plainerr" {
		// Not-found error that does not match fs.ErrNotExist.
		return nil, errors.New("not found")
	}
	return rd, err
}

func (s *brokenStore) Delete(ctx context.Context, res integration.StorageResource, f integration.StorageFlag, mac integration.MAC) error {
	if s.mode == "delerr" {
		// Delete of a missing key errors.
		s.repo.mu.Lock()
		_, ok := s.repo.data[res][mac]
		s.repo.mu.Unlock()
		if !ok {
			return errors.New("no such file")
		}
	}
	return s.memStore.Delete(ctx, res, f, mac)
}

type brokenEnv struct {
	*memEnv
	mode string
}

func (e brokenEnv) Open(t *testing.T, cfg map[string]string) integration.Store {
	return &brokenStore{memStore: e.memEnv.Open(t, cfg).(*memStore), mode: e.mode}
}

func TestBroken(t *testing.T) {
	for _, mode := range []string{"noreplace", "inplace", "plainerr", "delerr"} {
		t.Run(mode, func(t *testing.T) {
			Run(t, brokenEnv{&memEnv{repos: map[string]*memRepo{}}, mode})
		})
	}
}
