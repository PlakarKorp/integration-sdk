package storagetest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"sync"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/location"
)

// memRepo is the server side: shared by every client of a location.
type memRepo struct {
	mu     sync.Mutex
	config []byte
	data   map[integration.StorageResource]map[integration.MAC][]byte
}

// memStore is a reference implementation of the contract.
type memStore struct {
	repo *memRepo
}

func (s *memStore) Create(_ context.Context, config []byte) error {
	s.repo.mu.Lock()
	defer s.repo.mu.Unlock()
	if s.repo.config != nil {
		return fmt.Errorf("already created")
	}
	s.repo.config = bytes.Clone(config)
	return nil
}

func (s *memStore) Open(context.Context) ([]byte, error) {
	s.repo.mu.Lock()
	defer s.repo.mu.Unlock()
	if s.repo.config == nil {
		return nil, fs.ErrNotExist
	}
	return bytes.Clone(s.repo.config), nil
}

func (s *memStore) Ping(context.Context) error { return nil }
func (s *memStore) Origin() string             { return "memory" }
func (s *memStore) Type() string               { return "memory" }
func (s *memStore) Root() string               { return "/" }
func (s *memStore) Flags() location.Flags      { return 0 }
func (s *memStore) Close(context.Context) error {
	return nil
}

func (s *memStore) Mode(context.Context) (integration.Mode, error) {
	return integration.ModeRead | integration.ModeWrite, nil
}

func (s *memStore) Size(context.Context) (int64, error) {
	s.repo.mu.Lock()
	defer s.repo.mu.Unlock()
	var n int64
	for _, m := range s.repo.data {
		for _, d := range m {
			n += int64(len(d))
		}
	}
	return n, nil
}

func (s *memStore) List(_ context.Context, res integration.StorageResource, _ integration.StorageFlag) iter.Seq2[*integration.StorageResourceEntry, error] {
	s.repo.mu.Lock()
	var entries []*integration.StorageResourceEntry
	for mac, d := range s.repo.data[res] {
		entries = append(entries, &integration.StorageResourceEntry{MAC: mac, Size: int64(len(d))})
	}
	s.repo.mu.Unlock()

	return func(yield func(*integration.StorageResourceEntry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
	}
}

func (s *memStore) Put(_ context.Context, res integration.StorageResource, _ integration.StorageFlag, mac integration.MAC, rd io.Reader) (int64, error) {
	// Buffer everything, then publish: atomic.
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	s.repo.mu.Lock()
	defer s.repo.mu.Unlock()
	if s.repo.data[res] == nil {
		s.repo.data[res] = map[integration.MAC][]byte{}
	}
	s.repo.data[res][mac] = data
	return int64(len(data)), nil
}

func (s *memStore) Get(_ context.Context, res integration.StorageResource, _ integration.StorageFlag, mac integration.MAC, rg *integration.Range) (io.ReadCloser, error) {
	s.repo.mu.Lock()
	data, ok := s.repo.data[res][mac]
	s.repo.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("%s %x: %w", res, mac, fs.ErrNotExist)
	}

	// Stored slices are replaced, never mutated: readers keep their version.
	if rg != nil {
		end := rg.Offset + uint64(rg.Length)
		if end > uint64(len(data)) {
			return nil, fmt.Errorf("%s %x: range %+v out of bounds", res, mac, *rg)
		}
		data = data[rg.Offset:end]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *memStore) Delete(_ context.Context, res integration.StorageResource, _ integration.StorageFlag, mac integration.MAC) error {
	s.repo.mu.Lock()
	defer s.repo.mu.Unlock()
	delete(s.repo.data[res], mac)
	return nil
}

type memEnv struct {
	mu    sync.Mutex
	repos map[string]*memRepo
}

func (e *memEnv) NewLocation(t *testing.T) map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	loc := fmt.Sprintf("memory://%d", len(e.repos))
	e.repos[loc] = &memRepo{data: map[integration.StorageResource]map[integration.MAC][]byte{}}
	return map[string]string{"location": loc}
}

func (e *memEnv) Open(t *testing.T, cfg map[string]string) integration.Store {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &memStore{repo: e.repos[cfg["location"]]}
}

func TestReferenceStore(t *testing.T) {
	Run(t, &memEnv{repos: map[string]*memRepo{}})
}
