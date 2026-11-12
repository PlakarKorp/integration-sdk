// Package storagetest is a conformance suite for integration.Store
// implementations.
//
// It checks the properties kloset relies on from a store, which object
// stores (S3, GCS, ...) provide for free but filesystem-like backends (fs,
// sftp, smb, nfs, ...) have to implement explicitly:
//
//   - a Put is atomic: a key is either absent or complete, never partial;
//   - a Put is visible to every other client as soon as it returns, and so
//     is a Delete;
//   - a Get of a missing key fails with an error matching fs.ErrNotExist;
//   - a Delete of a missing key succeeds;
//   - a re-Put of an existing key with the same content succeeds (retries);
//   - locks can be overwritten, even while another client is reading them;
//   - a key can be deleted while another client is reading it;
//   - ranged Gets return exactly the requested bytes;
//   - a Store is safe for concurrent use.
//
// Packfiles and states are never overwritten with a different content, so
// the suite does not check what happens then.
//
// Durability (data on stable storage when Put returns) and retries across a
// real reconnect cannot be checked from here: they need fault injection
// specific to each backend.
package storagetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/stretchr/testify/require"
)

// Env provides the suite with the store under test.
type Env interface {
	// NewLocation returns the configuration of a fresh, empty repository
	// location. Each call must return a distinct location.
	NewLocation(t *testing.T) map[string]string

	// Open returns a new client for the location described by cfg,
	// independent from any other client: its own connection or session,
	// as a second host would have.  The suite calls Create or Open and
	// Close on it.
	Open(t *testing.T, cfg map[string]string) integration.Store
}

// resources are the store resources kloset reads and writes.
var resources = []integration.StorageResource{
	integration.StorageResourcePackfile,
	integration.StorageResourceState,
	integration.StorageResourceLock,
}

// immutables are the resources that are never overwritten.
var immutables = []integration.StorageResource{
	integration.StorageResourcePackfile,
	integration.StorageResourceState,
}

// Run runs the whole suite against env.
func Run(t *testing.T, env Env) {
	tests := []struct {
		name string
		fn   func(*testing.T, *pair)
	}{
		{"Mode", testMode},
		{"RoundTrip", testRoundTrip},
		{"Range", testRange},
		{"GetMissing", testGetMissing},
		{"DeleteMissing", testDeleteMissing},
		{"PutIdempotent", testPutIdempotent},
		{"ListExact", testListExact},
		{"Visibility", testVisibility},
		{"OverwriteLock", testOverwriteLock},
		{"OverwriteLockWhileOpen", testOverwriteLockWhileOpen},
		{"NoPartialPut", testNoPartialPut},
		{"NoTornRead", testNoTornRead},
		{"DeleteWhileReading", testDeleteWhileReading},
		{"Concurrent", testConcurrent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newPair(t, env))
		})
	}
}

// pair is two independent clients, a and b, on the same fresh repository.
type pair struct {
	ctx  context.Context
	a, b integration.Store
}

func newPair(t *testing.T, env Env) *pair {
	ctx := t.Context()
	cfg := env.NewLocation(t)

	// StoreFn implementations are allowed to rewrite their config.
	a := env.Open(t, maps.Clone(cfg))
	t.Cleanup(func() { a.Close(context.Background()) })

	config := randBytes(256)
	require.NoError(t, a.Create(ctx, config), "Create")

	b := env.Open(t, maps.Clone(cfg))
	t.Cleanup(func() { b.Close(context.Background()) })

	got, err := b.Open(ctx)
	require.NoError(t, err, "Open")
	require.Equal(t, config, got, "Open must return the configuration given to Create")

	return &pair{ctx: ctx, a: a, b: b}
}

func testMode(t *testing.T, p *pair) {
	mode, err := p.a.Mode(p.ctx)
	require.NoError(t, err)
	require.Equal(t, integration.ModeRead|integration.ModeWrite, mode&(integration.ModeRead|integration.ModeWrite),
		"the suite needs a read-write store")
}

func testRoundTrip(t *testing.T, p *pair) {
	for _, res := range resources {
		for _, size := range []int{0, 1, 4096, 3<<20 + 7} {
			data := randBytes(size)
			mac := macOf(data)

			n, err := p.a.Put(p.ctx, res, 0, mac, bytes.NewReader(data))
			require.NoError(t, err, "%s: Put %d bytes", res, size)
			require.EqualValues(t, size, n, "%s: Put must return the number of bytes written", res)

			require.Equal(t, data, get(t, p.ctx, p.a, res, mac, nil), "%s: %d bytes", res, size)
		}
	}
}

func testRange(t *testing.T, p *pair) {
	const size = 9 << 20

	data := randBytes(size)
	mac := put(t, p.ctx, p.a, integration.StorageResourcePackfile, data)

	for _, rg := range []integration.Range{
		{Offset: 0, Length: 1},
		{Offset: 0, Length: 4096},
		{Offset: 1<<20 - 1, Length: 2},       // straddles a 1MB boundary
		{Offset: 123457, Length: 4 << 20},    // merged read, unaligned
		{Offset: 1<<20 + 3, Length: 6 << 20}, // single blob larger than 4MB
		{Offset: size - 4096, Length: 4096},  // ends exactly at EOF
		{Offset: size - 1, Length: 1},        // last byte
		{Offset: 0, Length: size},            // everything
	} {
		got := get(t, p.ctx, p.b, integration.StorageResourcePackfile, mac, &rg)
		want := data[rg.Offset : rg.Offset+uint64(rg.Length)]
		require.Equal(t, len(want), len(got), "range %+v: wrong length", rg)
		require.True(t, bytes.Equal(want, got), "range %+v: wrong content", rg)
	}
}

func testGetMissing(t *testing.T, p *pair) {
	for _, res := range resources {
		rd, err := p.a.Get(p.ctx, res, 0, randMAC(), nil)
		requireNotExist(t, rd, err, "%s: Get of a missing key", res)
	}

	rd, err := p.a.Get(p.ctx, integration.StorageResourcePackfile, 0, randMAC(), &integration.Range{Offset: 0, Length: 16})
	requireNotExist(t, rd, err, "packfile: ranged Get of a missing key")
}

func testDeleteMissing(t *testing.T, p *pair) {
	for _, res := range resources {
		require.NoError(t, p.a.Delete(p.ctx, res, 0, randMAC()), "%s: Delete of a missing key", res)

		// And the retry of a Delete that succeeded.
		mac := put(t, p.ctx, p.a, res, randBytes(64))
		require.NoError(t, p.a.Delete(p.ctx, res, 0, mac), "%s: Delete", res)
		require.NoError(t, p.a.Delete(p.ctx, res, 0, mac), "%s: second Delete of the same key", res)
	}
}

func testPutIdempotent(t *testing.T, p *pair) {
	for _, res := range resources {
		data := randBytes(64 << 10)
		mac := put(t, p.ctx, p.a, res, data)

		// The retry of a Put that succeeded, possibly from another client.
		n, err := p.b.Put(p.ctx, res, 0, mac, bytes.NewReader(data))
		require.NoError(t, err, "%s: second Put of the same key and content", res)
		require.EqualValues(t, len(data), n, "%s", res)

		require.Equal(t, data, get(t, p.ctx, p.a, res, mac, nil), "%s", res)
		require.Equal(t, 1, list(t, p.ctx, p.a, res)[mac], "%s: List must return a key once", res)
	}
}

func testListExact(t *testing.T, p *pair) {
	for _, res := range resources {
		require.Empty(t, list(t, p.ctx, p.a, res), "%s: List on a fresh repository", res)
	}

	want := map[integration.StorageResource]map[integration.MAC]int{}
	for _, res := range resources {
		want[res] = map[integration.MAC]int{}
		for i := range 5 {
			s := p.a
			if i%2 == 1 {
				s = p.b
			}
			data := randBytes(1000 + i)
			want[res][put(t, p.ctx, s, res, data)] = len(data)
		}
	}

	for _, res := range resources {
		for name, s := range map[string]integration.Store{"a": p.a, "b": p.b} {
			seen := map[integration.MAC]int{}
			for e, err := range s.List(p.ctx, res, 0) {
				require.NoError(t, err, "%s: List from %s", res, name)
				seen[e.MAC]++
				if e.Size != 0 {
					require.EqualValues(t, want[res][e.MAC], e.Size, "%s: List from %s: size of %x", res, name, e.MAC)
				}
			}

			require.Equal(t, sortedMACs(want[res]), sortedMACs(seen),
				"%s: List from %s must return exactly the keys put", res, name)
			for mac, n := range seen {
				require.Equal(t, 1, n, "%s: List from %s returned %x %d times", res, name, mac, n)
			}
		}
	}
}

func testVisibility(t *testing.T, p *pair) {
	for _, res := range resources {
		data := randBytes(4096)
		mac := put(t, p.ctx, p.a, res, data)

		require.Contains(t, list(t, p.ctx, p.b, res), mac, "%s: a Put must be listed by other clients right away", res)
		require.Equal(t, data, get(t, p.ctx, p.b, res, mac, nil), "%s: a Put must be readable by other clients right away", res)

		require.NoError(t, p.a.Delete(p.ctx, res, 0, mac))

		require.NotContains(t, list(t, p.ctx, p.b, res), mac, "%s: a Delete must be seen by other clients' List right away", res)
		rd, err := p.b.Get(p.ctx, res, 0, mac, nil)
		requireNotExist(t, rd, err, "%s: a Delete must be seen by other clients' Get right away", res)
	}
}

func testOverwriteLock(t *testing.T, p *pair) {
	const res = integration.StorageResourceLock

	mac := randMAC()
	v1, v2 := randBytes(300), randBytes(200) // v2 shorter, to catch leftovers

	_, err := p.a.Put(p.ctx, res, 0, mac, bytes.NewReader(v1))
	require.NoError(t, err)

	_, err = p.a.Put(p.ctx, res, 0, mac, bytes.NewReader(v2))
	require.NoError(t, err, "lock refresh: Put over an existing lock")

	require.Equal(t, v2, get(t, p.ctx, p.a, res, mac, nil))
	require.Equal(t, v2, get(t, p.ctx, p.b, res, mac, nil))
	require.Equal(t, 1, list(t, p.ctx, p.b, res)[mac])
}

func testOverwriteLockWhileOpen(t *testing.T, p *pair) {
	const res = integration.StorageResourceLock

	mac := randMAC()
	v1, v2 := randBytes(300), randBytes(200)

	_, err := p.a.Put(p.ctx, res, 0, mac, bytes.NewReader(v1))
	require.NoError(t, err)

	// Another host is in the middle of reading the lock.
	rd, err := p.b.Get(p.ctx, res, 0, mac, nil)
	require.NoError(t, err)
	defer rd.Close()

	head := make([]byte, 1)
	_, err = io.ReadFull(rd, head)
	require.NoError(t, err)

	_, err = p.a.Put(p.ctx, res, 0, mac, bytes.NewReader(v2))
	require.NoError(t, err, "lock refresh: Put over a lock another client is reading")

	// The reader sees the old version or fails, never a mix.
	if rest, err := io.ReadAll(rd); err == nil {
		require.Equal(t, v1, append(head, rest...), "a reader must keep seeing the version it opened")
	}
	rd.Close()

	require.Equal(t, v2, get(t, p.ctx, p.b, res, mac, nil))
}

var errInjected = errors.New("storagetest: injected read error")

func testNoPartialPut(t *testing.T, p *pair) {
	for _, res := range resources {
		mac := randMAC()
		rd := io.MultiReader(bytes.NewReader(randBytes(1<<20)), iotestErrReader{errInjected})

		_, err := p.a.Put(p.ctx, res, 0, mac, rd)
		require.Error(t, err, "%s: Put must fail when its reader fails", res)

		for name, s := range map[string]integration.Store{"a": p.a, "b": p.b} {
			require.NotContains(t, list(t, p.ctx, s, res), mac, "%s: a failed Put must not be listed (%s)", res, name)
			rd, err := s.Get(p.ctx, res, 0, mac, nil)
			requireNotExist(t, rd, err, "%s: a failed Put must not be readable (%s)", res, name)
		}
	}

	// Nothing a failed Put might have left behind shows up.
	for _, res := range resources {
		require.Empty(t, list(t, p.ctx, p.b, res), "%s: leftovers of failed Puts are listed", res)
	}
}

func testNoTornRead(t *testing.T, p *pair) {
	for _, res := range resources {
		data := randBytes(4 << 20)
		half := len(data) / 2
		mac := randMAC()

		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() {
			_, err := p.a.Put(p.ctx, res, 0, mac, pr)
			// Unblock the writer if Put gave up early.
			pr.CloseWithError(fmt.Errorf("Put returned: %v", err))
			done <- err
		}()

		// Once Write returns, Put has consumed the first half.
		_, err := pw.Write(data[:half])
		require.NoError(t, err)

		require.NotContains(t, list(t, p.ctx, p.b, res), mac, "%s: a Put in progress must not be listed", res)
		rd, err := p.b.Get(p.ctx, res, 0, mac, nil)
		requireNotExist(t, rd, err, "%s: a Put in progress must not be readable", res)

		_, err = pw.Write(data[half:])
		require.NoError(t, err)
		require.NoError(t, pw.Close())
		require.NoError(t, <-done, "%s: Put", res)

		require.Equal(t, data, get(t, p.ctx, p.b, res, mac, nil), "%s", res)
	}
}

func testDeleteWhileReading(t *testing.T, p *pair) {
	for _, res := range immutables {
		data := randBytes(2 << 20)
		mac := put(t, p.ctx, p.a, res, data)

		// A restore or check on another host is reading it.
		rd, err := p.b.Get(p.ctx, res, 0, mac, nil)
		require.NoError(t, err)

		head := make([]byte, 4096)
		_, err = io.ReadFull(rd, head)
		require.NoError(t, err)
		require.Equal(t, data[:len(head)], head)

		require.NoError(t, p.a.Delete(p.ctx, res, 0, mac), "%s: Delete while another client is reading", res)

		// The key is gone at once, not when the reader closes.
		require.NotContains(t, list(t, p.ctx, p.a, res), mac, "%s: deleted key still listed while a reader is open", res)
		r2, err := p.a.Get(p.ctx, res, 0, mac, nil)
		requireNotExist(t, r2, err, "%s: deleted key still readable while a reader is open", res)

		// The reader may fail, but never returns wrong bytes.
		rest, err := io.ReadAll(rd)
		if err == nil {
			require.True(t, bytes.Equal(data[len(head):], rest), "%s: reader returned wrong bytes after Delete", res)
		} else {
			require.True(t, bytes.HasPrefix(data[len(head):], rest), "%s: reader returned wrong bytes after Delete", res)
		}
		rd.Close()
	}
}

func testConcurrent(t *testing.T, p *pair) {
	const res = integration.StorageResourcePackfile

	// Concurrent ranged reads of one packfile, as GetBlobs does.
	data := randBytes(6 << 20)
	mac := put(t, p.ctx, p.a, res, data)

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			rg := integration.Range{Offset: uint64(i) * 150001, Length: 1<<20 + uint32(i)}
			rd, err := p.b.Get(p.ctx, res, 0, mac, &rg)
			if err != nil {
				t.Errorf("Get %+v: %v", rg, err)
				return
			}
			defer rd.Close()
			got, err := io.ReadAll(rd)
			if err != nil {
				t.Errorf("Get %+v: read: %v", rg, err)
				return
			}
			if !bytes.Equal(data[rg.Offset:rg.Offset+uint64(rg.Length)], got) {
				t.Errorf("Get %+v: wrong content (%d bytes)", rg, len(got))
			}
		})
	}
	wg.Wait()

	// Concurrent Puts, as the packer does.
	var mu sync.Mutex
	want := map[integration.MAC][]byte{mac: data}
	for range 16 {
		wg.Go(func() {
			d := randBytes(256 << 10)
			m := macOf(d)
			if _, err := p.a.Put(p.ctx, res, 0, m, bytes.NewReader(d)); err != nil {
				t.Errorf("Put %x: %v", m, err)
				return
			}
			mu.Lock()
			want[m] = d
			mu.Unlock()
		})
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	require.Equal(t, sortedMACs(want), sortedMACs(list(t, p.ctx, p.b, res)))
	for m, d := range want {
		require.Equal(t, d, get(t, p.ctx, p.b, res, m, nil))
	}
}

// helpers

type iotestErrReader struct{ err error }

func (r iotestErrReader) Read([]byte) (int, error) { return 0, r.err }

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func randMAC() integration.MAC {
	var m integration.MAC
	rand.Read(m[:])
	return m
}

func macOf(data []byte) integration.MAC {
	return integration.MAC(sha256.Sum256(data))
}

func put(t *testing.T, ctx context.Context, s integration.Store, res integration.StorageResource, data []byte) integration.MAC {
	t.Helper()
	mac := macOf(data)
	if res == integration.StorageResourceLock {
		mac = randMAC()
	}
	_, err := s.Put(ctx, res, 0, mac, bytes.NewReader(data))
	require.NoError(t, err, "%s: Put", res)
	return mac
}

func get(t *testing.T, ctx context.Context, s integration.Store, res integration.StorageResource, mac integration.MAC, rg *integration.Range) []byte {
	t.Helper()
	rd, err := s.Get(ctx, res, 0, mac, rg)
	require.NoError(t, err, "%s: Get %x", res, mac)
	defer rd.Close()
	data, err := io.ReadAll(rd)
	require.NoError(t, err, "%s: Get %x: read", res, mac)
	return data
}

func list(t *testing.T, ctx context.Context, s integration.Store, res integration.StorageResource) map[integration.MAC]int {
	t.Helper()
	ret := map[integration.MAC]int{}
	for e, err := range s.List(ctx, res, 0) {
		require.NoError(t, err, "%s: List", res)
		ret[e.MAC]++
	}
	return ret
}

func sortedMACs[V any](m map[integration.MAC]V) []integration.MAC {
	return slices.SortedFunc(maps.Keys(m), func(a, b integration.MAC) int { return bytes.Compare(a[:], b[:]) })
}

// requireNotExist checks that Get itself failed with fs.ErrNotExist: an
// error surfacing only on the first Read does not count.
func requireNotExist(t *testing.T, rd io.ReadCloser, err error, msgAndArgs ...any) {
	t.Helper()
	if err == nil {
		rd.Close()
	}
	require.Error(t, err, msgAndArgs...)
	require.ErrorIs(t, err, fs.ErrNotExist, msgAndArgs...)
}
