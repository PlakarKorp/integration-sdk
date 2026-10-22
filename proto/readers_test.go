package proto

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

type countingReadCloser struct {
	io.Reader
	closed atomic.Bool
	err    error
}

func (c *countingReadCloser) Close() error {
	c.closed.Store(true)
	return c.err
}

func TestHoldingReaders_TrackGetDelete(t *testing.T) {
	h := NewHoldingReaders()

	rc := &countingReadCloser{Reader: strings.NewReader("hi")}
	rec := &connectors.Record{
		Pathname: "/etc/hello.txt",
		Reader:   rc,
	}

	h.Track(rec)
	require.Same(t, rc, h.Get(rec), "Get did not return the same reader we Tracked")
	// Second Get must return nil (entry removed)
	require.Nil(t, h.Get(rec), "Get returned a non-nil reader after entry was claimed")
}

func TestHoldingReaders_XattrKeySeparators(t *testing.T) {
	h := NewHoldingReaders()

	r1 := &countingReadCloser{Reader: strings.NewReader("a")}
	r2 := &countingReadCloser{Reader: strings.NewReader("b")}
	r3 := &countingReadCloser{Reader: strings.NewReader("c")}

	ext := &connectors.Record{
		Pathname:  "/file",
		IsXattr:   true,
		XattrName: "user.x",
		XattrType: objects.AttributeExtended,
		Reader:    r1,
	}
	ads := &connectors.Record{
		Pathname:  "/file",
		IsXattr:   true,
		XattrName: "user.x",
		XattrType: objects.AttributeADS,
		Reader:    r2,
	}
	custom := &connectors.Record{
		Pathname:  "/file",
		IsXattr:   true,
		XattrName: "user.x",
		XattrType: objects.AttributeCustom,
		Reader:    r3,
	}

	h.Track(ext)
	h.Track(ads)
	h.Track(custom)

	require.Same(t, r1, h.Get(ext), "extended xattr lookup got the wrong reader")
	require.Same(t, r2, h.Get(ads), "ADS xattr lookup got the wrong reader")
	require.Same(t, r3, h.Get(custom), "Custom xattr lookup got the wrong reader")
}

func TestHoldingReaders_CloseDrainsRemaining(t *testing.T) {
	h := NewHoldingReaders()

	rc1 := &countingReadCloser{Reader: strings.NewReader("x")}
	rc2 := &countingReadCloser{Reader: strings.NewReader("y"), err: errors.New("boom")}

	h.Track(&connectors.Record{Pathname: "/a", Reader: rc1})
	h.Track(&connectors.Record{Pathname: "/b", Reader: rc2})

	err := h.Close()
	require.True(t, rc1.closed.Load(), "Close did not close all tracked readers")
	require.True(t, rc2.closed.Load(), "Close did not close all tracked readers")
	require.Error(t, err, "Close should propagate the first non-nil close error")
}

func TestHoldingReaders_ConcurrentTrackGet(t *testing.T) {
	h := NewHoldingReaders()

	const N = 256
	var wg sync.WaitGroup
	wg.Add(2 * N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			rec := &connectors.Record{
				Pathname: pathFor(i),
				Reader:   &countingReadCloser{Reader: strings.NewReader("x")},
			}
			h.Track(rec)
		}()
		go func() {
			defer wg.Done()
			rec := &connectors.Record{Pathname: pathFor(i)}
			_ = h.Get(rec)
		}()
	}
	wg.Wait()
}

func pathFor(i int) string {
	return "/p/" + itoa(i)
}

func itoa(n int) string {
	// avoid strconv allocations in tight loops; tiny inline helper
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
