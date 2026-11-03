//go:build !windows

/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package stdioconn

import (
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// starveFDs leaves room for exactly n more file descriptors by filling
// the holes in the table and capping the soft limit, until the test
// ends.
func starveFDs(t *testing.T, n uint64) {
	t.Helper()

	var saved syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &saved))
	t.Cleanup(func() { syscall.Setrlimit(syscall.RLIMIT_NOFILE, &saved) })

	entries, err := os.ReadDir("/dev/fd")
	require.NoError(t, err)
	var highest uint64
	for _, e := range entries {
		if fd, err := strconv.ParseUint(e.Name(), 10, 64); err == nil && fd > highest {
			highest = fd
		}
	}

	limit := syscall.Rlimit{Cur: highest + 1, Max: saved.Max}
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit))

	// fill the holes below the limit
	for {
		f, err := os.Open(os.DevNull)
		if err != nil {
			break
		}
		t.Cleanup(func() { f.Close() })
	}

	limit.Cur += n
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit))
}

func TestSpawnPipeFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		room uint64
	}{
		{"first pipe", 0},
		{"second pipe", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			starveFDs(t, tc.room)

			_, err := Spawn(cat())
			require.ErrorIs(t, err, syscall.EMFILE)
		})
	}
}
