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

package server

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleConnListener(t *testing.T) {
	ours, theirs := net.Pipe()
	defer theirs.Close()

	notify := make(chan struct{})
	lis := &singleConnListener{notify: notify, conn: ours}
	require.Equal(t, ours.LocalAddr(), lis.Addr())

	conn, err := lis.Accept()
	require.NoError(t, err)
	require.Same(t, ours, conn)

	// the second Accept parks until the connection is gone
	accepted := make(chan error, 1)
	go func() {
		_, err := lis.Accept()
		accepted <- err
	}()
	select {
	case err := <-accepted:
		require.Fail(t, "Accept returned a second time", "%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(notify)
	select {
	case err := <-accepted:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(5 * time.Second):
		require.Fail(t, "Accept is still blocked after the connection closed")
	}

	require.NoError(t, lis.Close())
	_, err = ours.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe, "Close did not close the connection")
}
