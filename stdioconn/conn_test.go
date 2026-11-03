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
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAddrs(t *testing.T) {
	stdin, stdout, _, _ := pipes(t)
	conn := New(stdin, stdout)
	defer conn.Close()

	require.Equal(t, "unix", conn.LocalAddr().Network())
	require.Equal(t, "stdio", conn.LocalAddr().String())
	require.Equal(t, conn.LocalAddr(), conn.RemoteAddr())
}

func TestDeadlines(t *testing.T) {
	stdin, stdout, _, _ := pipes(t)
	conn := New(stdin, stdout)
	defer conn.Close()

	deadline := time.Now().Add(time.Hour)
	require.NoError(t, conn.SetDeadline(deadline))
	require.NoError(t, conn.SetReadDeadline(deadline))
	require.NoError(t, conn.SetWriteDeadline(deadline))

	// a deadline in the past times the read out instead of blocking
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(-time.Second)))
	_, err := conn.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)

	// once stdin is gone there is nothing to set the deadline on
	require.NoError(t, stdin.Close())
	require.ErrorContains(t, conn.SetDeadline(deadline), "closed")
}

// Closing a Conn whose files are already closed reports it, and still
// waits for the command.
func TestCloseReportsClosedFiles(t *testing.T) {
	stdin, stdout, _, _ := pipes(t)
	require.NoError(t, stdin.Close())
	require.NoError(t, stdout.Close())

	cmd := helperCmd(t, 0)
	conn := New(stdin, stdout, WithCmd(cmd))

	require.ErrorIs(t, conn.Close(), os.ErrClosed)
	require.NotNil(t, cmd.ProcessState, "Close returned before waiting for the command")
}

func TestSpawnRejectsSetStdio(t *testing.T) {
	cmd := cat()
	cmd.Stdin = os.Stdin
	_, err := Spawn(cmd)
	require.ErrorContains(t, err, "already set")

	cmd = cat()
	cmd.Stdout = os.Stdout
	_, err = Spawn(cmd)
	require.ErrorContains(t, err, "already set")
}

func TestSpawnStartFailure(t *testing.T) {
	cmd := exec.Command(t.TempDir() + "/no-such-binary")
	_, err := Spawn(cmd)
	require.ErrorIs(t, err, os.ErrNotExist)
}
