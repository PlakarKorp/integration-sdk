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
package integration

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type closeCounter struct {
	io.Reader
	closed int
}

func (c *closeCounter) Close() error {
	c.closed++
	return nil
}

func TestRecordClose(t *testing.T) {
	rec := &Record{Pathname: "/a"}
	require.ErrorIs(t, rec.Close(), errors.ErrUnsupported, "nothing to close")

	rd := &closeCounter{Reader: strings.NewReader("x")}
	rec.Reader = rd
	require.NoError(t, rec.Close())
	require.Equal(t, 1, rd.closed)
}

func TestRecordResults(t *testing.T) {
	rd := &closeCounter{Reader: strings.NewReader("x")}
	rec := &Record{Pathname: "/a", Reader: rd}

	res := rec.Ok()
	require.NoError(t, res.Err)
	require.Equal(t, "/a", res.Record.Pathname)
	require.Equal(t, 1, rd.closed, "Ok closes the reader")

	boom := errors.New("boom")
	res = rec.Error(boom)
	require.ErrorIs(t, res.Err, boom)
	require.Equal(t, "/a", res.Record.Pathname)
	require.Equal(t, 2, rd.closed, "Error closes the reader")
}

func TestNewRecords(t *testing.T) {
	open := func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("payload")), nil
	}

	fi := NewFileInfo("f", 7, 0o644, time0(), 1, 2, 3, 4, 5)
	rec := NewRecord("/f", "", fi, []string{"user.x"}, open)
	require.Equal(t, "/f", rec.Pathname)
	require.Equal(t, fi, rec.FileInfo)
	require.Equal(t, []string{"user.x"}, rec.ExtendedAttributes)
	require.False(t, rec.IsXattr)
	data, err := io.ReadAll(rec.Reader)
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))

	xattr := NewXattr("/f", "user.x", AttributeExtended, open)
	require.Equal(t, "/f", xattr.Pathname)
	require.True(t, xattr.IsXattr)
	require.Equal(t, "user.x", xattr.XattrName)
	require.Equal(t, AttributeExtended, xattr.XattrType)
	require.NotNil(t, xattr.Reader)

	boom := errors.New("boom")
	failed := NewError("/missing", boom)
	require.Equal(t, "/missing", failed.Pathname)
	require.ErrorIs(t, failed.Err, boom)
	require.Nil(t, failed.Reader)
}
