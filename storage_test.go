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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func time0() time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
}

func TestStorageResourceString(t *testing.T) {
	for res, want := range map[StorageResource]string{
		StorageResourceUndefined:   "undefined",
		StorageResourcePackfile:    "packfile",
		StorageResourceState:       "state",
		StorageResourceLock:        "lock",
		StorageResourceECCPackfile: "ECC packfile",
		StorageResourceECCState:    "ECC state",
		StorageResource(42):        "unknown",
	} {
		require.Equal(t, want, res.String())
	}
}
