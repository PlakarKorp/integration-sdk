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

package example

import (
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/stretchr/testify/require"
)

func TestInventory(t *testing.T) {
	ctx := t.Context()

	inv, err := NewInventory(ctx, map[string]string{
		"example_param1": "one",
		"example_param2": "two",
		"ignored":        "x",
	})
	require.NoError(t, err)

	c := make(chan *integration.InventoryEntry)
	var entries []*integration.InventoryEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for e := range c {
			entries = append(entries, e)
		}
	}()
	require.NoError(t, inv.List(ctx, c))
	<-done

	require.Len(t, entries, 3)
	for i, e := range entries {
		require.Equal(t, integration.ResourceClassCompute, e.Class, "entry %d", i)
		require.Equal(t, integration.Country("FR"), e.Country, "entry %d", i)
		require.Len(t, e.Endpoints, 1, "entry %d", i)
	}

	require.NoError(t, inv.Close(ctx))
}
