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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMACJSON(t *testing.T) {
	mac := RandomMAC()
	require.NotEqual(t, NilMac, mac, "a random MAC is not the nil one")
	require.NotEqual(t, RandomMAC(), mac, "two random MACs differ")

	data, err := json.Marshal(mac)
	require.NoError(t, err)
	require.Equal(t, `"`+mac.FormatHex()+`"`, string(data))

	var got MAC
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, mac, got)

	require.Error(t, json.Unmarshal([]byte(`42`), &got), "a number is not a MAC")
}

func TestMACParseHex(t *testing.T) {
	var mac MAC

	require.Error(t, mac.ParseHex("zz"), "not hex")
	require.ErrorContains(t, mac.ParseHex("abcd"), "invalid mac length: 2")
	require.Equal(t, NilMac, mac, "a failed parse leaves the MAC alone")

	require.NoError(t, mac.ParseHex(strings.Repeat("ab", 32)))
	require.Equal(t, strings.Repeat("ab", 32), mac.FormatHex())
}
