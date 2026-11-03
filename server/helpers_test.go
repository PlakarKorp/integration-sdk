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
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeStream stands in for any kind of server stream: Recv, Send and
// SendAndClose are whatever the test makes them.
type fakeStream[Req, Res any] struct {
	grpc.ServerStream
	ctx          context.Context
	recv         func() (*Req, error)
	send         func(*Res) error
	sendAndClose func(*Res) error
}

func (s *fakeStream[Req, Res]) Context() context.Context { return s.ctx }

func (s *fakeStream[Req, Res]) Recv() (*Req, error) { return s.recv() }

func (s *fakeStream[Req, Res]) Send(res *Res) error {
	if s.send == nil {
		return nil
	}
	return s.send(res)
}

func (s *fakeStream[Req, Res]) SendAndClose(res *Res) error {
	if s.sendAndClose == nil {
		return nil
	}
	return s.sendAndClose(res)
}

// scripted hands out reqs one at a time, then ends with final.
func scripted[Req any](final error, reqs ...*Req) func() (*Req, error) {
	return func() (*Req, error) {
		if len(reqs) == 0 {
			return nil, final
		}
		req := reqs[0]
		reqs = reqs[1:]
		return req, nil
	}
}

// serveOn runs serve, one of the RunXOn functions, on a bufconn listener
// and returns a connection to it.  Closing the listener is what ends
// serve, and the test does so when it is over.
func serveOn(t *testing.T, serve func(net.Listener) error) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	served := make(chan error, 1)
	go func() { served <- serve(lis) }()
	t.Cleanup(func() {
		lis.Close()
		select {
		case err := <-served:
			require.Error(t, err, "Serve returned before its listener was closed")
		case <-time.After(5 * time.Second):
			require.Fail(t, "Serve is still running after its listener was closed")
		}
	})

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}
