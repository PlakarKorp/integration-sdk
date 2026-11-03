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

package client

import (
	"context"

	"google.golang.org/grpc"
)

// failingConn is a connection on which every RPC fails with err.
type failingConn struct{ err error }

func (c failingConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return c.err
}

func (c failingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, c.err
}

// fakeClientStream stands in for any kind of client stream: Send, Recv,
// CloseSend and CloseAndRecv are whatever the test makes them.
type fakeClientStream[Req, Res any] struct {
	grpc.ClientStream
	send         func(*Req) error
	recv         func() (*Res, error)
	closeSend    func() error
	closeAndRecv func() (*Res, error)
}

func (s *fakeClientStream[Req, Res]) Send(req *Req) error {
	if s.send == nil {
		return nil
	}
	return s.send(req)
}

func (s *fakeClientStream[Req, Res]) Recv() (*Res, error) { return s.recv() }

func (s *fakeClientStream[Req, Res]) CloseSend() error {
	if s.closeSend == nil {
		return nil
	}
	return s.closeSend()
}

func (s *fakeClientStream[Req, Res]) CloseAndRecv() (*Res, error) { return s.closeAndRecv() }

// sendScript fails the nth Send with errs[n], and succeeds past them.
func sendScript[Req any](errs ...error) func(*Req) error {
	return func(*Req) error {
		if len(errs) == 0 {
			return nil
		}
		err := errs[0]
		errs = errs[1:]
		return err
	}
}

// recvScript hands out ress one at a time, then ends with final.
func recvScript[Res any](final error, ress ...*Res) func() (*Res, error) {
	return func() (*Res, error) {
		if len(ress) == 0 {
			return nil, final
		}
		res := ress[0]
		ress = ress[1:]
		return res, nil
	}
}

// recordsOf is a closed channel holding recs.
func recordsOf[T any](items ...*T) <-chan *T {
	ch := make(chan *T, len(items))
	for _, item := range items {
		ch <- item
	}
	close(ch)
	return ch
}
