package server

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotInitialized     = status.Error(codes.FailedPrecondition, "not initialized")
	ErrAlreadyInitialized = status.Error(codes.FailedPrecondition, "already initialized")
)

type instance[T any] struct {
	once sync.Once
	conn atomic.Pointer[T]
}

func (in *instance[T]) init(fn func() (T, error)) (T, error) {
	var (
		conn T
		err  = ErrAlreadyInitialized
	)
	in.once.Do(func() {
		if conn, err = fn(); err == nil {
			in.conn.Store(&conn)
		}
	})
	return conn, err
}

func (in *instance[T]) get() (zero T, err error) {
	if p := in.conn.Load(); p != nil {
		return *p, nil
	}
	return zero, ErrNotInitialized
}

func (in *instance[T]) take() (zero T, err error) {
	if p := in.conn.Swap(nil); p != nil {
		return *p, nil
	}
	return zero, ErrNotInitialized
}
