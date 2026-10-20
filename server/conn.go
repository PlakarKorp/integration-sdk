package server

import (
	"io"
	"net"
)

// singleConnListener is a net.Listener that accepts only one connection.
type singleConnListener struct {
	notify <-chan struct{}
	conn   net.Conn
	used   bool
}

// Accept returns the connection once, then blocks forever.
func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.used {
		<-l.notify
		return nil, io.EOF
	}
	l.used = true
	return l.conn, nil
}

// Close closes the underlying connection.
func (l *singleConnListener) Close() error {
	return l.conn.Close()
}

// Addr returns the local address of the listener.
func (l *singleConnListener) Addr() net.Addr {
	return l.conn.LocalAddr()
}
