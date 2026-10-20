// Package stdioconn provides a net.Conn over a pair of files, such as
// the standard input and output of a process or the pipes to a child.
package stdioconn

import (
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
)

var stdioaddr = &net.UnixAddr{Name: "stdio", Net: "unix"}

// Conn is a net.Conn that wraps a pipe and optionally an [exec.Cmd].
type Conn struct {
	stdin  *os.File
	stdout *os.File

	// thanks to grpc, we might end up in Close multiple times
	closeOnce sync.Once
	closeErr  error

	onclose func()
	cmd     *exec.Cmd
}

// Option configures a Conn.
type Option func(*Conn)

// WithCmd makes Close wait for cmd to exit and return its error.
func WithCmd(cmd *exec.Cmd) Option {
	return func(c *Conn) { c.cmd = cmd }
}

// WithOnClose makes Close call fn, once, before closing the files.
func WithOnClose(fn func()) Option {
	return func(c *Conn) { c.onclose = fn }
}

// New returns a Conn reading from stdin and writing to stdout.
func New(stdin, stdout *os.File, opts ...Option) *Conn {
	c := &Conn{
		stdin:  stdin,
		stdout: stdout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Read reads from stdin.
func (c *Conn) Read(b []byte) (int, error) { return c.stdin.Read(b) }

// Write writes to stdout.
func (c *Conn) Write(b []byte) (int, error) { return c.stdout.Write(b) }

// LocalAddr returns a placeholder address.
func (c *Conn) LocalAddr() net.Addr { return stdioaddr }

// RemoteAddr returns a placeholder address.
func (c *Conn) RemoteAddr() net.Addr { return stdioaddr }

func (c *Conn) close() (ret error) {
	if c.onclose != nil {
		c.onclose()
	}

	if err := c.stdin.Close(); err != nil {
		ret = err
	}
	if err := c.stdout.Close(); err != nil {
		ret = err
	}
	if c.cmd != nil {
		if err := c.cmd.Wait(); err != nil {
			ret = err
		}
	}
	return
}

// Close closes both files and waits for the command, if any.  It is
// safe to call more than once; later calls return the first result.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.close()
	})
	return c.closeErr
}

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline on stdin.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.stdin.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline on stdout.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.stdout.SetWriteDeadline(t) }
