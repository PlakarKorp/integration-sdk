package client

import (
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
)

var stdioaddr = &net.UnixAddr{Name: "stdio", Net: "unix"}

type StdioConn struct {
	stdin  *os.File
	stdout *os.File
	cmd    *exec.Cmd

	// thanks to grpc, we might end up in Close multiple times
	closeOnce sync.Once
	closeErr  error
}

func NewStdioConn(stdin, stdout *os.File, cmd *exec.Cmd) net.Conn {
	c := &StdioConn{
		stdin:  stdin,
		stdout: stdout,
		cmd:    cmd,
	}
	return c
}

func (c *StdioConn) Read(b []byte) (int, error)  { return c.stdin.Read(b) }
func (c *StdioConn) Write(b []byte) (int, error) { return c.stdout.Write(b) }
func (c *StdioConn) LocalAddr() net.Addr         { return stdioaddr }
func (c *StdioConn) RemoteAddr() net.Addr        { return stdioaddr }

func (c *StdioConn) close() (ret error) {
	if err := c.stdin.Close(); err != nil {
		ret = err
	}
	if err := c.stdout.Close(); err != nil {
		ret = err
	}
	if err := c.cmd.Wait(); err != nil {
		ret = err
	}
	return
}

func (c *StdioConn) Close() (ret error) {
	c.closeOnce.Do(func() {
		c.closeErr = c.close()
	})
	return c.closeErr
}

func (c *StdioConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *StdioConn) SetReadDeadline(t time.Time) error  { return c.stdin.SetReadDeadline(t) }
func (c *StdioConn) SetWriteDeadline(t time.Time) error { return c.stdout.SetWriteDeadline(t) }
