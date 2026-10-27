package server

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitListenerStdioDefault(t *testing.T) {
	t.Setenv(ListenEnv, "")
	os.Unsetenv(ListenEnv)

	// The closer is deliberately not closed: in stdio mode it wraps the
	// process's real stdin/stdout, and closing those would silence the
	// remaining tests.
	listener, _, err := initListener()
	require.NoError(t, err)

	require.IsType(t, &singleConnListener{}, listener)
}

func TestInitListenerUnixSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "plugin.sock")
	t.Setenv(ListenEnv, "unix://"+sock)

	listener, closer, err := initListener()
	require.NoError(t, err)
	defer closer.Close()

	fi, err := os.Stat(sock)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0666), fi.Mode().Perm(), "socket mode")

	// Prove the listener actually accepts connections.
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			conn.Close()
		}
		done <- err
	}()
	conn, err := net.Dial("unix", sock)
	require.NoError(t, err)
	conn.Close()
	require.NoError(t, <-done, "accept")
}

func TestInitListenerRejectsNonUnix(t *testing.T) {
	t.Setenv(ListenEnv, "tcp://0.0.0.0:9876")

	_, _, err := initListener()
	require.Error(t, err, "a non-unix address has to be rejected")
}
