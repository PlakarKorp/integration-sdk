package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
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

// A plugin cancelled before it gets to serve has to stop as cleanly as
// one cancelled while serving.
func TestRunOnCancelledContext(t *testing.T) {
	tests := []struct {
		name  string
		runOn func(context.Context, net.Listener) error
	}{
		{"importer", func(ctx context.Context, lis net.Listener) error {
			return RunImporterOn(ctx, nil, lis)
		}},
		{"exporter", func(ctx context.Context, lis net.Listener) error {
			return RunExporterOn(ctx, nil, lis)
		}},
		{"storage", func(ctx context.Context, lis net.Listener) error {
			return RunStorageOn(ctx, nil, lis)
		}},
		{"inventory", func(ctx context.Context, lis net.Listener) error {
			return RunInventoryOn(ctx, nil, lis)
		}},
		{"secret", func(ctx context.Context, lis net.Listener) error {
			return RunSecretProviderOn(ctx, nil, lis)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			served := make(chan error, 1)
			go func() { served <- tt.runOn(ctx, bufconn.Listen(1<<20)) }()

			select {
			case err := <-served:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.Fail(t, "did not return")
			}
		})
	}
}
