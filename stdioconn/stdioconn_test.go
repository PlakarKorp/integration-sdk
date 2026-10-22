package stdioconn

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// pipes returns the conn's ends (stdin, stdout) and the peer's ends
// (to write into the conn, to read from the conn).
func pipes(t *testing.T) (stdin, stdout, peerW, peerR *os.File) {
	t.Helper()
	inR, inW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { inW.Close() })
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { outR.Close() })
	return inR, outW, inW, outR
}

func sh(script string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.Command("cmd", "/c", script)
	}
	return exec.Command("sh", "-c", script)
}

func helperCmd(t *testing.T, code int) *exec.Cmd {
	t.Helper()
	cmd := sh(fmt.Sprintf("exit %d", code))
	require.NoError(t, cmd.Start())
	return cmd
}

func TestReadWrite(t *testing.T) {
	stdin, stdout, peerW, peerR := pipes(t)
	conn := New(stdin, stdout)
	defer conn.Close()

	_, err := peerW.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))

	_, err = conn.Write([]byte("pong"))
	require.NoError(t, err)
	_, err = io.ReadFull(peerR, buf)
	require.NoError(t, err)
	require.Equal(t, "pong", string(buf), "peer read")
}

func TestClose(t *testing.T) {
	tests := []struct {
		name     string
		exitCode int // -1 for no command
		wantExit int // 0 for a nil error from Close
	}{
		{name: "no command", exitCode: -1},
		{name: "command succeeds", exitCode: 0},
		{name: "command fails", exitCode: 3, wantExit: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdin, stdout, _, _ := pipes(t)

			calls := 0
			opts := []Option{WithOnClose(func() { calls++ })}

			var cmd *exec.Cmd
			if tt.exitCode >= 0 {
				cmd = helperCmd(t, tt.exitCode)
				opts = append(opts, WithCmd(cmd))
			}

			conn := New(stdin, stdout, opts...)

			err := conn.Close()
			if tt.wantExit == 0 {
				require.NoError(t, err)
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr)
				require.Equal(t, tt.wantExit, exitErr.ExitCode())
			}
			if cmd != nil {
				require.NotNil(t, cmd.ProcessState, "Close returned before waiting for the command")
			}

			require.Equal(t, err, conn.Close(), "second Close")
			require.Equal(t, 1, calls, "onclose calls")

			_, err = conn.Read(make([]byte, 1))
			require.ErrorIs(t, err, os.ErrClosed, "Read after Close")
			_, err = conn.Write([]byte("x"))
			require.ErrorIs(t, err, os.ErrClosed, "Write after Close")
		})
	}
}

func cat() *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.Command("findstr", "^")
	}
	return exec.Command("cat")
}

func TestSpawn(t *testing.T) {
	conn, err := Spawn(cat())
	require.NoError(t, err)

	_, err = conn.Write([]byte("ping\n"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))

	// closing our end of the pipe is what lets the child exit.
	require.NoError(t, conn.Close())
}

func TestSpawnExitStatus(t *testing.T) {
	conn, err := Spawn(sh("exit 3"))
	require.NoError(t, err)

	var exitErr *exec.ExitError
	require.ErrorAs(t, conn.Close(), &exitErr)
	require.Equal(t, 3, exitErr.ExitCode())
}
