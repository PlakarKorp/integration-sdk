package stdioconn

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

// pipes returns the conn's ends (stdin, stdout) and the peer's ends
// (to write into the conn, to read from the conn).
func pipes(t *testing.T) (stdin, stdout, peerW, peerR *os.File) {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { inW.Close() })
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
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
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func TestReadWrite(t *testing.T) {
	stdin, stdout, peerW, peerR := pipes(t)
	conn := New(stdin, stdout)
	defer conn.Close()

	if _, err := peerW.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Errorf("read %q, want %q", buf, "ping")
	}

	if _, err := conn.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(peerR, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "pong" {
		t.Errorf("peer read %q, want %q", buf, "pong")
	}
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
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			} else {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("Close: got %v, want *exec.ExitError", err)
				}
				if got := exitErr.ExitCode(); got != tt.wantExit {
					t.Fatalf("exit code %d, want %d", got, tt.wantExit)
				}
			}
			if cmd != nil && cmd.ProcessState == nil {
				t.Error("Close returned before waiting for the command")
			}

			if again := conn.Close(); again != err {
				t.Errorf("second Close: got %v, want %v", again, err)
			}
			if calls != 1 {
				t.Errorf("onclose called %d times, want 1", calls)
			}

			if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Errorf("Read after Close: got %v, want %v", err, os.ErrClosed)
			}
			if _, err := conn.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
				t.Errorf("Write after Close: got %v, want %v", err, os.ErrClosed)
			}
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
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Errorf("read %q, want %q", buf, "ping")
	}

	// closing our end of the pipe is what lets the child exit.
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestSpawnExitStatus(t *testing.T) {
	conn, err := Spawn(sh("exit 3"))
	if err != nil {
		t.Fatal(err)
	}

	var exitErr *exec.ExitError
	if err := conn.Close(); !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Errorf("Close: got %v, want exit status 3", err)
	}
}
