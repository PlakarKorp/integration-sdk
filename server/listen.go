package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/PlakarKorp/integration-sdk/stdioconn"
	"google.golang.org/grpc"
)

// When running over a container plakar sets this to the path of the socket we
// should be listening on. Empty when running on a default runner.
const ListenEnv = "PLAKAR_PLUGIN_LISTEN"

func initListener() (net.Listener, io.Closer, error) {
	addr := os.Getenv(ListenEnv)
	if addr == "" {
		ch := make(chan struct{})
		conn := stdioconn.New(os.Stdin, os.Stdout, stdioconn.WithOnClose(func() { close(ch) }))
		return &singleConnListener{notify: ch, conn: conn}, conn, nil
	}

	path, ok := strings.CutPrefix(addr, "unix://")
	if !ok || len(path) == 0 {
		// TODO: If we want to support more than linux we need to swap this to a
		// tcp socket, but it needs a bit more coordination from plakar.
		return nil, nil, fmt.Errorf("unsupported %s address %q: only unix:// is supported", ListenEnv, addr)
	}

	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, err
	}

	// The socket is created with the container-side uid; open it up and let
	// the 0700 host-side parent dir gate access instead.
	if err := os.Chmod(path, 0666); err != nil {
		listener.Close()
		return nil, nil, err
	}
	return listener, listener, nil
}

// serve runs srv on lis until ctx is cancelled.  Then it runs the
// hooks, in order, and stops srv.
func serve(ctx context.Context, srv *grpc.Server, lis net.Listener, hooks ...func()) error {
	unregister := context.AfterFunc(ctx, func() {
		for _, hook := range hooks {
			hook()
		}
		srv.Stop()
	})
	defer unregister()

	err := srv.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) && ctx.Err() != nil {
		// cancelled before Serve got to run
		return nil
	}
	return err
}
