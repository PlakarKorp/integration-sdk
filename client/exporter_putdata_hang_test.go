package client_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	"github.com/PlakarKorp/integration-sdk/location"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/PlakarKorp/integration-sdk/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// readingExporter is the plainest exporter there is: it reads every
// payload to the end, in order, and acks it.
type readingExporter struct{}

func (readingExporter) Origin() string              { return "o" }
func (readingExporter) Type() string                { return "t" }
func (readingExporter) Root() string                { return "/" }
func (readingExporter) Flags() location.Flags       { return 0 }
func (readingExporter) Ping(context.Context) error  { return nil }
func (readingExporter) Close(context.Context) error { return nil }

func (readingExporter) Export(ctx context.Context, _ map[string]string, records <-chan *integration.Record, results chan<- *integration.Result) error {
	defer close(results)
	for record := range records {
		if record.Reader != nil {
			if _, err := io.Copy(io.Discard, record.Reader); err != nil {
				results <- record.Error(err)
				continue
			}
		}
		results <- record.Ok()
	}
	return nil
}

// failFirstPutData makes the first PutData stream fail to open, as if
// that one stream had been reset, while the Export stream stays healthy.
func failFirstPutData() grpc.StreamClientInterceptor {
	var calls atomic.Int32
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if method == proto.Exporter_PutData_FullMethodName && calls.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "injected PutData failure")
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// bigRecord has a payload too large to travel inline, so that it goes
// on a PutData stream.
func bigRecord(name string) *integration.Record {
	const size = 128 * 1024
	return &integration.Record{
		Pathname: "/" + name,
		FileInfo: integration.FileInfo{Lname: name, Lsize: size, Lmode: 0o644},
		Reader:   io.NopCloser(bytes.NewReader(make([]byte, size))),
	}
}

// A PutData failure must fail the export, not leave it waiting forever
// for results: the payloads announced on the Export stream but never
// uploaded leave the exporter blocked on their pipe, so the Export
// stream has to be torn down for anything to give.
func TestExporter_PutDataFailureDoesNotHangExport(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	go server.RunExporterOn(func(context.Context, *integration.Options, string, map[string]string) (integration.Exporter, error) {
		return readingExporter{}, nil
	}, lis)

	cc, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(failFirstPutData()),
	)
	require.NoError(t, err)
	defer cc.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	exp, err := client.NewExporter(ctx, cc, &integration.Options{MaxConcurrency: 2}, "p", nil)
	require.NoError(t, err)
	// releases the payloads the plugin is still waiting on.
	defer exp.Close(context.Background())

	records := make(chan *integration.Record, 8)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		records <- bigRecord(name)
	}
	close(records)

	results := make(chan *integration.Result, 8)
	go func() {
		for range results {
		}
	}()

	done := make(chan error, 1)
	go func() { done <- exp.Export(ctx, nil, records, results) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "injected PutData failure")
	case <-time.After(5 * time.Second):
		cancel() // unwind the hung export before failing
		<-done
		t.Fatal("Export hung after a PutData failure")
	}
}
