package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type exporterPluginServer struct {
	constructor    exporter.ExporterFn
	instance       instance[exporter.Exporter]
	maxConcurrency int
	flags          location.Flags

	// inflight matches the records on the Export stream with the
	// PutData streams carrying their payloads.
	inflight payloads

	proto.UnimplementedExporterServer
}

var (
	ErrExporterClosed = errors.New("the exporter is closed")
)

func (plugin *exporterPluginServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	if req.Options == nil {
		return nil, status.Error(codes.InvalidArgument, "missing options")
	}

	opts := connectors.Options{
		MaxConcurrency:  int(req.Options.Maxconcurrency),
		Hostname:        req.Options.Hostname,
		OperatingSystem: req.Options.Os,
		Architecture:    req.Options.Arch,
		CWD:             req.Options.Cwd,
		Excludes:        req.Options.Excludes,
		NoXattr:         req.Options.Noxattr,
	}

	exp, err := plugin.instance.init(func() (exporter.Exporter, error) {
		exp, err := plugin.constructor(ctx, &opts, req.Proto, req.Config)
		if err != nil {
			return nil, err
		}
		plugin.maxConcurrency = int(req.Options.Maxconcurrency)
		plugin.flags = exp.Flags()
		return exp, nil
	})
	if err != nil {
		return nil, err
	}

	return &proto.InitResponse{
		Origin:     exp.Origin(),
		Type:       exp.Type(),
		Root:       exp.Root(),
		Flags:      uint32(plugin.flags),
		HasPutData: true,
	}, nil
}

func (plugin *exporterPluginServer) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	exp, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	if err := exp.Ping(ctx); err != nil {
		return nil, err
	}

	return &proto.PingResponse{}, nil
}

type streamReader struct {
	end    chan struct{}
	eof    bool
	stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse]
	buf    bytes.Buffer

	// Record.Ok() and Record.Error() close the reader too, so the
	// exporter may well have closed it already.
	closeOnce sync.Once
	closeErr  error
}

func open(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse], end chan struct{}) io.ReadCloser {
	return &streamReader{
		stream: stream,
		end:    end,
	}
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	if s.buf.Len() != 0 {
		n, err = s.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}

	// don't attempt to read more on the stream, we got to
	// the end, or we'd consume the next record.
	if s.eof {
		return 0, io.EOF
	}

	fileResponse, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("failed to receive file data: %w", err)
	}

	if chunk := fileResponse.GetChunk(); chunk != nil {
		if len(chunk) == 0 {
			s.eof = true
		}

		s.buf.Write(chunk)
		n, err = s.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, fmt.Errorf("unexpected response: %v", fileResponse)
}

func (s *streamReader) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.close()
	})
	return s.closeErr
}

// We still drain this in order to avoid a misuse of the API (where someone
// would request less than what the server is sending), which leads to hangs.
func (s *streamReader) close() error {
	defer close(s.end)

	for !s.eof {
		res, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		} else if err != nil {
			return err
		}

		if chunk := res.GetChunk(); chunk != nil {
			if len(chunk) == 0 {
				break
			}
		} else {
			return fmt.Errorf("unexpected response: %v", res)
		}
	}
	return nil
}

func (plugin *exporterPluginServer) receiveRecords(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse], records chan<- *connectors.Record) error {
	defer close(records)

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		var (
			record *connectors.Record
			end    chan struct{}
		)

		switch p := res.Packet.(type) {
		case *proto.ExportRequest_Record:
			record, end, err = plugin.receiveRecord(stream, p.Record)
		case *proto.ExportRequest_Entry:
			record, err = plugin.receiveEntry(p.Entry)
		default:
			err = fmt.Errorf("bad type; expected a record, got %t", res.Packet)
		}
		if err != nil {
			return err
		}

		records <- record

		// the legacy protocol carries the payload on this very
		// stream, so we cannot read the next record before the
		// exporter is done with this one.
		if end != nil {
			<-end
		}
	}
}

// receiveRecord handles the legacy protocol, where the payload follows
// the record on the Export stream.
func (plugin *exporterPluginServer) receiveRecord(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse], r *conn.Record) (*connectors.Record, chan struct{}, error) {
	record, err := conn.RecordFromProto(r)
	if err != nil {
		return nil, nil, err
	}

	if !r.HasReader {
		return record, nil, nil
	}

	end := make(chan struct{})
	record.Reader = open(stream, end)

	return record, end, nil
}

// receiveEntry handles the entry protocol, where the payload is either
// inline or on a PutData stream of its own.
func (plugin *exporterPluginServer) receiveEntry(entry *proto.ExportEntry) (*connectors.Record, error) {
	if entry.Record == nil {
		return nil, fmt.Errorf("entry without a record")
	}

	record, err := conn.RecordFromProto(entry.Record)
	if err != nil {
		return nil, err
	}

	switch {
	case entry.Chunk != nil && entry.Id != 0:
		return nil, fmt.Errorf("%s: payload both inline and on a stream", record.Pathname)
	case entry.Chunk != nil:
		record.Reader = io.NopCloser(bytes.NewReader(entry.Chunk))
	case entry.Id != 0:
		rd, err := plugin.inflight.reader(entry.Id)
		if err != nil {
			return nil, err
		}
		record.Reader = rd
	case entry.Record.HasReader:
		return nil, fmt.Errorf("%s: payload announced but not sent", record.Pathname)
	}

	return record, nil
}

// PutData receives the payload of one record, which the Export stream
// named with the same id.  Either stream can get here first.
func (plugin *exporterPluginServer) PutData(stream grpc.ClientStreamingServer[proto.PutDataRequest, proto.PutDataResponse]) error {
	if _, err := plugin.instance.get(); err != nil {
		return err
	}

	var wr *io.PipeWriter

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if wr != nil {
				wr.CloseWithError(err)
			}
			return err
		}

		switch p := req.Packet.(type) {
		case *proto.PutDataRequest_Id:
			if wr != nil {
				return fmt.Errorf("payload %d: id sent twice", p.Id)
			}
			if wr, err = plugin.inflight.writer(p.Id); err != nil {
				return err
			}

		case *proto.PutDataRequest_Chunk:
			if wr == nil {
				return fmt.Errorf("payload data before its id")
			}
			if _, err := wr.Write(p.Chunk); err != nil {
				// the exporter closed the record early;
				// say so rather than let the client take
				// it for a broken stream.
				if errors.Is(err, io.ErrClosedPipe) {
					return stream.SendAndClose(&proto.PutDataResponse{Aborted: true})
				}
				return err
			}

		case *proto.PutDataRequest_Error:
			if wr == nil {
				return fmt.Errorf("payload error before its id")
			}
			// the client could not read this record; the
			// exporter decides what that means for it.
			wr.CloseWithError(errors.New(p.Error))

		default:
			return fmt.Errorf("bad type; expected a payload, got %t", req.Packet)
		}
	}

	if wr == nil {
		return fmt.Errorf("payload without an id")
	}
	wr.Close()

	return stream.SendAndClose(&proto.PutDataResponse{})
}

func (plugin *exporterPluginServer) transmitResults(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse], results <-chan *connectors.Result) error {
	for result := range results {
		hdr := proto.ExportResponse{
			Result: conn.ResultToProto(result),
		}
		if err := stream.Send(&hdr); err != nil {
			return err
		}
	}

	return nil
}

func (plugin *exporterPluginServer) Export(stream grpc.BidiStreamingServer[proto.ExportRequest, proto.ExportResponse]) error {
	exp, err := plugin.instance.get()
	if err != nil {
		return err
	}

	var (
		size    = plugin.maxConcurrency
		records = make(chan *connectors.Record, size)
		results = make(chan *connectors.Result, size)
		wg      = errgroup.Group{}
	)

	ctx, cancel := context.WithCancel(stream.Context())

	wg.Go(func() error {
		return plugin.receiveRecords(stream, records)
	})

	wg.Go(func() error {
		defer cancel()
		return plugin.transmitResults(stream, results)
	})

	if err := exp.Export(ctx, records, results); err != nil {
		return err
	}

	return wg.Wait()
}

func (plugin *exporterPluginServer) Close(ctx context.Context, req *proto.CloseRequest) (*proto.CloseResponse, error) {
	exp, err := plugin.instance.take()
	if err != nil {
		return nil, err
	}

	// we are done for good: release the payloads still waiting,
	// before the exporter gets a chance to wait on one of them.
	plugin.inflight.fail(ErrExporterClosed)

	if err := exp.Close(ctx); err != nil {
		return nil, err
	}

	return &proto.CloseResponse{}, nil
}

// RunExporter launches the gRPC server for an exporter plugin.
//
// The given constructor will be used to initialize the exporter instance.
func RunExporter(ctx context.Context, constructor exporter.ExporterFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunExporterOn(ctx, constructor, listener)
}

func RunExporterOn(ctx context.Context, constructor exporter.ExporterFn, listener net.Listener) error {
	server := grpc.NewServer()

	plugin := &exporterPluginServer{constructor: constructor}
	proto.RegisterExporterServer(server, plugin)

	return serve(ctx, server, listener, func() {
		// an exporter parked on a payload that is never going to
		// arrive now would never get to wind down: Stop() does not
		// wait for the handlers.
		plugin.inflight.fail(fmt.Errorf("the plugin is shutting down: %w", ctx.Err()))
	})
}
