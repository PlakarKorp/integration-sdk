package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"

	conn "github.com/PlakarKorp/integration-sdk/proto"
	proto "github.com/PlakarKorp/integration-sdk/proto/importer"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type importerPluginServer struct {
	constructor    importer.ImporterFn
	instance       instance[importer.Importer]
	flags          location.Flags
	maxconcurrency int
	readers        *conn.HoldingReaders

	proto.UnimplementedImporterServer
}

func (plugin *importerPluginServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	if req.Options == nil {
		return nil, status.Error(codes.InvalidArgument, "missing options")
	}

	opts := connectors.Options{
		Hostname:        req.Options.Hostname,
		OperatingSystem: req.Options.Os,
		Architecture:    req.Options.Arch,
		CWD:             req.Options.Cwd,
		MaxConcurrency:  max(1, int(req.Options.Maxconcurrency)),
		Excludes:        req.Options.Excludes,
		NoXattr:         req.Options.Noxattr,
	}

	imp, err := plugin.instance.init(func() (importer.Importer, error) {
		imp, err := plugin.constructor(ctx, &opts, req.Proto, req.Config)
		if err != nil {
			return nil, err
		}
		plugin.flags = imp.Flags()
		plugin.maxconcurrency = opts.MaxConcurrency
		plugin.readers = conn.NewHoldingReaders()
		return imp, nil
	})
	if err != nil {
		return nil, err
	}

	return &proto.InitResponse{
		Origin: imp.Origin(),
		Type:   imp.Type(),
		Root:   imp.Root(),
		Flags:  uint32(plugin.flags),
	}, nil
}

func (plugin *importerPluginServer) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	imp, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}
	return nil, imp.Ping(ctx)
}

func (plugin *importerPluginServer) sendRecords(stream grpc.BidiStreamingServer[proto.ImportRequest, proto.ImportResponse], records <-chan *connectors.Record) error {
	for record := range records {
		hdr := proto.ImportResponse{
			Record: conn.RecordToProto(record),
		}

		if record.Reader != nil {
			plugin.readers.Track(record)
		}

		if err := stream.Send(&hdr); err != nil {
			return err
		}
	}

	return stream.Send(&proto.ImportResponse{Finished: true})
}

func (plugin *importerPluginServer) receiveResults(stream grpc.BidiStreamingServer[proto.ImportRequest, proto.ImportResponse], results chan<- *connectors.Result) error {
	if results != nil {
		defer close(results)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		result, err := conn.ResultFromProto(res.Result)
		if err != nil {
			return err
		}

		if results != nil {
			results <- result
		}

		if rd := plugin.readers.Get(&result.Record); rd != nil {
			rd.Close()
		}
	}
}

func (plugin *importerPluginServer) Import(stream grpc.BidiStreamingServer[proto.ImportRequest, proto.ImportResponse]) error {
	imp, err := plugin.instance.get()
	if err != nil {
		return err
	}

	var (
		size    = plugin.maxconcurrency
		records = make(chan *connectors.Record, size)

		wg errgroup.Group
	)

	var results chan *connectors.Result
	if (plugin.flags & location.FLAG_NEEDACK) != 0 {
		results = make(chan *connectors.Result, size)
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	wg.Go(func() error {
		return plugin.sendRecords(stream, records)
	})

	wg.Go(func() error {
		return plugin.receiveResults(stream, results)
	})

	if err := imp.Import(ctx, records, results); err != nil {
		return err
	}

	return wg.Wait()
}

func (plugin *importerPluginServer) Open(req *proto.OpenRequest, stream grpc.ServerStreamingServer[proto.OpenResponse]) error {
	if req.Record == nil {
		return status.Error(codes.InvalidArgument, "missing record")
	}

	if _, err := plugin.instance.get(); err != nil {
		return err
	}

	rd := plugin.readers.Get(&connectors.Record{
		Pathname:  req.Record.Pathname,
		IsXattr:   req.Record.IsXattr,
		XattrName: req.Record.XattrName,
		XattrType: objects.Attribute(req.Record.XattrType),
		Target:    req.Record.Target,
	})
	if rd == nil {
		return fs.ErrNotExist
	}
	defer rd.Close()

	var buf = make([]byte, 1024*1024)
	for {
		n, err := rd.Read(buf)
		if n > 0 {
			err := stream.Send(&proto.OpenResponse{
				Chunk: buf[:n],
			})
			if err != nil {
				return err
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (plugin *importerPluginServer) Close(ctx context.Context, req *proto.CloseRequest) (*proto.CloseResponse, error) {
	imp, err := plugin.instance.take()
	if err != nil {
		return nil, err
	}

	plugin.readers.Close()

	if err := imp.Close(ctx); err != nil {
		return nil, err
	}
	return &proto.CloseResponse{}, nil
}

// RunImporter starts the gRPC server for the importer plugin.
func RunImporter(constructor importer.ImporterFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunImporterOn(constructor, listener)
}

func RunImporterOn(constructor importer.ImporterFn, listener net.Listener) error {
	server := grpc.NewServer()
	proto.RegisterImporterServer(server, &importerPluginServer{
		constructor: constructor,
	})
	return server.Serve(listener)
}
