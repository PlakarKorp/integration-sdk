package server

import (
	"context"
	"fmt"
	"net"

	"github.com/PlakarKorp/kloset/objects"

	"github.com/PlakarKorp/integration-sdk/client"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/PlakarKorp/kloset/connectors/storage"

	"google.golang.org/grpc"
)

type storagePluginServer struct {
	constructor storage.StoreFn
	storage     storage.Store

	proto.UnimplementedStoreServer
}

func (plugin *storagePluginServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	storage, err := plugin.constructor(ctx, req.Proto, req.Config)
	if err != nil {
		return nil, err
	}

	plugin.storage = storage
	return &proto.InitResponse{
		Origin: storage.Origin(),
		Type:   storage.Type(),
		Root:   storage.Root(),
		Flags:  uint32(storage.Flags()),
	}, nil
}

func (plugin *storagePluginServer) Create(ctx context.Context, req *proto.CreateRequest) (*proto.CreateResponse, error) {
	err := plugin.storage.Create(ctx, req.Config)
	if err != nil {
		return nil, err
	}
	return &proto.CreateResponse{}, nil
}

func (plugin *storagePluginServer) Open(ctx context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	b, err := plugin.storage.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.OpenResponse{
		Config: b,
	}, nil
}

func (plugin *storagePluginServer) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	if err := plugin.storage.Ping(ctx); err != nil {
		return nil, err
	}
	return &proto.PingResponse{}, nil
}

func (plugin *storagePluginServer) Mode(ctx context.Context, req *proto.ModeRequest) (*proto.ModeResponse, error) {
	mode, err := plugin.storage.Mode(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.ModeResponse{Mode: int32(mode)}, nil
}

func (plugin *storagePluginServer) Size(ctx context.Context, req *proto.SizeRequest) (*proto.SizeResponse, error) {
	size, err := plugin.storage.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &proto.SizeResponse{Size: size}, nil
}

func (plugin *storagePluginServer) List(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	ctx = storage.WithFlag(ctx, storage.StorageFlag(req.Flags))
	macs, err := plugin.storage.List(ctx, storage.StorageResource(req.Type))
	if err != nil {
		return nil, err
	}

	gmacs := make([][]byte, 0, len(macs))
	for i := range macs {
		gmacs = append(gmacs, macs[i][:])
	}

	return &proto.ListResponse{Macs: gmacs}, nil
}

func (plugin *storagePluginServer) Put(stream grpc.ClientStreamingServer[proto.PutRequest, proto.PutResponse]) error {
	// read meta first
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var (
		res = storage.StorageResource(req.Type)
		mac = objects.MAC(req.Mac)
		ctx = storage.WithFlag(stream.Context(), storage.StorageFlag(req.Flags))
	)

	size, err := plugin.storage.Put(ctx, res, mac, client.ReceiveChunks(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return req.Chunk, nil
	}))
	if err != nil {
		return err
	}

	return stream.SendAndClose(&proto.PutResponse{BytesWritten: size})
}

func (plugin *storagePluginServer) Get(req *proto.GetRequest, stream grpc.ServerStreamingServer[proto.GetResponse]) error {
	var (
		res = storage.StorageResource(req.Type)
		mac = objects.MAC(req.Mac)
		ctx = storage.WithFlag(stream.Context(), storage.StorageFlag(req.Flags))

		rg *storage.Range
	)

	if r := req.Range; r != nil {
		rg = &storage.Range{
			Offset: r.Offset,
			Length: r.Length,
		}
	}

	r, err := plugin.storage.Get(ctx, res, mac, rg)
	if err != nil {
		return err
	}

	_, err = client.SendChunks(r, func(chunk []byte) error {
		return stream.Send(&proto.GetResponse{
			Chunk: chunk,
		})
	})
	return err

}

func (plugin *storagePluginServer) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	ctx = storage.WithFlag(ctx, storage.StorageFlag(req.Flags))
	if err := plugin.storage.Delete(ctx, storage.StorageResource(req.Type), objects.MAC(req.Mac)); err != nil {
		return nil, err
	}

	return &proto.DeleteResponse{}, nil
}

func (plugin *storagePluginServer) Close(ctx context.Context, req *proto.CloseRequest) (*proto.CloseResponse, error) {
	err := plugin.storage.Close(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.CloseResponse{}, nil
}

// RunStorage starts the gRPC server for the storage plugin.
func RunStorage(ctx context.Context, constructor storage.StoreFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunStorageOn(ctx, constructor, listener)
}

func RunStorageOn(ctx context.Context, constructor storage.StoreFn, listener net.Listener) error {
	server := grpc.NewServer()

	proto.RegisterStoreServer(server, &storagePluginServer{
		constructor: constructor,
	})

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
		case <-done:
		}
	}()

	if err := server.Serve(listener); err != nil {
		return err
	}
	return nil
}
