package server

import (
	"context"
	"fmt"
	"net"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"google.golang.org/grpc"
)

type storagePluginServer struct {
	constructor integration.StoreFn
	instance    instance[integration.Store]

	proto.UnimplementedStoreServer
}

func (plugin *storagePluginServer) Init(ctx context.Context, req *proto.InitRequest) (*proto.InitResponse, error) {
	store, err := plugin.instance.init(func() (integration.Store, error) {
		return plugin.constructor(ctx, req.Proto, req.Config)
	})
	if err != nil {
		return nil, err
	}

	return &proto.InitResponse{
		Origin: store.Origin(),
		Type:   store.Type(),
		Root:   store.Root(),
		Flags:  uint32(store.Flags()),
	}, nil
}

func (plugin *storagePluginServer) Create(ctx context.Context, req *proto.CreateRequest) (*proto.CreateResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	if err := store.Create(ctx, req.Config); err != nil {
		return nil, err
	}
	return &proto.CreateResponse{}, nil
}

func (plugin *storagePluginServer) Open(ctx context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	b, err := store.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.OpenResponse{
		Config: b,
	}, nil
}

func (plugin *storagePluginServer) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	if err := store.Ping(ctx); err != nil {
		return nil, err
	}
	return &proto.PingResponse{}, nil
}

func (plugin *storagePluginServer) Mode(ctx context.Context, req *proto.ModeRequest) (*proto.ModeResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	mode, err := store.Mode(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.ModeResponse{Mode: int32(mode)}, nil
}

func (plugin *storagePluginServer) Size(ctx context.Context, req *proto.SizeRequest) (*proto.SizeResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	size, err := store.Size(ctx)
	if err != nil {
		return nil, err
	}

	return &proto.SizeResponse{Size: size}, nil
}

func (plugin *storagePluginServer) List(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	ctx = integration.WithFlag(ctx, integration.StorageFlag(req.Flags))
	macs, err := store.List(ctx, integration.StorageResource(req.Type))
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
	store, err := plugin.instance.get()
	if err != nil {
		return err
	}

	// read meta first
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	var (
		res = integration.StorageResource(req.Type)
		mac = integration.MAC(req.Mac)
		ctx = integration.WithFlag(stream.Context(), integration.StorageFlag(req.Flags))
	)

	size, err := store.Put(ctx, res, mac, client.ReceiveChunks(func() ([]byte, error) {
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
	store, err := plugin.instance.get()
	if err != nil {
		return err
	}

	var (
		res = integration.StorageResource(req.Type)
		mac = integration.MAC(req.Mac)
		ctx = integration.WithFlag(stream.Context(), integration.StorageFlag(req.Flags))

		rg *integration.Range
	)

	if r := req.Range; r != nil {
		rg = &integration.Range{
			Offset: r.Offset,
			Length: r.Length,
		}
	}

	r, err := store.Get(ctx, res, mac, rg)
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
	store, err := plugin.instance.get()
	if err != nil {
		return nil, err
	}

	ctx = integration.WithFlag(ctx, integration.StorageFlag(req.Flags))
	if err := store.Delete(ctx, integration.StorageResource(req.Type), integration.MAC(req.Mac)); err != nil {
		return nil, err
	}

	return &proto.DeleteResponse{}, nil
}

func (plugin *storagePluginServer) Close(ctx context.Context, req *proto.CloseRequest) (*proto.CloseResponse, error) {
	store, err := plugin.instance.take()
	if err != nil {
		return nil, err
	}

	if err := store.Close(ctx); err != nil {
		return nil, err
	}
	return &proto.CloseResponse{}, nil
}

// RunStorage starts the gRPC server for the storage plugin.
func RunStorage(constructor integration.StoreFn) error {
	listener, closer, err := initListener()
	if err != nil {
		return fmt.Errorf("failed to initialize connection: %w", err)
	}
	defer closer.Close()

	return RunStorageOn(constructor, listener)
}

func RunStorageOn(constructor integration.StoreFn, listener net.Listener) error {
	server := grpc.NewServer()
	proto.RegisterStoreServer(server, &storagePluginServer{
		constructor: constructor,
	})
	return server.Serve(listener)
}
