package server

import (
	"context"
	"fmt"
	"net"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/client"
	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	var (
		res  = integration.StorageResource(req.Type)
		flag = integration.StorageFlag(req.Flags)
		ret  [][]byte
	)

	for res, err := range store.List(ctx, res, flag) {
		if err != nil {
			return nil, err
		}

		ret = append(ret, res.MAC[:])
	}

	return &proto.ListResponse{Macs: ret}, nil
}

func (plugin *storagePluginServer) ListStream(req *proto.ListRequest, stream grpc.ServerStreamingServer[proto.ListStreamResponse]) error {
	store, err := plugin.instance.get()
	if err != nil {
		return err
	}

	var (
		res  = integration.StorageResource(req.Type)
		flag = integration.StorageFlag(req.Flags)
	)

	const batch = 1024
	buf := make([]*proto.Resource, 0, batch)

	for res, err := range store.List(stream.Context(), res, flag) {
		if err != nil {
			return err
		}

		buf = append(buf, &proto.Resource{
			Mac:   res.MAC[:],
			Size:  res.Size,
			Mtime: timestamppb.New(res.MTime),
		})
		if len(buf) < batch {
			continue
		}

		// flush
		if err := stream.Send(&proto.ListStreamResponse{Resources: buf}); err != nil {
			return err
		}
		buf = make([]*proto.Resource, 0, batch)
	}

	if len(buf) != 0 {
		return stream.Send(&proto.ListStreamResponse{Resources: buf})
	}
	return nil
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
		res  = integration.StorageResource(req.Type)
		mac  = integration.MAC(req.Mac)
		flag = integration.StorageFlag(req.Flags)
	)

	size, err := store.Put(stream.Context(), res, flag, mac, client.ReceiveChunks(func() ([]byte, error) {
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
		res  = integration.StorageResource(req.Type)
		flag = integration.StorageFlag(req.Flags)
		mac  = integration.MAC(req.Mac)

		rg *integration.Range
	)

	if r := req.Range; r != nil {
		rg = &integration.Range{
			Offset: r.Offset,
			Length: r.Length,
		}
	}

	r, err := store.Get(stream.Context(), res, flag, mac, rg)
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

	var (
		res  = integration.StorageResource(req.Type)
		flag = integration.StorageFlag(req.Flags)
		mac  = integration.MAC(req.Mac)
	)

	if err := store.Delete(ctx, res, flag, mac); err != nil {
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
