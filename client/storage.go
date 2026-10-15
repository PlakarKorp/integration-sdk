/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	proto "github.com/PlakarKorp/integration-sdk/proto/storage"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"google.golang.org/grpc"
)

type Store struct {
	client proto.StoreClient
	root   string
	origin string
	typ    string
	mode   storage.Mode
	flags  location.Flags
}

func NewStorage(ctx context.Context, client grpc.ClientConnInterface, p string, config map[string]string) (storage.Store, error) {
	s := &Store{
		client: proto.NewStoreClient(client),
	}

	res, err := s.client.Init(ctx, &proto.InitRequest{
		Proto:  p,
		Config: config,
	})
	if err != nil {
		return nil, unwrap(err)
	}

	s.origin = res.Origin
	s.root = res.Root
	s.typ = res.Type
	s.flags = location.Flags(res.Flags)

	return s, nil
}

func (s *Store) Create(ctx context.Context, config []byte) error {
	_, err := s.client.Create(ctx, &proto.CreateRequest{
		Config: config,
	})
	if err != nil {
		return unwrap(err)
	}
	return nil
}

func (s *Store) Open(ctx context.Context) ([]byte, error) {
	resp, err := s.client.Open(ctx, &proto.OpenRequest{})
	if err != nil {
		return nil, unwrap(err)
	}
	return resp.Config, nil
}

func (s *Store) Ping(ctx context.Context) error {
	_, err := s.client.Ping(ctx, &proto.PingRequest{})

	return unwrap(err)
}

func (s *Store) Origin() string        { return s.origin }
func (s *Store) Type() string          { return s.typ }
func (s *Store) Root() string          { return s.root }
func (s *Store) Flags() location.Flags { return s.flags }

func (s *Store) Mode(ctx context.Context) (storage.Mode, error) {
	if resp, err := s.client.Mode(ctx, &proto.ModeRequest{}); err != nil {
		return 0, unwrap(err)
	} else {
		return storage.Mode(resp.Mode), nil
	}
}

func (s *Store) Size(ctx context.Context) (int64, error) {
	resp, err := s.client.Size(ctx, &proto.SizeRequest{})
	if err != nil {
		return -1, unwrap(err)
	}
	return resp.Size, nil
}

func SendChunks(rd io.ReadCloser, chunkSendFn func(chunk []byte) error) (int64, error) {
	// 1MB buffer, the underlying grpc transport limits us to 4MB.
	buffer := make([]byte, 1024*1024)
	var totalBytes int64

	defer rd.Close()
	for {
		n, err := rd.Read(buffer)
		if n > 0 {
			if err := chunkSendFn(buffer[:n]); err != nil {
				return totalBytes, fmt.Errorf("failed to send chunk: %w", unwrap(err))
			}
			totalBytes += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, fmt.Errorf("failed to read: %w", unwrap(err))
		}
	}
	return totalBytes, nil
}

type grpcChunkReader struct {
	streamRecv func() ([]byte, error)
	buf        bytes.Buffer
}

func (g *grpcChunkReader) Read(p []byte) (int, error) {
	totalRead := 0
	for totalRead < len(p) {
		//if there is data in the internal buffer -> read from it first
		if g.buf.Len() > 0 {
			n, err := g.buf.Read(p[totalRead:])
			totalRead += n
			if err != nil {
				return totalRead, unwrap(err)
			}
			//if the buffer is full -> done
			if totalRead == len(p) {
				return totalRead, nil
			}
		}

		//receive the next chunk of data
		chunk, err := g.streamRecv()
		if err != nil {
			if err == io.EOF {
				if totalRead > 0 {
					return totalRead, nil //return what we have before signaling EOF
				}
				return 0, io.EOF
			}
			return totalRead, fmt.Errorf("failed to receive file data: %w", unwrap(err))
		}

		//add chunk to the internal buffer
		g.buf.Write(chunk)
	}

	return totalRead, nil
}

// Closing of the actual reader is left to the caller, close is not to be
// forwarded over grpc.
// We still drain this in order to avoid a misuse of the API (where someone
// would request less than what the server is sending), which leads to leaks.
func (g *grpcChunkReader) Close() error {
	for {
		_, err := g.streamRecv()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
	}
}

func ReceiveChunks(chunkReceiverFn func() ([]byte, error)) io.ReadCloser {
	streamReader := &grpcChunkReader{
		streamRecv: chunkReceiverFn,
	}
	return streamReader
}

func (s *Store) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	resp, err := s.client.List(ctx, &proto.ListRequest{
		Type:  proto.StorageResource(res),
		Flags: proto.StorageFlags(storage.Flag(ctx)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", res, unwrap(err))
	}

	var macs []objects.MAC
	for _, mac := range resp.Macs {
		if len(mac) != len(objects.MAC{}) {
			return nil, fmt.Errorf("invalid MAC length: %d", len(mac))
		}
		macs = append(macs, objects.MAC(mac))
	}
	return macs, nil
}

func (s *Store) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	stream, err := s.client.Put(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to start Put stream: %w", unwrap(err))
	}

	err = stream.Send(&proto.PutRequest{
		Mac:   mac[:],
		Type:  proto.StorageResource(res),
		Flags: proto.StorageFlags(storage.Flag(ctx)),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to send mac: %w", unwrap(err))
	}

	n, err := SendChunks(io.NopCloser(rd), func(chunk []byte) error {
		return stream.Send(&proto.PutRequest{
			Chunk: chunk,
		})
	})
	if err != nil {
		if err == io.EOF {
			_, err = stream.CloseAndRecv()
		}
		return n, unwrap(err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return n, unwrap(err)
	}
	return resp.BytesWritten, nil
}

func (s *Store) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	var grg *proto.GetRequest_Range
	if rg != nil {
		grg = &proto.GetRequest_Range{
			Offset: rg.Offset,
			Length: rg.Length,
		}
	}

	stream, err := s.client.Get(ctx, &proto.GetRequest{
		Mac:   mac[:],
		Type:  proto.StorageResource(res),
		Range: grg,
		Flags: proto.StorageFlags(storage.Flag(ctx)),
	})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", res, unwrap(err))
	}

	return ReceiveChunks(func() ([]byte, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, unwrap(err)
		}
		return resp.Chunk, nil
	}), nil
}

func (s *Store) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	_, err := s.client.Delete(ctx, &proto.DeleteRequest{
		Mac:   mac[:],
		Type:  proto.StorageResource(res),
		Flags: proto.StorageFlags(storage.Flag(ctx)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete %s: %w", res, unwrap(err))
	}
	return nil
}

func (s *Store) Close(ctx context.Context) error {
	_, err := s.client.Close(ctx, &proto.CloseRequest{})
	return unwrap(err)
}
