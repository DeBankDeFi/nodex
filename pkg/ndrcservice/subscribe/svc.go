// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package subscribe

import (
	"context"

	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/nodemanager"
	"github.com/DeBankDeFi/nodex/pkg/pb"
)

// GrpcSubscribeSVC ...
type GrpcSubscribeSVC struct {
	pb.UnimplementedSubscribeServiceServer
	writePool *nodemanager.WriterNodePool
	readPool  *nodemanager.ReaderNodePool
}

func NewGrpcSubscribeSVC(writePool *nodemanager.WriterNodePool,
	readPool *nodemanager.ReaderNodePool) *GrpcSubscribeSVC {
	return &GrpcSubscribeSVC{
		writePool: writePool,
		readPool:  readPool,
	}
}

func (s *GrpcSubscribeSVC) WatchWriterEvent(in *pb.WriterEventSubcribeRequest, stream pb.SubscribeService_WatchWriterEventServer) error {
	watchCh := s.writePool.Subscribe()
	leader := s.writePool.GetLeader()
	if leader != nil {
		err := stream.Send(&pb.WriterEventResponse{
			Event:  pb.WriterEvent_ROLE_CHANGED,
			Leader: leader,
		})
		if err != nil {
			log.Error("send error watch event", err)
			return nil
		}
	}
outer:
	for {
		select {
		case event := <-watchCh:
			err := stream.Send(event)
			if err != nil {
				log.Error("send error watch event", err)
				break outer
			}
		case <-stream.Context().Done():
			log.Info("event watch canceled")
			break outer
		}
	}
	return nil
}

func (s *GrpcSubscribeSVC) ListReader(ctx context.Context, in *pb.ListReaderRequest) (*pb.ListReaderResponse, error) {
	return &pb.ListReaderResponse{
		Readers: s.readPool.GetReaders(),
	}, nil
}
