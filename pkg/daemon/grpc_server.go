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

package daemon

import (
	"github.com/DeBankDeFi/nodex/pkg/internal/server/grpc"
	"github.com/DeBankDeFi/nodex/pkg/ndrcservice/heartbeat"
	"github.com/DeBankDeFi/nodex/pkg/ndrcservice/subscribe"
	"github.com/DeBankDeFi/nodex/pkg/nodemanager"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"google.golang.org/grpc/reflection"
)

type grpcServer struct {
	pb.HeartbeatServiceServer
	pb.SubscribeServiceServer
}

func newGrpcServer(s3Client *s3.Client, bucket, prefix string) *grpcServer {
	s := &grpcServer{}
	readerPool := nodemanager.NewReaderNodePool()
	writerPool := nodemanager.NewWriterNodePool(s3Client, bucket, prefix)
	s.HeartbeatServiceServer = heartbeat.NewGrpcHeartbeatSVC(writerPool, readerPool)
	s.SubscribeServiceServer = subscribe.NewGrpcSubscribeSVC(writerPool, readerPool)
	return s
}

func registerServices(srv *grpc.Server, s3Client *s3.Client, bucket, prefix string) {
	s := newGrpcServer(s3Client, bucket, prefix)
	pb.RegisterHeartbeatServiceServer(srv.GrpcServer(), s)
	pb.RegisterSubscribeServiceServer(srv.GrpcServer(), s)
	reflection.Register(srv.GrpcServer())
}
