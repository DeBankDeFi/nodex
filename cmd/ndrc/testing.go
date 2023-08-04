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

package main

import (
	"context"
	"io"

	"go.uber.org/zap"

	"github.com/DeBankDeFi/nodex/pkg/cmdhelper"
	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/types"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var testingFlag types.TestingFlag

func testingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "testing",
		Short: "Testing the daemon server",
		Run:   testingRun,
	}

	cmdhelper.ResolveFlagVariable(cmd, &testingFlag)
	if testingFlag.GrpcService == "" {
		testingFlag.GrpcService = "heartbeat"
	}
	if testingFlag.GrpcServer == "" {
		testingFlag.GrpcServer = "127.0.0.1:8089"
	}

	return cmd
}

func testingRun(cmd *cobra.Command, args []string) {
	switch testingFlag.GrpcService {
	case "heartbeat":
		testingHeartbeat()
	case "subscribe":
		testingSubscribe()
	default:
		log.Fatal("Unsupported testing case", nil)
	}
}

func testingHeartbeat() {
	conn, err := grpc.Dial(testingFlag.GrpcServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("did not connect to grpc server", err)
	}
	defer conn.Close()

	client := pb.NewHeartbeatServiceClient(conn)
	stream, err := client.Report(context.Background())
	if err != nil {
		log.Fatal("report heartbeat to ndrc failed", err)
	}
	err = stream.Send(&pb.HeartbeatRequest{
		Id: &pb.NodeId{
			ChainId: "eth",
			Env:     "product",
			Uuid:    uuid.New().String(),
			Role:    pb.NodeRole_WRITERM,
		},
	})
	if err != nil {
		log.Fatal("report heartbeat to ndrc failed", err)
	}

	log.Info("report heartbeat to ndrc successfully")
}

func testingSubscribe() {
	conn, err := grpc.Dial(testingFlag.GrpcServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("did not connect to grpc server", err)
	}
	defer conn.Close()

	client := pb.NewSubscribeServiceClient(conn)
	stream, err := client.WatchWriterEvent(context.Background(), &pb.WriterEventSubcribeRequest{
		Timeout: 120,
	})
	if err != nil {
		log.Fatal("watch writer event failed", err)
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			log.Info("server end connection, quit.")
			return
		}
		if err != nil {
			log.Fatal("server got error, quit", err)
		}
		log.Info("got writer event from server", zap.Any("leader", in.Leader), zap.Any("event", in.Event.String()))
	}
}
