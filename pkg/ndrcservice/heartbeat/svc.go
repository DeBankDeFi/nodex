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

package heartbeat

import (
	"context"

	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/nodemanager"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"go.uber.org/zap"
)

// GrpcHeartbeatSVC ...
type GrpcHeartbeatSVC struct {
	pb.UnimplementedHeartbeatServiceServer
	writePool *nodemanager.WriterNodePool
	readPool  *nodemanager.ReaderNodePool
}

// NewGrpcHeartbeatSVC ...
func NewGrpcHeartbeatSVC(writePool *nodemanager.WriterNodePool,
	readPool *nodemanager.ReaderNodePool) *GrpcHeartbeatSVC {
	return &GrpcHeartbeatSVC{
		writePool: writePool,
		readPool:  readPool,
	}
}

func (g *GrpcHeartbeatSVC) Report(client pb.HeartbeatService_ReportServer) error {
	var nodeId *pb.NodeId
	defer func() {
		if nodeId != nil {
			if nodeId.Role == pb.NodeRole_WRITERB ||
				nodeId.Role == pb.NodeRole_WRITERM {
				g.writePool.OffLine(nodeId.Uuid)
			}
			if nodeId.Role == pb.NodeRole_READER {
				g.readPool.OffLine(nodeId.Uuid)
			}
		}
	}()
	for {
		req, err := client.Recv()
		if err != nil {
			return nil
		}
		if req.Id == nil {
			continue
		}
		nodeId = req.Id
		log.Info("heartbeat report received", zap.Any("node_id", req.Id))
		if nodeId.Role == pb.NodeRole_WRITERB ||
			nodeId.Role == pb.NodeRole_WRITERM {
			g.writePool.Update(nodeId)
		}
		if nodeId.Role == pb.NodeRole_READER {
			g.readPool.Update(nodeId)
		}
	}
}

func (g *GrpcHeartbeatSVC) SetRole(ctx context.Context, req *pb.SetRoleRequest) (*pb.SetRoleResponse, error) {
	if req.Id == nil {
		return nil, nil
	}
	g.writePool.SetLeader(req.Id)
	return &pb.SetRoleResponse{}, nil
}
