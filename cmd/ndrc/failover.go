package main

import (
	"context"

	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/types"

	"github.com/DeBankDeFi/nodex/pkg/cmdhelper"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var failoverFlag types.FailoverFlag

func failoverCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "failover",
		Short: "make reader switch to another writer",
		Run:   failoverRun,
	}
	cmdhelper.ResolveFlagVariable(cmd, &failoverFlag)
	return cmd
}

func failoverRun(cmd *cobra.Command, args []string) {
	conn, err := grpc.Dial(failoverFlag.GrpcServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("did not connect to grpc server", err)
	}
	defer conn.Close()
	client := pb.NewHeartbeatServiceClient(conn)
	_, err = client.SetRole(context.Background(), &pb.SetRoleRequest{
		Id: &pb.NodeId{
			Env:     failoverFlag.Env,
			ChainId: failoverFlag.ChainID,
			Role:    pb.NodeRole(failoverFlag.Role),
		},
	})
	if err != nil {
		log.Fatal("failover failed", err)
		return
	}
}
