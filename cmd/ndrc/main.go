package main

import (
	"github.com/DeBankDeFi/nodex/pkg/types"

	"github.com/DeBankDeFi/nodex/pkg/cmdhelper"
	"github.com/DeBankDeFi/nodex/pkg/shared"
	"github.com/spf13/cobra"
)

var flag types.NdrcFlag

func main() {
	shared.SetAppName("ndrc")
	cmd := rootCmd()
	cmd.AddCommand(daemonCmd())
	cmd.AddCommand(testingCmd())
	cmd.AddCommand(failoverCmd())
	cmd.AddCommand(cmdhelper.Version())
	cmd.Execute()
}

func rootCmd() *cobra.Command {
	cmd := cobra.Command{
		Use:  shared.GetAppName(),
		Long: "A cordinator that handle data replication of blockchain node(s).",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}
	return setFlags(cmd)
}

func setFlags(cmd cobra.Command) *cobra.Command {
	cmdhelper.ResolveFlagVariable(&cmd, &flag)
	return &cmd
}
