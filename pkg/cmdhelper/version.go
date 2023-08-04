package cmdhelper

import (
	"fmt"

	"github.com/DeBankDeFi/nodex/pkg/shared"

	"github.com/spf13/cobra"
)

func Version() *cobra.Command {
	cmd := cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(shared.AppInfo())
		},
	}
	return &cmd
}
