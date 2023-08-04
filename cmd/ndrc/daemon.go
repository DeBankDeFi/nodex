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

	"github.com/DeBankDeFi/nodex/pkg/daemon"
	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/types"

	"github.com/DeBankDeFi/nodex/pkg/cmdhelper"
	"github.com/spf13/cobra"
)

var daemonFlag types.DaemonFlag

func daemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the daemon server",
		Run:   daemonRun,
	}
	cmdhelper.ResolveFlagVariable(cmd, &daemonFlag)
	if daemonFlag.GRPCListen == "" {
		daemonFlag.GRPCListen = ":8089"
	}
	if daemonFlag.HTTPListen == "" {
		daemonFlag.HTTPListen = ":8088"
	}
	return cmd
}

func daemonRun(cmd *cobra.Command, args []string) {
	if flag.DevelopmentMode {
		log.DevelopmentModeWithoutStackTrace()
	} else {
		log.ProductionModeWithoutStackTrace()
	}

	rootCtx := context.Background()
	d, err := daemon.NewDaemon(rootCtx, &daemonFlag)
	if err != nil {
		log.Fatal("daemon server error created", err)
	}
	d.Start()
}
