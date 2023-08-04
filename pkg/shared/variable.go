package shared

import (
	"fmt"
	"runtime"
)

const unknown = "unknown"

var (
	appInfo = Info{
		AppName: unknown,
		Version: Version{
			GitVersion:   gitVersion,
			GitCommit:    gitCommit,
			GitBranch:    gitBranch,
			GitTreeState: gitTreeState,
			BuildTime:    buildTime,
			GoVersion:    runtime.Version(),
			Compiler:     runtime.Compiler,
			Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		},
	}
)
