package shared

import (
	"strings"
)

type Info struct {
	Version Version
	AppName string
}

type Version struct {
	GitVersion   string
	GitCommit    string
	GitBranch    string
	GitTreeState string
	BuildTime    string
	GoVersion    string
	Compiler     string
	Platform     string
}

func (i Info) String() string {
	var buf strings.Builder
	buf.WriteString("AppName: " + "\t" + i.AppName)
	buf.WriteByte('\n')
	buf.WriteString("GitVersion: " + "\t" + i.Version.GitVersion)
	buf.WriteByte('\n')
	buf.WriteString("GitCommit: " + "\t" + i.Version.GitCommit)
	buf.WriteByte('\n')
	buf.WriteString("GitBranch: " + "\t" + i.Version.GitBranch)
	buf.WriteByte('\n')
	buf.WriteString("GitTreeState: " + "\t" + i.Version.GitTreeState)
	buf.WriteByte('\n')
	buf.WriteString("BuildTime: " + "\t" + i.Version.BuildTime)
	buf.WriteByte('\n')
	buf.WriteString("GoVersion: " + "\t" + i.Version.GoVersion)
	buf.WriteByte('\n')
	buf.WriteString("Compiler: " + "\t" + i.Version.Compiler)
	buf.WriteByte('\n')
	buf.WriteString("Platform: " + "\t" + i.Version.Platform)
	return buf.String()
}
