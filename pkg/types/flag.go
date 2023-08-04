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

package types

// CommonFlag ...
type CommonFlag struct {
	DevelopmentMode bool `type:"bool" shorthand:"d" enable-env:"true" usage:"development mode" json:"development_mode"`
}

// NdrcFlag ...
type NdrcFlag struct {
	CommonFlag
	Config string `type:"string" shorthand:"c" enable-env:"true" usage:"configuration file, default is config.yaml" json:"config"`
}

// DaemonFlag ...
type DaemonFlag struct {
	UseS3       bool   `type:"bool" shorthand:"s" enable-env:"true" usage:"use s3" json:"use_s3"`
	S3Region    string `type:"string" shorthand:"r" enable-env:"true" usage:"s3 region" json:"s3_region"`
	BucketName  string `type:"string" shorthand:"b" enable-env:"true" usage:"bucket name" json:"bucket_name"`
	Prefix      string `type:"string" shorthand:"x" enable-env:"true" usage:"prefix" json:"prefix"`
	GRPCListen  string `type:"string" shorthand:"g" enable-env:"true" usage:"listen address of grpc server" json:"grpc_listen"`
	HTTPListen  string `type:"string" shorthand:"l" enable-env:"true" usage:"listen address of http server" json:"http_listen"`
	EnablePprof bool   `type:"bool" shorthand:"p" enable-env:"true" usage:"enable pprof server" json:"enable_pprof"`
}

type TestingFlag struct {
	GrpcService string `type:"string" shorthand:"t" enable-env:"true" usage:"service that need to testing" json:"grpc_service"`
	GrpcServer  string `type:"string" shorthand:"g" enable-env:"true" usage:"address of grpc server" json:"grpc_server"`
}

// FailoverFlag is the flag for failover tool
type FailoverFlag struct {
	Env        string `type:"string" shorthand:"e" enable-env:"true" usage:"environment" json:"env"`
	ChainID    string `type:"string" shorthand:"i" enable-env:"true" usage:"chain id" json:"chain_id"`
	Role       int    `type:"int" shorthand:"r" enable-env:"true" usage:"role 1 master, 2 backup" json:"role"`
	GrpcServer string `type:"string" shorthand:"g" enable-env:"true" usage:"address of grpc server" json:"grpc_server"`
}
