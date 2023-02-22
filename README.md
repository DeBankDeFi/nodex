# db-replicator
主从架构的leveldb/rocksdb复制器


## Design Overview

<!-- ![整体架构](resource/db.png) -->

## Getting Started

### Prerequisites

Go >= 1.18

### How to Use

## Build

1. 生成protobuf文件
`protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pb/*.proto`
TODO