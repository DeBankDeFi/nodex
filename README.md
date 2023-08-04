# Nodex
NodeX is a Geth read-only cluster synchronization solution based on a master-slave architecture, which does not require peer-to-peer or consensus components.
Based on Kafka and S3, NodeX's reader geth nodes can synchronize without the need for p2p communication and block validation.

## Components
* Write nodes: a normal geth node, which is used to synchronize the block data to S3 and Kafka. Each time Blockheader is committed, they are first committed to S3.
* Compute nodes: Compute nodes, follow write nodes without block synchronization.No local disk is mounted, the database is accessed via the Remotedb interface through gRPC.
* Remotedb: Read from S3, apply Blockdata and Blockheader to the DB, and provide an access interface to the underlying data for Compute nodes via gRPC.
* S3Proxy: A proxy that provides access to S3 data for Write nodes and Remotedb via gRPC.
* Ndrc: A command line tool for managing NodeX clusters.

## Design
see [Technical Design](doc/Technical%20Design.md)

## How to use 
see [How to use](doc/How%20to%20use.md)