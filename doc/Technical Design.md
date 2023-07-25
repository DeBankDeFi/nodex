## Design
### Introduce
P2p synchronization failure may be the most frequent and troublesome issue encountered by the Debank blockchain team when maintaining the ETH node cluster.
Typical manifestations include:
* A node does not have any peers
* A node might have peers, but still not receive new blocks
* A node might be starting up, but the synchronization process is very slow.

In addition, the p2p synchronization mechanism also limits the horizontal scaling capability of the ETH cluster. Even using a snapshot within one day, deploying a new ETH node and bringing it up to date takes several hours. The process of discovering peers from bootnodes, downloading and validating blocks, and updating the state trie all consume a significant amount of time.

In order to minimize the impact of p2p synchronization on the synchronization stability of the Ethereum cluster, and to provide minute-level horizontal scaling capabilities, the Debank team has developed NodeX.

Nodex runs as a cluster of servers. A cluster consists of one **writer** node, which generally behaves as a conventional Geth node, connecting to peers and validating blocks, but it also commits database modifications to S3 and notifies other **reader** nodes via Kafka. One or more **reader** nodes, which pull the **writer**'s changes via Kafka and store them locally. 

These **reader** nodes work as rpc nodes, which can be horizontal scaling to meet debank’s need for RPC capacity. 

Because **reader** nodes don't need to connect with other's peers and validate blocks, So they can be run on cheaper hardware.