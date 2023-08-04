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

### Write Process
- Writer nodes submit Blockdata
  - First, they are committed to S3
  - Then, the write batch is committed to the DB
- Writer nodes submit Blockheader
  - First, they are committed to S3
  - Then, Blockinfo is appended to the write batch
  - Finally, the write batch is committed to the DB

### Replication Process
#### Message discovery and backtracking methods
Message discovery and backtracking are both based on blockheight. Since the key of Blockheader is blockheight/write timestamp, in most cases, a blockheight corresponds to a Blockheader. When a Chain reorg occurs, there are multiple different Blockheaders for the equivalent blockheight of the reorg, and the Blockheader with the latest write timestamp is the block of the reorg.
Since POS chains all have eventual consistency, Chain reorg has a maximum value (64 for ETH 2.0). For example, for ETH 2.0, if the maximum height of the chain that has been processed is 128, then no reorg block will be received for heights 0~64, but it is possible to receive a reorg block for heights 64~128.
Specifically, there are two situations:
1. For blocks with a blockheight of the latest blockheight-reorg max, the block with blockheight/latest timestamp is the final state of this block across the entire network and can be applied directly.
2. Blocks with a blockheight in the range [highest blockheight-reorg max, blockheight] may change due to reorg, so we need to watch for updates within this range.
Using blockheight as the key has a significant advantage. Compared to msgoffset, blockheight is more meaningful in business. For example,
  - The business may report that the data after a certain blockheight is incorrect, and we can backtrack to before this height and reapply the block.

### Block Discovery
- We save the highest blockheight and the timestamp of the latest applied block in the DB.
- Each time we list the Blockheader of [highest blockheight-reorg max, +∞].
- For multiple blocks with the same blockheight, we only keep the block with the latest timestamp.
- We apply blocks in order of blockheight, where the timestamp of the block is greater than the timestamp of the latest applied block.

### Block Backtracking
- We get the timestamp of the Blockheader of the height to be backtracked from S3, set the highest blockheight saved to the backtracked height, and set the timestamp of the latest applied block to the timestamp of the Blockheader of this height (if there are multiple timestamps, set it to the latest).
- Each time we list the Blockheader of [highest blockheight-reorg max, +∞].
- For multiple blocks with the same blockheight, we only keep the block with the latest timestamp.
- We apply blocks in order of blockheight, where the timestamp of the block is greater than the timestamp of the latest applied block.

### Startup
1. The Replicator reads the Blockinfo from the DB to get the blockheight and write timestamp.
2. Watch for updates to the Blockheader files from blockheight-128 (maximum fork height) to blockheight+128 in S3.
3. Apply Blockheader files and Blockdata files in order.

### Node Switching
1. Master/standby are distinguished by prefix.
2. The Replicator switches the prefix of the S3 client.
3. Reset the Blockinfo to the height/blockheight of blockheight-128 (ensure that this height has not forked).
4. Same as the startup process.