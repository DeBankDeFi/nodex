package nodemanager

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/DeBankDeFi/nodex/pkg/lib/broker"
	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type writerNodeInfo struct {
	nodeId       *pb.NodeId
	online       bool
	lastSeenTime int64
}

// WriterNodePool is a pool of nodes.
type WriterNodePool struct {
	nodes      map[string]*writerNodeInfo
	lock       sync.RWMutex
	broker     *broker.Broker[pb.WriterEventResponse]
	lastLeader *writerNodeInfo
	s3Client   *s3.Client
	bucketName string
	prefix     string
}

// NewWriterNodePool creates a new NodePool.
func NewWriterNodePool(s3Client *s3.Client, bucket, prefix string) *WriterNodePool {
	pool := &WriterNodePool{
		nodes:      make(map[string]*writerNodeInfo),
		broker:     broker.NewBroker[pb.WriterEventResponse](),
		s3Client:   s3Client,
		bucketName: bucket,
		prefix:     prefix,
	}
	if s3Client != nil {
		results, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &prefix,
		})
		if err != nil {
			return pool
		}
		defer results.Body.Close()
		var body *pb.NodeId
		data, err := io.ReadAll(results.Body)
		if err != nil {
			return pool
		}

		if err := json.Unmarshal(data, body); err != nil {
			return pool
		}
		pool.lastLeader = &writerNodeInfo{
			nodeId:       body,
			online:       true,
			lastSeenTime: time.Now().Unix(),
		}
	}
	return pool
}

// Update updates the pool with a new node.
func (p *WriterNodePool) Update(node *pb.NodeId) {
	p.lock.Lock()
	defer p.lock.Unlock()
	info := &writerNodeInfo{
		nodeId:       node,
		online:       true,
		lastSeenTime: time.Now().Unix(),
	}
	p.nodes[node.Uuid] = info
}

// OffLine marks a node as offline.
func (p *WriterNodePool) OffLine(uuid string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if node, ok := p.nodes[uuid]; ok {
		node.online = false
	}
}

// SetLeader sets the leader role.
func (p *WriterNodePool) SetLeader(id *pb.NodeId) {
	p.lock.Lock()
	defer p.lock.Unlock()
	expireTime := time.Now().Add(time.Hour * 24 * 365)
	if p.s3Client != nil {
		data, err := json.Marshal(id)
		if err == nil {
			if _, err := p.s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:  &p.bucketName,
				Key:     &p.prefix,
				Body:    bytes.NewReader(data),
				Expires: &expireTime,
			}); err != nil {
				log.Error("failed to write leader to s3", err)
			}
		} else {
			log.Error("failed to marshal node id", err)
		}
	}
	info := &writerNodeInfo{
		nodeId:       id,
		online:       true,
		lastSeenTime: time.Now().Unix(),
	}
	p.nodes[id.Uuid] = info
	p.lastLeader = info
	p.broker.Publish(&pb.WriterEventResponse{
		Event:  pb.WriterEvent_ROLE_CHANGED,
		Leader: id,
	})
}

// GetLeader returns the leader node.
func (p *WriterNodePool) GetLeader() *pb.NodeId {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.lastLeader != nil {
		return p.lastLeader.nodeId
	}
	return nil
}

// Subscribe subscribes to the event broker.
func (p *WriterNodePool) Subscribe() chan *pb.WriterEventResponse {
	return p.broker.Subscribe()
}
