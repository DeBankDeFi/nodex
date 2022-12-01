package replicator

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/DeBankDeFi/db-replicator/pkg/utils/blockpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	s3      *s3.Client
	bucket  string
	envPrex string
}

func NewS3Client(ctx context.Context, bucket string, envPrex string) (client *Client, err error) {
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	s3 := s3.NewFromConfig(sdkConfig)
	client = &Client{
		s3:      s3,
		bucket:  bucket,
		envPrex: envPrex,
	}
	return client, nil
}

func (c *Client) ListHeaderStartAt(ctx context.Context, blockNum int64, count int32, after int64) (objects []types.Object, err error) {
	result, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(c.bucket),
		Prefix:     aws.String(fmt.Sprintf("%s/header", c.envPrex)),
		MaxKeys:    count,
		StartAfter: aws.String(fmt.Sprintf("%s/header/%d", c.envPrex, blockNum)),
	})
	if err != nil {
		return nil, err
	}
	for _, object := range result.Contents {
		if object.LastModified.UnixMilli() > after {
			objects = append(objects, object)
		}
	}
	return result.Contents, nil
}

func (c *Client) GetHeaderFile(ctx context.Context, key *string) (header *blockpb.BlockHeader, err error) {
	result, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    key,
	})
	if result != nil {
		defer result.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	header = &blockpb.BlockHeader{}
	err = proto.Unmarshal(buf, header)
	if err != nil {
		return nil, err
	}
	header.Info.LastTime = result.LastModified.UnixMilli()
	return header, nil
}

func (c *Client) PutHeaderFile(ctx context.Context, header *blockpb.BlockHeader) (err error) {
	buf, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	_, err = c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fmt.Sprintf("%s/header/%d", c.envPrex, header.Info.BlockNum)),
		Body:   bytes.NewReader(buf),
	})
	return err
}

func (c *Client) GetBlockFile(ctx context.Context, blockHash string) (header *blockpb.BlockData, err error) {
	result, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fmt.Sprintf("%s/block/%s", c.envPrex, blockHash)),
	})
	if result != nil {
		defer result.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	header = &blockpb.BlockData{}
	err = proto.Unmarshal(buf, header)
	if err != nil {
		return nil, err
	}
	header.Info.LastTime = result.LastModified.UnixMilli()
	return header, nil
}

func (c *Client) PutBlockFile(ctx context.Context, block *blockpb.BlockData) (err error) {
	buf, err := proto.Marshal(block)
	if err != nil {
		return err
	}
	_, err = c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(fmt.Sprintf("%s/block/%s", c.envPrex, block.Info.BlockHash)),
		Body:   bytes.NewReader(buf),
	})
	return err
}
