package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
)

func CommonPrefix(chainId, env string, isHeader bool) string {
	if isHeader {
		return fmt.Sprintf("%s/%s/header", chainId, env)
	}
	return fmt.Sprintf("%s/%s/block", chainId, env)
}

func Topic(chainId, env string, isHeader bool) string {
	if isHeader {
		return fmt.Sprintf("%s-%s-header", chainId, env)
	}
	return fmt.Sprintf("%s-%s-block", chainId, env)
}

func HeaderPrefix(info *pb.BlockInfo) string {
	return fmt.Sprintf("%s/%s/header/%012d/%012d/%s", info.ChainId,
		info.Env, info.BlockNum, info.MsgOffset, info.BlockHash)
}

func BlockPrefix(info *pb.BlockInfo) string {
	return fmt.Sprintf("%s/%s/block/%s", info.ChainId,
		info.Env, info.BlockHash)
}

func DBInfoPrefix(chainId, env string) string {
	return fmt.Sprintf("%s/%s/dbinfo", chainId, env)
}

func PrefixToHeaderInfo(key string) (info *pb.BlockInfo, err error) {
	info = &pb.BlockInfo{}
	keys := strings.Split(key, "/")
	if len(keys) != 6 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}
	info.ChainId = keys[0]
	info.Env = keys[1]
	info.BlockType = pb.BlockInfo_HEADER
	info.BlockNum, err = strconv.ParseInt(keys[3], 10, 64)
	if err != nil {
		return nil, err
	}
	info.MsgOffset, err = strconv.ParseInt(keys[4], 10, 64)
	if err != nil {
		return nil, err
	}
	info.BlockHash = keys[5]
	return info, nil
}

func PrefixToBlockInfo(key string) (info *pb.BlockInfo, err error) {
	info = &pb.BlockInfo{}
	keys := strings.Split(key, "/")
	if len(keys) != 4 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}
	info.ChainId = keys[0]
	info.Env = keys[1]
	info.BlockType = pb.BlockInfo_DATA
	info.BlockHash = keys[3]
	return info, nil
}
