package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
)

func CommonPrefix(chainId, env string, blockTy pb.BlockInfo_BlockType) string {
	switch blockTy {
	case pb.BlockInfo_HEADER:
		return fmt.Sprintf("%s/%s/header", chainId, env)
	case pb.BlockInfo_DATA:
		return fmt.Sprintf("%s/%s/block", chainId, env)
	case pb.BlockInfo_MEM:
		return fmt.Sprintf("%s/%s/mem", chainId, env)
	default:
		panic("invalid block type")
	}
}

func Topic(chainId, env string) string {
	return fmt.Sprintf("%s-%s-header", chainId, env)
}

func InfoToPrefix(info *pb.BlockInfo) string {
	switch info.BlockType {
	case pb.BlockInfo_HEADER:
		return HeaderPrefix(info)
	case pb.BlockInfo_DATA:
		return BlockPrefix(info)
	case pb.BlockInfo_MEM:
		return MemPrefix(info)
	default:
		panic("invalid block type")
	}
}

func HeaderPrefix(info *pb.BlockInfo) string {
	return fmt.Sprintf("%s/%s/header/%012d/%012d/%s", info.ChainId,
		info.Env, info.BlockNum, info.MsgOffset, info.BlockHash)
}

func BlockPrefix(info *pb.BlockInfo) string {
	return fmt.Sprintf("%s/%s/block/%s", info.ChainId,
		info.Env, info.BlockHash)
}

func MemPrefix(info *pb.BlockInfo) string {
	return fmt.Sprintf("%s/%s/mem/%s", info.ChainId,
		info.Env, info.BlockRoot)
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

func PrefixToMemkInfo(key string) (info *pb.BlockInfo, err error) {
	info = &pb.BlockInfo{}
	keys := strings.Split(key, "/")
	if len(keys) != 4 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}
	info.ChainId = keys[0]
	info.Env = keys[1]
	info.BlockType = pb.BlockInfo_MEM
	info.BlockRoot = keys[3]
	return info, nil
}
