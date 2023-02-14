package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
)

func CommonPrefix(env, chainId, role string, blockTy pb.BlockInfo_BlockType) string {
	switch blockTy {
	case pb.BlockInfo_HEADER:
		return fmt.Sprintf("%s/%s/%s/header", env, chainId, role)
	case pb.BlockInfo_DATA:
		return fmt.Sprintf("%s/%s/%s/block", env, chainId, role)
	default:
		panic("invalid block type")
	}
}

func Topic(env, chainId, role string) string {
	return fmt.Sprintf("%s-%s-%s-header", env, chainId, role)
}

func InfoToPrefix(info *pb.BlockInfo) string {
	switch info.BlockType {
	case pb.BlockInfo_HEADER:
		return HeaderPrefix(info)
	case pb.BlockInfo_DATA:
		return BlockPrefix(info)
	default:
		panic("invalid block type")
	}
}

func HeaderPrefix(info *pb.BlockInfo) string {
	commonPrefix := CommonPrefix(info.Env, info.ChainId, info.Role, pb.BlockInfo_HEADER)
	return fmt.Sprintf("%s/%012d/%012d/%s", commonPrefix, info.BlockNum, info.MsgOffset, info.BlockHash)
}

func BlockPrefix(info *pb.BlockInfo) string {
	commonPrefix := CommonPrefix(info.Env, info.ChainId, info.Role, pb.BlockInfo_DATA)
	return fmt.Sprintf("%s/%s", commonPrefix, info.BlockHash)
}

func PrefixToHeaderInfo(key string) (info *pb.BlockInfo, err error) {
	info = &pb.BlockInfo{}
	keys := strings.Split(key, "/")
	if len(keys) != 7 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}
	info.Env = keys[0]
	info.ChainId = keys[1]
	info.Role = keys[2]
	info.BlockType = pb.BlockInfo_HEADER
	info.BlockNum, err = strconv.ParseInt(keys[4], 10, 64)
	if err != nil {
		return nil, err
	}
	info.MsgOffset, err = strconv.ParseInt(keys[5], 10, 64)
	if err != nil {
		return nil, err
	}
	info.BlockHash = keys[5]
	return info, nil
}

func PrefixToBlockInfo(key string) (info *pb.BlockInfo, err error) {
	info = &pb.BlockInfo{}
	keys := strings.Split(key, "/")
	if len(keys) != 5 {
		return nil, fmt.Errorf("invalid key: %s", key)
	}
	info.ChainId = keys[0]
	info.Env = keys[1]
	info.Role = keys[2]
	info.BlockType = pb.BlockInfo_DATA
	info.BlockHash = keys[4]
	return info, nil
}
