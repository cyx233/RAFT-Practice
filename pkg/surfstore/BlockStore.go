package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		block, exist := bs.BlockMap[blockHash.Hash]
		if !exist {
			return nil, errors.New("Hash Not Found")
		}
		return block, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	select {
	case <-ctx.Done():
		return &Success{Flag: false}, ctx.Err()
	default:
		bs.BlockMap[GetBlockHashString(block.BlockData)] = &Block{
			BlockData: block.BlockData,
			BlockSize: block.BlockSize,
		}
		return &Success{Flag: true}, nil
	}
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		ans := &BlockHashes{
			Hashes: make([]string, 0),
		}
		for _, v := range blockHashesIn.Hashes {
			_, exist := bs.BlockMap[v]
			if exist {
				ans.Hashes = append(ans.Hashes, v)
			}
		}
		return ans, nil
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := make([]string, 0)
	for k := range bs.BlockMap {
		hashes = append(hashes, k)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
