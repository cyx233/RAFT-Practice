package surfstore

import (
	context "context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	mu                 sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		m.mu.Lock()
		defer m.mu.Unlock()
		fileInfoMap := make(map[string]*FileMetaData)
		for k, v := range m.FileMetaMap {
			fileInfoMap[k] = v
		}
		return &FileInfoMap{FileInfoMap: fileInfoMap}, nil
	}
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	select {
	case <-ctx.Done():
		return &Version{Version: -1}, ctx.Err()
	default:
		if fileMetaData == nil {
			return &Version{Version: -1}, status.Error(codes.InvalidArgument, "fileMetaData == nil")
		}
		fname := fileMetaData.GetFilename()
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.FileMetaMap[fname].GetVersion() <= fileMetaData.GetVersion() {
			m.FileMetaMap[fname] = &FileMetaData{
				Filename:      fileMetaData.GetFilename(),
				Version:       fileMetaData.GetVersion(),
				BlockHashList: fileMetaData.GetBlockHashList(),
			}
			ver := &Version{Version: m.FileMetaMap[fname].GetVersion()}
			return ver, nil
		} else {
			return &Version{Version: -1}, status.Error(codes.Aborted, ErrOldVer.Error())
		}
	}
}

func (m *MetaStore) GetHashAddrMap(ctx context.Context, blockHashesIn *BlockHashes) (*HashAddrMap, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		hashAddrMap := make(map[string]string)
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, v := range blockHashesIn.Hashes {
			addr := m.ConsistentHashRing.GetResponsibleServer(v)
			hashAddrMap[v] = addr
		}
		return &HashAddrMap{HashAddrMap: hashAddrMap}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
	}
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
