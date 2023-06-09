package surfstore

import (
	context "context"
	"errors"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	RaftAddrs []string
	BaseDir   string
	BlockSize int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	if block == nil {
		return errors.New("block == nil")
	}
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.GetBlockData()
	block.BlockSize = b.GetBlockSize()

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	if succ == nil {
		return errors.New("succ == nil")
	}
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	res, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = res.GetFlag()

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	if blockHashesOut == nil {
		return errors.New("blockHashesOut == nil")
	}
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	bs, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = bs.GetHashes()

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	if blockHashes == nil {
		return errors.New("blockHashesOut == nil")
	}
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	defer cancel()
	bs, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = bs.GetHashes()

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	if serverFileInfoMap == nil {
		return errors.New("serverFileInfoMap == nil")
	}
	// connect to the server
	for i := 0; i < len(surfClient.RaftAddrs); i++ {
		conn, err := grpc.Dial(surfClient.RaftAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
		infoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if status.Code(err) == codes.Unavailable {
			conn.Close()
			cancel()
			continue
		}
		if err != nil {
			conn.Close()
			cancel()
			return err
		}
		*serverFileInfoMap = infoMap.GetFileInfoMap()
		cancel()
		return conn.Close()
	}
	return ErrLeaderNotFound
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	if latestVersion == nil {
		return errors.New("latestVersion == nil")
	}
	for i := 0; i < len(surfClient.RaftAddrs); i++ {
		// connect to the server
		conn, err := grpc.Dial(surfClient.RaftAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
		v, err := c.UpdateFile(ctx, fileMetaData)
		if status.Code(err) == codes.Unavailable {
			cancel()
			conn.Close()
			continue
		}
		if err != nil {
			cancel()
			conn.Close()
			return err
		}
		*latestVersion = v.GetVersion()
		cancel()
		return conn.Close()
	}
	return ErrLeaderNotFound
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddr *[]string) error {
	if blockStoreAddr == nil {
		return errors.New("blockStoreAddr == nil")
	}
	for i := 0; i < len(surfClient.RaftAddrs); i++ {
		// connect to the server
		conn, err := grpc.Dial(surfClient.RaftAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
		v, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if status.Code(err) == codes.Unavailable {
			conn.Close()
			cancel()
			continue
		}
		if err != nil {
			cancel()
			conn.Close()
			return err
		}
		*blockStoreAddr = v.GetBlockStoreAddrs()

		cancel()
		conn.Close()
	}
	return ErrLeaderNotFound
}

func (surfClient *RPCClient) GetHashAddrMap(blockHashesIn []string, hashAddrMap *map[string]string) error {
	if hashAddrMap == nil {
		return errors.New("blockStoreAddr == nil")
	}
	for i := 0; i < len(surfClient.RaftAddrs); i++ {
		// connect to the server
		conn, err := grpc.Dial(surfClient.RaftAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
		bs, err := c.GetHashAddrMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if status.Code(err) == codes.Unavailable {
			conn.Close()
			cancel()
			continue
		}
		if err != nil {
			conn.Close()
			cancel()
			return err
		}
		*hashAddrMap = bs.GetHashAddrMap()

		cancel()
		return conn.Close()
	}
	return ErrLeaderNotFound
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(RaftAddrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		RaftAddrs: RaftAddrs,
		BaseDir:   baseDir,
		BlockSize: blockSize,
	}
}
