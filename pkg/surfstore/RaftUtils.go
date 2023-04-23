package surfstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}

	server := RaftSurfstore{
		state:         Follower,
		isLeaderMutex: &isLeaderMutex,
		term:          0,
		metaStore:     NewMetaStore(config.BlockAddrs),
		log:           []*UpdateOperation{{Term: 0, FileMetaData: nil}},
		id:            id,
		raftAddrs:     config.RaftAddrs,
		commitIndex:   0,
		lastApplied:   0,
	}
	go server.run(context.Background(), &emptypb.Empty{})

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	lis, err := net.Listen("tcp", server.raftAddrs[server.id])
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)
	log.Printf("Starting %d service on %s\n", server.id, server.raftAddrs[server.id])
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
