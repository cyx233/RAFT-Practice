package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	id            int64
	raftAddrs     []string

	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) checkIsLeader() bool {
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	if s.isLeader {
		return true
	} else {
		return false
	}
}

func (s *RaftSurfstore) checkIsCrash() bool {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	if s.isCrashed {
		return true
	} else {
		return false
	}
}

func (s *RaftSurfstore) runStateMachine(ctx context.Context) (*Version, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		var v *Version
		var err error
		for s.lastApplied < s.commitIndex {
			v, err = s.metaStore.UpdateFile(ctx, s.log[s.lastApplied+1].GetFileMetaData())
			if err != nil {
				return nil, err
			}
			s.lastApplied += 1
		}
		return v, nil
	}
}

func (s *RaftSurfstore) readPrep(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if s.checkIsCrash() {
			return ERR_SERVER_CRASHED
		}
		if !s.checkIsLeader() {
			return ERR_NOT_LEADER
		}
		if err := s.getResponse(ctx); err != nil {
			return err
		}
		if _, err := s.runStateMachine(ctx); err != nil {
			return err
		}
		return nil
	}
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if err := s.readPrep(ctx); err != nil {
			return nil, err
		}
		return s.metaStore.GetFileInfoMap(ctx, empty)
	}
}

func (s *RaftSurfstore) GetHashAddrMap(ctx context.Context, hashes *BlockHashes) (*HashAddrMap, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if err := s.readPrep(ctx); err != nil {
			return nil, err
		}
		return s.metaStore.GetHashAddrMap(ctx, hashes)
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if err := s.readPrep(ctx); err != nil {
			return nil, err
		}
		return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if s.checkIsCrash() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.checkIsLeader() {
			return nil, ERR_NOT_LEADER
		}
		//2 phase commit
		s.log = append(s.log, &UpdateOperation{
			Term:         s.term,
			FileMetaData: filemeta,
		})
		if err := s.getResponse(ctx); err != nil {
			return nil, err
		}
		s.commitIndex = int64(len(s.log)) - 1
		go s.getResponse(context.Background())
		return s.runStateMachine(ctx)
	}
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		ans := &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: int64(len(s.log) - 1),
		}
		if s.checkIsCrash() {
			return ans, ERR_SERVER_CRASHED
		}
		// 1. Reply false if term < currentTerm (§5.1)
		if input.GetTerm() < s.term {
			return ans, nil
		} else if input.GetTerm() > s.term {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.term = input.GetTerm()
			s.isLeaderMutex.Unlock()
		}
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		// matches prevLogTerm (§5.3)
		prevLogIndex := input.GetPrevLogIndex()
		if prevLogIndex >= int64(len(s.log)) {
			return ans, nil
		}
		// 3. If an existing entry conflicts with a new one (same index but different
		// terms), delete the existing entry and all that follow it (§5.3)
		if s.log[prevLogIndex].GetTerm() != input.GetPrevLogTerm() {
			s.log = s.log[:prevLogIndex]
			if s.lastApplied >= prevLogIndex {
				s.metaStore.mu.Lock()
				for k := range s.metaStore.FileMetaMap {
					delete(s.metaStore.FileMetaMap, k)
				}
				s.metaStore.mu.Unlock()
			}
		}
		// 4. Append any new entries not already in the log
		s.log = append(s.log, input.GetEntries()...)
		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
		// of last new entry)
		leaderCommit := input.GetLeaderCommit()
		if leaderCommit > s.commitIndex {
			if leaderCommit < int64(len(s.log)-1) {
				s.commitIndex = leaderCommit
			} else {
				s.commitIndex = int64(len(s.log) - 1)
			}
		}
		ans.MatchedIndex = int64(len(s.log) - 1)
		if _, err := s.runStateMachine(ctx); err != nil {
			return ans, nil
		}
		ans.Success = true
		return ans, nil
	}
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if s.checkIsCrash() {
			return &Success{Flag: false}, ERR_SERVER_CRASHED
		}
		if s.checkIsLeader() {
			return &Success{Flag: true}, nil
		}
		s.isLeaderMutex.Lock()
		s.isLeader = true
		s.term += 1
		s.matchIndex = make([]int64, len(s.raftAddrs))
		s.nextIndex = make([]int64, len(s.raftAddrs))
		for i := range s.nextIndex {
			s.nextIndex[i] = int64(len(s.log))
		}
		s.isLeaderMutex.Unlock()
		return &Success{Flag: true}, nil
	}
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if s.checkIsCrash() {
			return &Success{Flag: false}, ERR_SERVER_CRASHED
		}

		if !s.checkIsLeader() {
			return &Success{Flag: false}, ERR_NOT_LEADER
		}
		if err := s.getResponse(ctx); err != nil {
			return &Success{Flag: false}, err
		}
		return &Success{Flag: true}, nil
	}
}

func (s *RaftSurfstore) getResponse(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		child_ctx, cancel := context.WithTimeout(ctx, TIMEOUT)
		defer cancel()
		ch := make(chan error, len(s.raftAddrs)-1)
		for i := range s.raftAddrs {
			if int64(i) != s.id {
				go s.replicateLogs(child_ctx, i, ch)
			}
		}
		cnt := 1
		for i := 0; i < len(s.raftAddrs)-1; i++ {
			if err := <-ch; err == nil {
				cnt += 1
			} else if err == ERR_LARGER_TERM {
				return ERR_LARGER_TERM
			}
		}
		if cnt <= len(s.raftAddrs)/2 {
			return errors.New(fmt.Sprintf("Only get %d response from %d servers", cnt, len(s.raftAddrs)))
		}
		return nil
	}
}

func (s *RaftSurfstore) replicateLogs(ctx context.Context, id int, ch chan error) {
	select {
	case <-ctx.Done():
		ch <- ctx.Err()
		return
	default:
		// connect to the server
		conn, err := grpc.DialContext(ctx, s.raftAddrs[id], grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			ch <- nil
			return
		}
		c := NewRaftSurfstoreClient(conn)

		for {
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: s.nextIndex[id] - 1,
				PrevLogTerm:  s.log[s.nextIndex[id]-1].GetTerm(),
				Entries:      s.log[s.nextIndex[id]:],
				LeaderCommit: s.commitIndex,
			}
			// perform the call
			res, err := c.AppendEntries(ctx, input)
			if err != nil {
				ch <- err
				return
			}
			//Larger term
			if res.GetTerm() > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.term = res.GetTerm()
				s.isLeaderMutex.Unlock()
				ch <- ERR_LARGER_TERM
				return
			}
			s.matchIndex[id] = res.GetMatchedIndex()
			s.nextIndex[id] = s.matchIndex[id] + 1
			if res.GetSuccess() {
				ch <- nil
				return
			}
		}
	}
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
