package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	Leader = iota
	Candidate
	Follower
)

// TODO Add fields you need here
type RaftSurfstore struct {
	state         int64
	isLeaderMutex *sync.RWMutex
	term          int64
	voteFor       int64
	log           []*UpdateOperation
	id            int64
	raftAddrs     []string

	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64

	metaStore *MetaStore

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) checkIsLeader() bool {
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	if s.state == Leader {
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
		if !s.checkIsLeader() {
			return ERR_NOT_LEADER
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
		if !s.checkIsLeader() {
			return nil, ERR_NOT_LEADER
		}
		s.log = append(s.log, &UpdateOperation{
			Term:         s.term,
			FileMetaData: filemeta,
		})
		//2 phase commit begin
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
		// 1. Reply false if term < currentTerm (§5.1)
		if input.GetTerm() < s.term {
			return ans, nil
		} else if input.GetTerm() > s.term {
			s.isLeaderMutex.Lock()
			s.state = Follower
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
		s.log = s.log[:prevLogIndex+1]
		if s.log[prevLogIndex].GetTerm() != input.GetPrevLogTerm() {
			s.log = s.log[:prevLogIndex]
		}
		if s.lastApplied >= int64(len(s.log)) {
			s.metaStore.mu.Lock()
			for k := range s.metaStore.FileMetaMap {
				delete(s.metaStore.FileMetaMap, k)
			}
			s.metaStore.mu.Unlock()
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

func (s *RaftSurfstore) RequestVote(ctx context.Context, input *RequestVoteInput) (*RequestVoteOutput, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		ans := &RequestVoteOutput{
			ServerId:    s.id,
			Term:        s.term,
			VoteGranted: s.term > input.GetTerm(),
		}
		return ans, nil
	}
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if !s.checkIsLeader() {
			return &Success{Flag: false}, ERR_NOT_LEADER
		}
		return &Success{Flag: true}, nil
	}
}

func (s *RaftSurfstore) run(ctx context.Context, empty *emptypb.Empty) error {
	return nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		State:   s.state,
		Term:    s.term,
		Log:     s.log[1:],
		MetaMap: fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
