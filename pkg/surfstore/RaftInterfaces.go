package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftInterface interface {
	AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error)
	SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error)
	RequestVote(ctx context.Context, input *RequestVoteInput) (*RequestVoteOutput, error)
}

type RaftTestingInterface interface {
	GetInternalState(ctx context.Context, _ *emptypb.Empty) (*RaftInternalState, error)
}

type RaftSurfstoreInterface interface {
	MetaStoreInterface
	RaftInterface
	RaftTestingInterface
}
