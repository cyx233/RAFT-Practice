package surfstore

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ERR_SERVER_CRASHED = status.Error(codes.Unavailable, "Server is crashed.")
var ERR_NOT_LEADER = status.Error(codes.Unavailable, "Server is not the leader.")
var ERR_LARGER_TERM = status.Error(codes.Aborted, "Found the Leader with a larger term")
var ERR_LEADER_NOT_FOUND = fmt.Errorf("Leader Not Found")
