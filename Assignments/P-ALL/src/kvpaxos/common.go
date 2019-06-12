package kvpaxos

import "time"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

// clients should wait RetryInterval time before
// sending another request
const RetryInterval = time.Millisecond * 100

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	// You'll have to add definitions here.
	CurrId int64   //  ID of current client request
	PrevId int64   //  ID of previous client request already served

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key    string

	// You'll have to add definitions here.
	CurrId int64   //  ID of current client request
	PrevId int64   //  ID of previous client request already served
}

type GetReply struct {
	Err   Err
	Value string
}
