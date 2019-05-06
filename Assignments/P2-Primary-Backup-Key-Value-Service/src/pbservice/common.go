package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	// You'll have to add definitions here.
	Op    string
	Id    int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

// Get
type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	Id  int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

//  Forward entire database to backup
type FwdDatabaseToBackupArgs struct {
	Database     map[string]string
	PrevRequests map[int64]Pair
}

type FwdDatabaseToBackupReply struct {
	Err Err
}
