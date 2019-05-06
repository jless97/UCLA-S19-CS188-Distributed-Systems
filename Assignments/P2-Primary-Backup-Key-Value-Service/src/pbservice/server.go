package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type Pair struct {
	Key   string
	Value string
	Op    string
}

type PBServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	unreliable   int32 // for testing
	me           string
	vs           *viewservice.Clerk

	// Your declarations here.
	currView     viewservice.View  //  keeps track of current view
	database     map[string]string //  keeps track of the k/v database
	prevRequests map[int64]Pair    //  keeps track of requests sent by the client
	syncDatabase bool              //  keeps track of whether primary and backup DB's are in sync
}


//
// Process GET RPC requests from the client
// Primary should propagate updates to the backup server
//
func IsDupGet(pb *PBServer, args *GetArgs, reply *GetReply) bool {
	key, id := args.Key, args.Id
	prev, ok := pb.prevRequests[id]

	//  Duplicate RPC request
	if ok && prev.Key == key {
		return true
	}

	return false
}


func (pb *PBServer) ApplyGet(args *GetArgs, reply *GetReply) error {
	value, ok := pb.database[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	//  Update our prevRequests map
	pb.prevRequests[args.Id] = Pair{Key: args.Key, Value: "", Op: "Get"}

	return nil
}


func (pb *PBServer) FwdGetToBackup(args *GetArgs, reply *GetReply) error {
	//  Should only perform update on the backup server
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before, and if so, return previously calculated value
	if IsDupGet(pb, args, reply) {
		reply.Value = pb.database[args.Key]
		reply.Err = OK
		return nil
	}

	//  Update backup's database with Get operation
	pb.ApplyGet(args, reply)

	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//  If not the primary, send error back to client
	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before, and if so, return previously calculated value
	if IsDupGet(pb, args, reply) {
		reply.Value = pb.database[args.Key]
		reply.Err = OK
		return nil
	}

	//  Retrieve key from database (or return empty string if not present)
	pb.ApplyGet(args, reply)

	//  Propagate update to the backup server
	if pb.currView.Backup != "" {
		//  Send an RPC request, wait for the reply
		ok := call(pb.currView.Backup, "PBServer.FwdGetToBackup", args, &reply)
		//  If something went wrong (e.g. server crashed and update didn't go through)
		if !ok || reply.Err == ErrWrongServer || reply.Value != pb.database[args.Key] {
			pb.syncDatabase = true
		}
	}

	return nil
}


//
// Process Put/Append RPC requests from the client
// Primary should propagate updates to the backup server
//
func IsDupPutAppend(pb *PBServer, args *PutAppendArgs, reply *PutAppendReply) bool {
	key, value, op, id := args.Key, args.Value, args.Op, args.Id
	prev, ok := pb.prevRequests[id]

	//  Duplicate RPC request
	if ok && prev.Key == key && prev.Value == value && prev.Op == op {
		return true
	}

	return false
}


func (pb *PBServer) ApplyPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	key, value := args.Key, args.Value 
	if args.Op == "Put" {
		pb.database[key] = value
	} else if args.Op == "Append" {
		prev := pb.database[key]
		pb.database[key] = prev + value
	}

	//  Update prevRequests map
	pb.prevRequests[args.Id] = Pair{Key: args.Key, Value: args.Value, Op: args.Op}

	reply.Err = OK
	return nil
}


func (pb *PBServer) FwdPutAppendToBackup(args *PutAppendArgs, reply *PutAppendReply) error {
	//  Should only perform update on the backup server
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before
	if IsDupPutAppend(pb, args, reply) {
		reply.Err = OK
		return nil
	}

	//  Update backup's database with Put/Append operation
	pb.ApplyPutAppend(args, reply)

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//  If not the primary, send error back to client
	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before
	if IsDupPutAppend(pb, args, reply) {
		reply.Err = OK
		return nil
	}

	//  Update database
	pb.ApplyPutAppend(args, reply)

	//  Propagate update to the backup server
	if pb.currView.Backup != "" {
		//  Send an RPC request, wait for the reply
		ok := call(pb.currView.Backup, "PBServer.FwdPutAppendToBackup", args, &reply)
		//  If something went wrong (e.g. server crashed and update didn't go through), resync next tick
		if !ok || reply.Err != OK || pb.database[args.Key] != args.Value {
			pb.syncDatabase = true
		}
	}

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) FwdDatabaseToBackup(args *FwdDatabaseToBackupArgs, 
	reply *FwdDatabaseToBackupReply) error {
	//  Ping viewservice for current view
	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		fmt.Errorf("Ping(%v) failed", pb.currView.Viewnum)
	}

	//  Should only perform update on the backup server
	if newView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Update backup's database
	pb.database = args.Database
	pb.prevRequests = args.PrevRequests

	reply.Err = OK
	return nil
}


func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//  Ping viewservice for current view
	newView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		fmt.Errorf("Ping(%v) failed", pb.currView.Viewnum)
	}

	//  Backup added to view, primary should send it's complete k/v database
	if newView.Primary == pb.me && newView.Backup != "" && pb.currView.Backup != newView.Backup {
		pb.syncDatabase = true
	}

	//  Sync primary and backup databases (for complete DB transfer AND Get/Put/Append RPC requests)
	if pb.syncDatabase == true {
		pb.syncDatabase = false

		//  Prepare the arguments
		args := &FwdDatabaseToBackupArgs{Database: pb.database, PrevRequests: pb.prevRequests}
		var reply FwdDatabaseToBackupReply

		//  Send an RPC request, wait for the reply
		ok := call(newView.Backup, "PBServer.FwdDatabaseToBackup", args, &reply)
		//  If something went wrong, then sync up on next tick
		if !ok || reply.Err != OK {
			pb.syncDatabase = true
		}
	}

	//  Update current view
	pb.currView = newView
}


// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}


// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}


// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.database = make(map[string]string)
	pb.prevRequests = make(map[int64]Pair)
	pb.syncDatabase = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
