package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Key           string
	Value         string
	OperationType string  //  Get, Put, Append, or Reconfigure
	CurrId        int64   //  Processing the current client request
	PrevId        int64   //  Cleaning up client requests previously served
	ConfigNum     int     //  Configure number used for updating databases
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config          shardmaster.Config           //  keep track of the current replica group/shard configuration
	currSeq         int                          //  keeps track of current sequence instance
	database        map[string]string            //  keeps track of the current k/v database
	prevRequests    map[int64]string             //  keeps track of requests sent by the client
	databasePerView map[int](map[string]string)  //  keeps track of k/v database for a specified configuration
}


//
// Run Paxos in this instance until we have gained consensus
//
func (kv *ShardKV) RunPaxos(seq int, v Op) Op {
	kv.px.Start(seq, v)

	to := 10 * time.Millisecond
	for {
		operationStatus, operationValue := kv.px.Status(seq)
		if operationStatus == paxos.Decided {
			return operationValue.(Op)
		}

		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}


//
// Clean up client requests that have previously been served
//
func (kv *ShardKV) FreePrevRequest(prevId int64) {
	if prevId != -1 {
		_, ok := kv.prevRequests[prevId]
		if ok {
			delete(kv.prevRequests, prevId)
		}
	}
}


//
// Apply Get Op
//
func (kv *ShardKV) ApplyGet(key string, value string, currId int64) {
	prev, ok := kv.database[key]
	if ok {
		kv.prevRequests[currId] = prev
	} else {
		kv.prevRequests[currId] = ErrNoKey
	}
}


//
// Apply Put Op
//
func (kv *ShardKV) ApplyPut(key string, value string, currId int64) {
	kv.database[key] = value
	kv.prevRequests[currId] = OK
}


//
// Apply Append Op
//
func (kv *ShardKV) ApplyAppend(key string, value string, currId int64) {
	prev, ok := kv.database[key]
	if ok {
		kv.database[key] = prev + value
	} else {
		kv.database[key] = value
	}
	kv.prevRequests[currId] = OK
}


//
// UpdateDatabase RPC Handler
// Update current server's DB with shard contents from other servers
// for the specified viewnum
//
func (kv *ShardKV) UpdateDatabase(args *UpdateDatabaseArgs, reply *UpdateDatabaseReply) error {
	database, ok := kv.databasePerView[args.ConfigNum]
	if ok {
		reply.Database = make(map[string]string)
		for k, v := range database {
			reply.Database[k] = v
		}
		reply.Err = OK
	}

	return nil
}


//
// Apply Reconfigure Op
// Current server's configuration is behind, so need to update current server's
// database to reflect the latest configuration
//
func (kv *ShardKV) ApplyReconfigure(latestConfigNum int) {
	//  Update current server's DB until it reflects the latest configuration
	for kv.config.Num < latestConfigNum {
		if kv.config.Num == 0 {
			kv.config = kv.sm.Query(1)
			continue
		}

		//  Save what the current server's DB looked like for the current configuration
		currDatabase := make(map[string]string)
		for k, v := range kv.database {
			shard := key2shard(k)
			if kv.config.Shards[shard] == kv.gid {
				currDatabase[k] = v
			}
		}
		kv.databasePerView[kv.config.Num] = currDatabase

		//  Update the current server's DB one view at a time
		currConfig := kv.config
		nextConfig := kv.sm.Query(kv.config.Num + 1)
		for shard, currGroup := range currConfig.Shards {
			nextGroup := nextConfig.Shards[shard]
			//  If a shard being served by some other replica group is now being served by me, retrieve the data
			if currGroup != nextGroup && nextGroup == kv.gid {
				done := false
				//  Update DB from one of the servers in the other replica group (retry in the face of network partitions)
				for !done {
					for _, server := range currConfig.Groups[currGroup] {
						//  Prepare the arguments
						args := &UpdateDatabaseArgs{currConfig.Num}
						var reply UpdateDatabaseReply

						//  Make RPC call to retrieve data from the other servers
						ok := call(server, "ShardKV.UpdateDatabase", args, &reply)
						if ok && reply.Err == OK {
							//  Update database, done for next configuration
							for k, v := range reply.Database {
								kv.database[k] = v
							}

							done = true
							break
						}
					}
				}
			}
		}

		//  Move on to next view
		kv.config = nextConfig
	}
}


//
// The Paxos replicas have agreed on the order to apply this client request, so update the k/v database
//
func (kv *ShardKV) ApplyOperation(op Op) {
	key, value, currId, operation, configNum := op.Key, op.Value, op.CurrId, op.OperationType, op.ConfigNum
	switch operation {
	case "Get":
		kv.ApplyGet(key, value, currId)
	case "Put":
		kv.ApplyPut(key, value, currId)
	case "Append":
		kv.ApplyAppend(key, value, currId)
	case "Reconfigure":
		kv.ApplyReconfigure(configNum)
	}
}


//
// Run Paxos for specified operation, and then apply the operation
//
func (kv *ShardKV) Run(operation Op, getReply *GetReply, putAppendReply *PutAppendReply) {
	for {
		kv.currSeq++
		//  Gain consensus on this Paxos instance
		var operationResult Op
		operationStatus, operationValue := kv.px.Status(kv.currSeq)
		//  We are decided on this instance
		if operationStatus == paxos.Decided {
			operationResult = operationValue.(Op)
		//  We aren't decided on this instance, run Paxos until we are
		} else {
			operationResult = kv.RunPaxos(kv.currSeq, operation)
		}

		//  Clean up client requests that have previously been served
		kv.FreePrevRequest(operation.PrevId)
		//  We have agreed on this instance, so apply the operation
		kv.ApplyOperation(operationResult)
		//  We are done processing this instance, and will no longer need it or any previous instance
		kv.px.Done(kv.currSeq)

		//  Paxos elected the current operation, done
		if operationResult.CurrId == operation.CurrId {
			val := kv.prevRequests[operation.CurrId]
			if val == ErrNoKey {
				getReply.Value = ""
				getReply.Err = ErrNoKey
			} else {
				getReply.Value = kv.prevRequests[operation.CurrId]
				getReply.Err = OK
			}

			putAppendReply.Err = OK

			break
		}
	}
}


//
// Checks to see if this is a duplicate GET request to ensure at-most-once semantics
//
func (kv *ShardKV) IsDupGet(args *GetArgs) bool {
	key, currId := args.Key, args.CurrId
	prev, ok := kv.prevRequests[currId]

	//  Duplicate RPC request
	if ok && prev == key {
		return true
	}

	return false
}


//
// GET Request Handler
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if this server is responsible for this key's shard
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	//  Check if we have seen this request before, and if so, return stored value
	if kv.IsDupGet(args) {
		reply.Value = kv.database[args.Key]
		reply.Err = OK
		return nil
	}

	//  Prepare Paxos value
	operation := Op{Key: args.Key, OperationType: "Get", CurrId: args.CurrId, PrevId: args.PrevId}
	kv.Run(operation, reply, &PutAppendReply{})

	return nil
}


//
// Checks to see if this is a duplicate PUT/APPEND request to ensure at-most-once semantics
//
func (kv *ShardKV) IsDupPutAppend(args *PutAppendArgs) bool {
	_, ok := kv.prevRequests[args.CurrId]

	//  Duplicate RPC request
	if ok {
		return true
	}

	return false
}


//
// PUT/APPEND Request Handler
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if this server is responsible for this key's shard
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}

	//  Check if we have seen this request before
	if kv.IsDupPutAppend(args) {
		reply.Err = OK
		return nil
	}

	//  Prepare Paxos value
	operation := Op{Key: args.Key, Value: args.Value, OperationType: args.Op, CurrId: args.CurrId, PrevId: args.PrevId}
	kv.Run(operation, &GetReply{}, reply)

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//  Check if the configuration this server has is up-to-date
	latestConfigNum := (kv.sm.Query(-1)).Num
	if kv.config.Num != latestConfigNum {
		//  Prepare Paxos value
		operation := Op{OperationType: "Reconfigure", ConfigNum: latestConfigNum}
		kv.Run(operation, &GetReply{}, &PutAppendReply{})
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = kv.sm.Query(-1)
	kv.currSeq = 0
	kv.database = make(map[string]string)
	kv.prevRequests = make(map[int64]string)
	kv.databasePerView = make(map[int](map[string]string))

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
