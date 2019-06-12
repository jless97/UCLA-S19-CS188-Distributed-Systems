package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)


// Helper function for GetMinMaxGroups function (INT_MIN and INT_MAX)
const UintSize = 32 << (^uint(0) >> 32 & 1) 
const (
	MaxInt = 1 << (UintSize - 1) - 1
	MinInt = -MaxInt - 1
)

//  Operation Opcodes
const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)


type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config //  indexed by config num
	currSeq int      //  keeps track of current sequence instance for Paxos agreement
}

type Op struct {
	// Your data here.
	OperationType string      //  Operation type (i.e. Join, Leave, Move, Query)
	OperationId   int64       //  Unique ID (for Paxos)
	GroupId       int64       //  Replica group ID (for Join, Leave, Move)
	Servers       []string    //  Array of server ports (for Join)
	ShardNum      int         //  Shard number (for Move)
	ConfigNum     int         //  Configuration number (for Query)
}

type MinMax struct {
	MinGroup  int64  //  Replica group with the min number of shards
	MaxGroup  int64  //  Replica group with the max number of shards
	MinShards int    //  Min number of shards being served by a replica group
	MaxShards int    //  Max number of shards being served by a replica group
}


//
// Used for generating a unique sequence number (for Paxos)
//
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}


//
// Initialize new configuration for new assignment
//
func (sm *ShardMaster) InitNextConfig() *Config {
	//  Get the current configuration
	currConfig := &sm.configs[len(sm.configs) - 1]

	//  Initialize the next configuration (using the current configuration)
	nextConfig := Config{}
	nextConfig.Num = currConfig.Num + 1
	for shard, group := range currConfig.Shards {
		nextConfig.Shards[shard] = group
	}
	nextConfig.Groups = make(map[int64][]string, len(currConfig.Groups))
	for group, servers := range currConfig.Groups {
		nextConfig.Groups[group] = servers
	}

	return &nextConfig
}


//
// Find the replica groups serving the min and max number of shards.
// Used for load rebalancing
//
func (sm *ShardMaster) GetMinMaxGroups(config *Config) MinMax {
	rs := MinMax{}
	rs.MinShards, rs.MaxShards = MaxInt, MinInt

	//  Init shard count for each replica group
	counts := make(map[int64]int)
	for group, _ := range config.Groups {
		counts[group] = 0
	}

	//  Get shard count for each replica group
	for _, group := range config.Shards {
		_, ok := config.Groups[group]
		if ok {
			counts[group]++
		}
	}

	//  Find the replica groups with the min and max number of shards
	for group, count := range counts {
		//  Update min group
		if count < rs.MinShards {
			rs.MinShards = count
			rs.MinGroup = group
		}

		//  Update max group
		if count > rs.MaxShards {
			rs.MaxShards = count
			rs.MaxGroup = group
		}
	}

	return rs
}


//
// Assign the replica groups serving the min number of shards to shards
// currently being unserved (i.e. group == 0, group groupId just left)
//
func (sm *ShardMaster) AssignUnservedShards(config *Config, groupId int64) {
	for shard, group := range config.Shards {
		if group == 0 || group == groupId {
			//  Get curr group with the min num of shards
			curr := sm.GetMinMaxGroups(config)
			//  Assign shard to replica group serving min number of shards
			config.Shards[shard] = curr.MinGroup
		}
	}
}


//
// Rebalance shard load in response to a new assignment
//
func (sm *ShardMaster) RebalanceLoad(config *Config, groupId int64) {
	//  Assign shards not currently being served (i.e. group == 0, group groupId just left)
	sm.AssignUnservedShards(config, groupId)

	//  Loop until load is balanced evenly
	for true {
		//  Get curr groups with the min and max num of shards
		curr := sm.GetMinMaxGroups(config)

		//  We are now balanced, so done
		if curr.MaxShards - curr.MinShards <= 1 {
			break
		}

		//  Perform rebalancing
		for shard, group := range config.Shards {
			//  Get curr groups with the min and max num of shards
			curr = sm.GetMinMaxGroups(config)

			//  Allocate shards from max to min replica group
			if group == curr.MaxGroup {
				config.Shards[shard] = curr.MinGroup
			}
		}
	}
}


//
// Apply Join Op
//
func (sm *ShardMaster) ApplyJoin(groupId int64, servers []string) {
	//  Initialize next configuration
	nextConfig := sm.InitNextConfig()
	//  Create new configuration that includes the new replica group
	nextConfig.Groups[groupId] = servers
	//  Rebalance load amongst replica groups
	sm.RebalanceLoad(nextConfig, groupId)
	//  Add new configuration to sequence of configurations
	sm.configs = append(sm.configs, *nextConfig)
}


//
// Apply Leave Op
//
func (sm *ShardMaster) ApplyLeave(groupId int64) {
	//  Initialize next configuration
	nextConfig := sm.InitNextConfig()
	//  Create new configuration that removes specified replica group
	delete(nextConfig.Groups, groupId)
	//  Rebalance load amongst replica groups
	sm.RebalanceLoad(nextConfig, groupId)
	//  Add new configuration to sequence of configurations
	sm.configs = append(sm.configs, *nextConfig)
}


//
// Apply Move Op
//
func (sm *ShardMaster) ApplyMove(groupId int64, shardNum int) {
	//  Initialize next configuration
	nextConfig := sm.InitNextConfig()
	//  Create new configuration that reassigns this shard to specified replica group
	nextConfig.Shards[shardNum] = groupId
	//  Add new configuration to sequence of configurations
	sm.configs = append(sm.configs, *nextConfig)
}


//
// Apply Query Op
//
func (sm *ShardMaster) ApplyQuery(configNum int) Config {
	//  Unknown configuration number, return highest known
	if configNum == -1 || configNum > len(sm.configs) - 1 {
		return sm.configs[len(sm.configs) - 1]
	//  Return specified configuration number
	} else {
		return sm.configs[configNum]
	}
}


//
// The Paxos replicas have agreed on the order to apply this client request, so apply the op
//
func (sm *ShardMaster) ApplyOperation(operation Op) Config {
	switch operation.OperationType {
	case Join:
		sm.ApplyJoin(operation.GroupId, operation.Servers)
	case Leave:
		sm.ApplyLeave(operation.GroupId)
	case Move:
		sm.ApplyMove(operation.GroupId, operation.ShardNum)
	case Query:
		return sm.ApplyQuery(operation.ConfigNum)
	}

	return Config{}
}


//
// Run Paxos in this instance until we have gained consensus
//
func (sm *ShardMaster) RunPaxos(seq int, v Op) Op {
	sm.px.Start(seq, v)

	to := 10 * time.Millisecond
	for {
		operationStatus, operationValue := sm.px.Status(seq)
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
// Run Paxos for specified operation, and then apply the operation
//
func (sm *ShardMaster) Run(operation Op) Config {
	for {
		sm.currSeq++

		//  Gain consensus on this Paxos instance
		var operationResult Op
		operationStatus, operationValue := sm.px.Status(sm.currSeq)
		//  We are decided on this instance
		if operationStatus == paxos.Decided {
			operationResult = operationValue.(Op)
		//  We aren't decided on this instance, run Paxos until we are
		} else {
			operationResult = sm.RunPaxos(sm.currSeq, operation)
		}

		//  We have agreed on this instance, so apply the operation
		config := sm.ApplyOperation(operationResult)
		//  We are done processing this instance, and will no longer need it or any previous instance
		sm.px.Done(sm.currSeq)

		//  Paxos elected the current operation, done
		if operationResult.OperationId == operation.OperationId {
			return config
		}
	}
}


//
// Join RPC Handler
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	//  Prepare Paxos value
	operation := Op{OperationType: Join, OperationId: nrand(), GroupId: args.GID, Servers: args.Servers}
	//  Add specified replica group to next configuration
	sm.Run(operation)

	return nil
}


//
// Leave RPC Handler
//
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	//  Prepare Paxos value
	operation := Op{OperationType: Leave, OperationId: nrand(), GroupId: args.GID}
	//  Remove specified replica group from next configuration
	sm.Run(operation)

	return nil
}


//
// Move RPC Handler
//
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	//  Prepare Paxos value
	operation := Op{OperationType: Move, OperationId: nrand(), GroupId: args.GID, ShardNum: args.Shard}
	//  Reassign shard to specified replica group
	sm.Run(operation)

	return nil
}


//
// Query RPC Handler
//
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	//  Prepare Paxos value
	operation := Op{OperationType: Query, OperationId: nrand(), ConfigNum: args.Num}
	//  Retrieve configuration specified by query
	reply.Config = sm.Run(operation)

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	// Your initialization code here.
	sm.currSeq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
