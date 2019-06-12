package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Err string
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const (
	OK            = "OK"
	ErrConnection = "ErrConnection"
	ErrRejected   = "ErrRejected"
)

type SequenceInfo struct {
	nPromised int64        //  Highest proposal number promised to accept
	nAccepted int64        //  Highest proposal number accepted
	vAccepted interface{}  //  Value accepted
	isDecided bool         //  Did we already decide on this sequence number
}

type Paxos struct {
	mu                  sync.Mutex
	l                   net.Listener
	dead                int32 // for testing
	unreliable          int32 // for testing
	rpcCount            int32 // for testing
	peers               []string
	me                  int // index into peers[]

	// Your data here.
	majorityCount       int                    //  Value constituting a majority vote
	sequenceLog         map[int]*SequenceInfo  //  Keeps track of agreement state for each instance sequence
	peerMinDoneSequence []int                  //  Keeps track of each peer's min sequence instance they are done with
}


//
// RPC argument/reply types for the Paxos protocol messages
//
type PrepareArgs struct {
	Seq       int    //  Sequence number
	NProposed int64  //  Current proposal number
}

type PrepareReply struct {
	Err       Err
	NPromised int64        //  Highest proposal number promised to accept
	NAccepted int64        //  Highest proposal number accepted
	VAccepted interface{}  //  Value accepted
}

type AcceptArgs struct {
	Seq       int          //  Sequence number
	NToAccept int64        //  Number we are agreeing to accept
	VToAccept interface{}  //  Value we are agreeing to accept
}

type AcceptReply struct {
	Err       Err
}

type LearnArgs struct {
	Seq        int         //  Sequence number
	VAccepted interface{}  //  Value we decided to accept
}

type LearnReply struct {
	Err Err
}

type DoneArgs struct {
	Seq    int  //  Min sequence number that we are done with
	Sender int  //  Peer ID of the peer sending the Done RPC
}

type DoneReply struct {
	Err Err
}


//
// Prepare RPC handler to handle the Prepare Phase of the Paxos Protocol
//
// Proposer proposes a certain value to its paxos peers in order to gain
// a majority commitment
//
func (px *Paxos) PrepareHandler(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//  Check if we have seen this sequence instance before
	seq, nProposed := args.Seq, args.NProposed
	sequenceInfo, ok := px.sequenceLog[seq]

	//  Have we seen this sequence instance before?
	//  Yes => Did we promise to ignore requests with this ID:nProposed?
	if ok {
		//  No => so promise to ignore any request lower than ID:nProposed
		if nProposed > sequenceInfo.nPromised {
			//  Update corresponding sequence info in log
			sequenceInfo.nPromised = nProposed

			//  Construct reply with <promise, nProposed, nAccepted, vAccepted>
			reply.NPromised = nProposed
			reply.NAccepted = sequenceInfo.nAccepted
			reply.VAccepted = sequenceInfo.vAccepted
		//  Yes => so ignore this request
		} else {
			reply.Err = ErrRejected
			return nil
		}
	//  No => no prior knowledge of this sequence instance, so add to log, and reply with <promise, nProposed, nil>
	} else {
		//  Add to log
		px.sequenceLog[seq] = &SequenceInfo{nProposed, -1, nil, false}

		//  Construct reply
		reply.NPromised = nProposed
		reply.NAccepted = -1
		reply.VAccepted = nil
	}

	reply.Err = OK
	return nil
}


//
// Accept RPC handler to handle the Accept Phase of the Paxos Protocol
//
// After gaining a majority of acceptors to promise in the Prepare Phase,
// proposer sends Accept-Request messages to peers to gain consensus
//
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//  Check if we have seen this sequence instance before
	seq, nToAccept, vToAccept := args.Seq, args.NToAccept, args.VToAccept
	sequenceInfo, ok := px.sequenceLog[seq]

	//  Have we seen this sequence instance before?
	//  Yes => Did we promise to ignore requests with this ID:nToAccept?
	if ok {
		//  No => so accept
		if nToAccept >= px.sequenceLog[seq].nPromised {
			//  Update corresponding sequence info in log
			sequenceInfo.nPromised = nToAccept
			sequenceInfo.nAccepted = nToAccept
			sequenceInfo.vAccepted = vToAccept
		//  Yes => so ignore this request
		} else {
			reply.Err = ErrRejected
			return nil
		}
	//  No => no prior knowledge of this seqeuence instance, so add to log
	} else {
		//  Add to log
		px.sequenceLog[seq] = &SequenceInfo{nToAccept, nToAccept, vToAccept, false}
	}

	reply.Err = OK
	return nil
}


//
// Learn RPC Handler to handle the Learn Phase of the Paxos Protocol
//
// Informs peers about the value that we agreed to accept
// for instance seq
//
func (px *Paxos) LearnHandler(args *LearnArgs, reply *LearnReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	//  Check if we have seen this sequence instance before
	seq, vAccepted := args.Seq, args.VAccepted
	sequenceInfo, ok := px.sequenceLog[seq]

	//  Have we seen this sequence instance before?
	//  Yes => so update the accepted value associated with this sequence number
	if ok {
		sequenceInfo.vAccepted = vAccepted
		sequenceInfo.isDecided = true
	//  No => no prior knowledge of this sequence instance, so add to log
	} else {
		px.sequenceLog[seq] = &SequenceInfo{-1, -1, vAccepted, true}
	}

	reply.Err = OK
	return nil
}


//
// Done RPC Handler to handle the Forgetting Phase of the Paxos Protocol
//
// Informs peers about the new minimum done value from the sending peer and
// tries to delete sequence instances given this new information
//
func (px *Paxos) DoneHandler(args *DoneArgs, reply *DoneReply) error {
	seq, sender := args.Seq, args.Sender

	//  Higher done value for the sender peer
	if seq > px.peerMinDoneSequence[sender] {
		px.mu.Lock()

		//  Update sender peer's minimum done value
		px.peerMinDoneSequence[sender] = seq

		//  With this new information, see if we can delete any sequence instances for curr peer
		minSeq := px.Min()
		for i := 0; i < minSeq; i++ {
			sequenceInfo, ok := px.sequenceLog[i]
			if ok {
				if sequenceInfo.isDecided {
					delete(px.sequenceLog, i)
				}
			}
		}

		px.mu.Unlock()
	}

	reply.Err = OK
	return nil
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


//
//  Start Helper function that generates unique proposal IDs to send
//  to acceptors while trying to gain consensus on an agreement
//  for instance seq
//
func GenerateUniqueProposalID(value int32, peerId int32) int64 {
	var uniqueProposalId int64 = ((int64(value) << 32) | int64(peerId))
	return uniqueProposalId
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go func(seq int, value interface{}) {
		//  Create state in the sequence log regarding this sequence instance
		px.mu.Lock()
		_, ok := px.sequenceLog[seq]
		if !ok {
			px.sequenceLog[seq] = &SequenceInfo{-1, -1, nil, false}
		}
		px.mu.Unlock()

		//  Keep track if we decided on the agreement
		isDecided := false
		//  Keep track of highest number proposed thus far
		var highestNProposed int64 = -1
		//  Keep track of associated value we promised to accept of the highest number proposed
		vPromised := value

		//  While server is still alive, and we haven't decided on the agreement
		for uniqueProposalId := GenerateUniqueProposalID(int32(1), int32(px.me)); !px.isdead() && !isDecided; uniqueProposalId++ {
			//  Keep track of the number of peers that agree at the Prepare phase
			count := 0

			//  Prepare the Reply arguments
			args := &PrepareArgs{seq, uniqueProposalId}

			//  Phase 1 (Prepare): Send Prepare message to all peers
			for i, peer := range px.peers {
				var reply PrepareReply

				//  Send message to local acceptor (i.e. itself) for paxos peer acting as the proposer
				if i == px.me {
					px.PrepareHandler(args, &reply)
				//  Make RPC call to send message to paxos peers
				} else {
					ok := call(peer, "Paxos.PrepareHandler", args, &reply)
					if !ok {
						reply.Err = ErrConnection
						//log.Printf("Prepare Network Error\n")
					}
				}

				//  If acceptor promises to accept
				if reply.Err == OK {
					//  Update promised count
					count++

					//  If acceptor has previously accepted a value, update highest number proposed and associated value
					if reply.NAccepted > highestNProposed {
						highestNProposed = reply.NAccepted
						vPromised = reply.VAccepted
					}
				}
			}

			//  If majority of acceptors promise
			if count >= px.majorityCount {
				//  Reset, and keep track of the number of peers that agree at the Accept phase
				count = 0

				//  Prepare the Accept arguments
				args := &AcceptArgs{seq, uniqueProposalId, vPromised}

				//  Phase 2 (Accept): Send Accept message to all peers
				for i, peer := range px.peers {
					var reply AcceptReply

					//  Send message to local acceptor (i.e. itself) for paxos peer acting as the proposer
					if i == px.me {
						px.AcceptHandler(args, &reply)
					//  Make RPC call to send message to paxos peers
					} else {
						ok := call(peer, "Paxos.AcceptHandler", args, &reply)
						if !ok {
							reply.Err = ErrConnection
							//log.Printf("Accept Network Error\n")
						}
					}

					//  If acceptor does accept
					if reply.Err == OK {
						count++
					}
				}

				//  If majority of acceptors accept, consensus is reached
				if count >= px.majorityCount {
					//  Decided on agreement (break out of loop)
					isDecided = true

					//  Prepare the Learn arguments
					args := &LearnArgs{seq, vPromised}

					//  Phase 3 (Learn via broadcasting): Send Learn message to all peers
					for i, peer := range px.peers {
						var reply LearnReply

						//  Send message to local acceptor (i.e. itself) for paxos peer acting as the proposer
						if i == px.me {
							px.LearnHandler(args, &reply)
						//  Make RPC call to send message to paxos peers
						} else {
							ok := call(peer, "Paxos.LearnHandler", args, &reply)
							if !ok {
								reply.Err = ErrConnection
								//log.Printf("Learn Network Error\n")
							}
						}
					}
				//  Else, consensus is not reached, try another proposal round
				} else {
					//  Generate unique ID that is larger than the highest number proposed thus far
					if highestNProposed > uniqueProposalId {
						uniqueProposalId = GenerateUniqueProposalID(int32(highestNProposed), int32(px.me))
					}
				}
			}
		}
	}(seq, v)
}


//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.

	//  Higher done value for the curr peer
	if seq > px.peerMinDoneSequence[px.me] {
		//  Broadcast curr peer's new done value to the other peers
		args := &DoneArgs{seq, px.me}
		for i, peer := range px.peers {
			if i != px.me {
				var reply DoneReply
				ok := call(peer, "Paxos.DoneHandler", args, &reply)
				if !ok {
					reply.Err = ErrConnection
					//log.Printf("Done Network Error\n")
				}
			}
		}

		px.mu.Lock()

		//  Update curr peer's minimum done value
		px.peerMinDoneSequence[px.me] = seq

		//  With this new information, see if we can delete any sequence instances for curr peer
		minSeq := px.Min()
		for i := 0; i < minSeq; i++ {
			sequenceInfo, ok := px.sequenceLog[i]
			if ok {
				if sequenceInfo.isDecided {
					delete(px.sequenceLog, i)
				}
			}
		}

		px.mu.Unlock()
	}
}


//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.

	//  Get highest sequence instance
	highestSeq := -1
	for seq, _ := range px.sequenceLog {
		if seq > highestSeq {
			highestSeq = seq
		}
	}

	return highestSeq
}


//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.

	//  Get new minimum sequence instance
	minSeq := px.peerMinDoneSequence[0]
	for _, seq := range px.peerMinDoneSequence {
		if seq < minSeq {
			minSeq = seq
		}
	}

	return minSeq + 1
}


//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	//  We are done with this sequence instance
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	//  If we have seen this sequence instance before and have come to consensus on it
	sequenceInfo, ok := px.sequenceLog[seq]
	if ok && sequenceInfo.isDecided {
		return Decided, sequenceInfo.vAccepted
	}

	return Pending, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}


//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}


// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}


//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.majorityCount = (len(px.peers) / 2) + 1
	px.sequenceLog = make(map[int]*SequenceInfo)
	px.peerMinDoneSequence = make([]int, len(peers))
	for i := 0; i < len(px.peerMinDoneSequence); i++ {
		px.peerMinDoneSequence[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
