package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"


type ViewServer struct {
	mu                   sync.Mutex
	l                    net.Listener
	dead                 int32 // for testing
	rpccount             int32 // for testing
	me                   string

	// Your declarations here.
	currView             View                  //  keeps track of current view
	pingTimeMap          map[string]time.Time  //  keeps track of most recent time VS heard ping from each server
	primaryAckedCurrView bool                  //  keeps track of whether primary has ACKed the current view
	idleServer           string                //  keeps track of any idle servers
}


//
//  Helper function to update view
//
func UpdateView(vs *ViewServer, primary string, backup string) {
	vs.currView.Viewnum += 1
	vs.currView.Primary = primary
	vs.currView.Backup = backup 
	vs.primaryAckedCurrView = false
	vs.idleServer = ""
}


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	kvServer, viewNum := args.Me, args.Viewnum

	//  Update ping times for current server
	vs.pingTimeMap[kvServer] = time.Now()

	//  Update view and/or idle server if reboot or new server, or ACK the current view
	if kvServer == vs.currView.Primary {  //  Pings from primary
		if viewNum == 0 && vs.primaryAckedCurrView == true {  //  Primary crashed
			UpdateView(vs, vs.currView.Backup, vs.idleServer)
		} else if viewNum == vs.currView.Viewnum {  //  Primary ACKs current view
			vs.primaryAckedCurrView = true
		}
	} else if kvServer == vs.currView.Backup {  //  Pings from backup
		if viewNum == 0 && vs.primaryAckedCurrView == true {  // Backup crashed
			UpdateView(vs, vs.currView.Primary, vs.idleServer)
		}
	} else {  //  Pings from other
		if vs.currView.Viewnum == 0 {  //  First time, so make primary
			UpdateView(vs, kvServer, "")
		} else {  //  Make the new idle server
			vs.idleServer = kvServer
		}
	}

	reply.View = vs.currView
	return nil
}


//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	currTime := time.Now()
	primaryTime := vs.pingTimeMap[vs.currView.Primary]
	backupTime := vs.pingTimeMap[vs.currView.Backup]
	idleServerTime := vs.pingTimeMap[vs.idleServer]
	pingWindow := PingInterval * DeadPings

	//  No recent pings from the idle server
	if currTime.Sub(idleServerTime) >= pingWindow {
		vs.idleServer = ""
	} else {  //  Look to add idle server to current view
		//  If the primary has ACKed the current view, we have no backup, and we have an idle, update view
		if vs.primaryAckedCurrView == true && vs.currView.Backup == "" && vs.idleServer != "" {
			UpdateView(vs, vs.currView.Primary, vs.idleServer)
		}
	}

	//  No recent pings from the backup
	if currTime.Sub(backupTime) >= pingWindow {
		//  If the primary has ACKed the current view and we have an idle server, update view
		if vs.primaryAckedCurrView == true && vs.idleServer != "" {
			UpdateView(vs, vs.currView.Primary, vs.idleServer)
		}
	}

	//  No recent pings from the primary
	if currTime.Sub(primaryTime) >= pingWindow {
		//  If the primary has ACKed the current view, update view
		if vs.primaryAckedCurrView == true {
			UpdateView(vs, vs.currView.Backup, vs.idleServer)
		}
	}
}


//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}


// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}


func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = make(map[string]time.Time)
	vs.primaryAckedCurrView = false
	vs.idleServer = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
