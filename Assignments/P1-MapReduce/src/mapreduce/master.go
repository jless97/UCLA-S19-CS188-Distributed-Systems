package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


func AssignJobsToWorkers(mr *MapReduce, doneChannel chan int, job JobType, nJobs int, nJobsOther int) {
	//  Process all jobs of type Map or Reduce
	for i := 0; i < nJobs; i++ {
		//  Hand out work to the workers via multithreading
		go func(jobNumber int) {
			//  Loop until the current job is completed by some worker
			for {
				//  Wait for a free worker to schedule job
				//  NOTE: if a worker fails, this implicitly reassigns the current job to a new worker
				worker := <-mr.registerChannel
				//  Build RPC message to send to worker
				args := DoJobArgs{mr.file, job, jobNumber, nJobsOther}
				var reply DoJobReply
				//  Send RPC message to worker
				ok := call(worker, "Worker.DoJob", args, &reply)
				//  Job completed, so update doneChannel and notify master the worker completed the current job
				if ok == true {
					doneChannel <-1
					mr.registerChannel <-worker
					break
				}
			}
		}(i)
	}
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	//  Init map and reduce buffered channels
	mapDoneChannel, reduceDoneChannel := make(chan int, mr.nMap), make(chan int, mr.nReduce)

	//  Do map jobs
	AssignJobsToWorkers(mr, mapDoneChannel, Map, mr.nMap, mr.nReduce)
	//  Await completion of map jobs
	for i := 0; i < mr.nMap; i++ {
		<-mapDoneChannel
	}
	//  Do reduce jobs
	AssignJobsToWorkers(mr, reduceDoneChannel, Reduce, mr.nReduce, mr.nMap)
	//  Await completion of reduce jobs
	for i := 0; i < mr.nReduce; i++ {
		<-reduceDoneChannel
	}

	//  Clean up all workers
	return mr.KillWorkers()
}
