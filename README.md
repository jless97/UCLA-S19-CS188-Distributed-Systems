# CS188, Distributed Systems, Spring 2019 

## Course Objective
This class teaches design and implementation techniques that enable the building of fast, scalable, fault-tolerant distributed systems.

## Course Description
Distributed systems enable developers to leverage computing and storage resources across many networked computers, to supply services that are fast, highly available, and scalable. Study covers fundamental concepts of design and implementation of such distributed systems. Topics include communication models for distributed machines (e.g., sockets, RPCs); synchronization (e.g., clock synchronization, logical clocks, vector clocks); failure recovery (e.g., snapshotting, primary-backup); consistency models (e.g., linearizability, eventual, causal); consensus protocols (e.g., Paxos, Raft); distributed transactions; and locking. Students gain hands-on, practical experience with these topics through multiple programming assignments, which work through steps of creating fault-tolerant, sharded key/value store. Exploration of how these concepts have manifested in several real-world, large-scale distributed systems used by Internet companies like Google, Facebook, and Amazon.

### Languages Covered 
Programming Language: Go

### Assignment Overviews
* P1 - MapReduce
  * Built a MapReduce library as a way to learn the Go programming language and as a way to learn about fault tolerance in distributed systems.
    * Part I: Word Count - wrote a simple MapReduce program
    * Part II: Distributed MapReduce jobs - wrote a Master that hands out jobs to workers
    * Part III: Handling worker failures - modified the Master to handle failure of workers
* P2 - Primary/Backup Key/Value Service
  * Built a key/value service using primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure
    * Part A: The Viewservice 
    * Part B: The primary/backup key/value service
* P3 - Paxos-Based Key/Value Service
  * Built a key/value service using the Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant
    * Part A: Paxos
    * Part B: Paxos-based Key/Value Server
* P4 - TBA
