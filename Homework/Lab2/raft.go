package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

// import "math"

// import "reflect"
// import "fmt"

//import "log"
import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

// define const for state
type STATE int

const (
	LEADER STATE = iota
	CANDIDATE
	FOLLOWER

	HiberInterval = 100 * time.Millisecond
)

type ApplyMsg struct {
	Index       int
	Command     interface{} // collection of method signatures
	UseSnapshot bool        // ignore for lab2; only used in lab3
	Snapshot    []byte      // ignore for lab2; only used in lab3
}

type Log struct { // inside the log we need to store the logIndex, logTerm, and also the command
	LogIndex int
	LogTerm  int
	Command  interface{}
}

//
// A Go object implementing a single Raft peer.
//

/*
Hint:
Add any state you need to the Raft struct in raft.go. You'll also need to define a struct to hold information about each log entry. Your code should follow Figure 2 in the paper as closely as possible.
*/
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers this means server
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// store the persistent state on server:
	currentTerm int
	votedFor    int
	log         []Log

	// store the state
	state STATE
	// Your data here (2A, 2B, 2C).

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to committed (initialized to 0, increases monotonically)
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// count vote received
	voteCount int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	chanLeader         chan bool
	chanGrantVote      chan bool
	chanCommit         chan bool
	chanHeartbeat      chan bool
	chanHeartbeatreply chan bool
	chanApply          chan ApplyMsg
	// extra set ups
	lastMsgTime   int64
	currentLeader int
}

func (rf *Raft) setlastMsgTime(time int64) {
	rf.lastMsgTime = time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	// how to check if it's a leader
	// DPrintf("server number is %d\n", term)
	// DPrintf("State is %d", rf.state)
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

/*
Fill in the RequestVoteArgs and RequestVoteReply structs. Modify Make() to create a background goroutine that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while. This way a peer will learn who is the leader, if there is already leader, or become itself the leader. Implement the RequestVote() RPC handler so that servers will vote for one another.
*/
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int //candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote

	// Your data here (2A).
}

// AppendEntries RPC args and reply
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty heartbeat)
	LeaderCommit int   // leader's commit index
}

type AppendEntriesReply struct {
	Term      int  // currentTerm, for leader to update itself
	Success   bool // true if follwer contained entry matching prevLogIndex and prevLogTerm
	NextIndex int
}

//
// example RequestVote RPC handler.
//

/*
Implement the leader and follower code to append new log entries. This will involve implementing Start(), completing the AppendEntries RPC structs, sending them, fleshing out the AppendEntry RPC handler, and advancing the commitIndex at the leader. Your first goal should be to pass the TestBasicAgree() test (in test_test.go). Once you have that working, you should get all the 2B tests to pass (go test -run 2B).
*/

// write help function
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// args is for the candidate, reply is for term and voteGranted renovation and rf is the receipient's server
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	term := args.Term
	currentTerm := rf.currentTerm
	if term < currentTerm { // in this case, the follwer will reject to vote for this candidate
		reply.VoteGranted = false
		reply.Term = currentTerm
		// DPrintf("candidate=%d term = %d smaller than server = %d, currentTerm = %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return
	}

	if term > currentTerm { // if the args's term is larger than receiver's term, set receiver's term to it, and state is still follower, after that go check if the log is uptodate
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}
	reply.Term = rf.currentTerm
	// then we need to check if this follower already voted for others before and its log is up-to-date or not
	// check up-to-date
	isupdate := false
	// Raft determines which of two logs is more update by comparing the  index and term of the last entries in the logs. If the logs have last entries with different terms, then thee log with the later term is more update. If the logs end with the same term, then whichever log is longer more update
	if rf.getLastLogTerm() < args.LastLogTerm {
		isupdate = true
	}
	if rf.getLastLogTerm() == args.LastLogTerm && (rf.getLastLogIndex() <= args.LastLogIndex) {
		isupdate = true
	}
	// DPrintf("The candiate %d log: logIndex = %d, logTerm = %d update is %b", args.CandidateId, args.LastLogIndex, args.LastLogTerm, isupdate)

	// if votedFor is null or candidateId and log is update the grant vote
	if ((rf.votedFor == -1) || (args.CandidateId == rf.votedFor)) && isupdate {
		rf.chanGrantVote <- true
		reply.Term = rf.currentTerm
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// DPrintf("accepted server = %d voted_for candidate = %d\n", rf.me, args.CandidateId)
	}
	// Your code here (2A, 2B).
}

func timeNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond) // return the current time
}

// Request AppendEntries
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// check inconsistency
	// fmt.Printf("Leader's term = %v, Server = %v, its term = %v, lastLogTerm = %v \n", args.Term, rf.me, rf.currentTerm, rf.log[rf.getLastLogIndex()].LogTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	// if args.Term == rf.currentTerm {
	// 	reply.Term = args.Term
	// 	reply.Success = true
	// }
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastLogIndex() + 1 // special revise
		return
	}
	rf.chanHeartbeat <- true

	if rf.currentTerm < args.Term {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Success = true
	}
	reply.Term = args.Term

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}
	// this is the case that args.PrevLogIndex <= rf's last log index while rf.currentTerm < args.Term
	// 2B part, first receiver's implementation 2
	// concept:check prevLogIndex and find coresponding term match with prevLogTerm
	// check over Receiver's implementation 2: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// fmt.Print("Server = %d doesn't contain an entry at prevLogIndex = %d whose term matches prevLogTerm = %v", rf.me, rf.getLastLogIndex(), rf.getLastLogTerm())
	initialIndex := rf.log[0].LogIndex
	// // fmt.Printf("InitialIndex = %v, args.PrevLogIndex = %v, rf.lastApplied = %v \n", initialIndex, args.PrevLogIndex, rf.lastApplied)
	// DPrintf("initialIndex = %v, args.PrevLogIndex = %v  \n", initialIndex, args.PrevLogIndex)
	if initialIndex < args.PrevLogIndex {
		term := rf.log[args.PrevLogIndex-initialIndex].LogTerm // new one
		if term == args.PrevLogTerm {
		} else {

			for i := args.PrevLogIndex - 1; i >= initialIndex; i-- {
				if rf.log[i-initialIndex].LogTerm != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}

	}

	if initialIndex <= args.PrevLogIndex {
		// DPrintf("AppendEntries: args = %+v \n", args)
		// fmt.Printf("AppendEntries : args.Log = %+v \n", args.Entries)
		rf.log = rf.log[:args.PrevLogIndex-initialIndex+1] // if an existing entry conflict with a new one delete the existing entry and all that follow it
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.Term = args.Term
		reply.NextIndex = rf.getLastLogIndex() + 1
	}

	// If leaderCommit > commitIndex set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last_new_entry := rf.getLastLogIndex()
		if args.LeaderCommit > last_new_entry {
			rf.commitIndex = last_new_entry
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}

	// baseIndex := rf.log[0].LogIndex

	// if args.PrevLogIndex > baseIndex {
	// 	term := rf.log[args.PrevLogIndex-baseIndex].LogTerm
	// 	if args.PrevLogTerm != term {
	// 		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
	// 			if rf.log[i-baseIndex].LogTerm != term {
	// 				reply.NextIndex = i + 1
	// 				break
	// 			}
	// 		}
	// 		return
	// 	}
	// }
	// else {
	// 	//fmt.Printf("????? len:%v\n",len(args.Entries))
	// 	last := rf.getLastIndex()
	// 	elen := len(args.Entries)
	// 	for i := 0; i < elen ;i++ {
	// 		if args.PrevLogIndex + i > last || rf.log[args.PrevLogIndex + i].LogTerm != args.Entries[i].LogTerm {
	// 			rf.log = rf.log[: args.PrevLogIndex+1]
	// 			rf.log = append(rf.log, args.Entries...)
	// 			app = false
	// 			fmt.Printf("?????\n")
	// 			break
	// 		}
	// 	}
	// }
	// if args.PrevLogIndex < baseIndex {

	// } else {
	// 	rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
	// 	rf.log = append(rf.log, args.Entries...)
	// 	reply.Success = true
	// 	reply.NextIndex = rf.getLastLogIndex() + 1
	// }
	// //println(rf.me,rf.getLastIndex(),reply.NextIndex,rf.log)
	// if args.LeaderCommit > rf.commitIndex {
	// 	last := rf.getLastLogIndex()
	// 	if args.LeaderCommit > last {
	// 		rf.commitIndex = last
	// 	} else {
	// 		rf.commitIndex = args.LeaderCommit
	// 	}
	// 	rf.chanCommit <- true
	// }
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool { // server is which server, rf represents the reciver, args represents the sender
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// DPrintf("The term of reply is %d, and grantVote for Sender %d is %b", reply.Term, args.CandidateId, reply.VoteGranted)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok { // if called successfully
		// check if the state of rf is candidate, and rf.currentTerm is equal to args.term
		if rf.state != CANDIDATE || rf.currentTerm != args.Term { // if current term is less than reply's term
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.persist()
		}

		if reply.VoteGranted == true {
			// fmt.Printf("Leader = %v, receiver = %v, vote reply = %+v , leader's log = %+v \n", rf.currentLeader, server, reply, rf.log)
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = FOLLOWER
				rf.chanLeader <- true
			}

		}
	}
	return ok
}

// To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an AppendEntries RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// check if I'm the current leader, rf
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if rf.state == LEADER && reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}
		if rf.me != server {
			if reply.Success { // special if the reply is true update each rf's matchIndex or nextIndex
				// fmt.Printf("Leader = %v, receiver = %v, Append Entry reply = %+v , leader's log = %+v \n", args.LeaderId, server, reply, rf.log)
				if len(args.Entries) > 0 {
					rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1 // set to the one after current logIndex
					// matchIndex is set to as one before
					rf.matchIndex[server] = rf.nextIndex[server] - 1

				}

			} else {
				rf.nextIndex[server] = reply.NextIndex
			}

		}

	}
	return ok

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// change code here to append committed entry into log
	// everytime remember to lock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		// fmt.Printf("raft:%d, start, isLeader = %v, command = %v \n", rf.me, isLeader, command)
		index = rf.getLastLogIndex() + 1 // current index last index + 1
		rf.log = append(rf.log, Log{LogIndex: index, LogTerm: term, Command: command})
		rf.persist()
	}
	// fmt.Printf("Start function: Leader's Log = %+v \n", rf.log)
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// definition of callsendVoteRequest()
// which calls sendVoteRequest() in the function, which sends vote requests for all its peers, it's a general function
func (rf *Raft) callsendVoteRequest() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastLogIndex() // this is for part(B)
	args.LastLogTerm = rf.getLastLogTerm()   // this is for part(B)
	rf.mu.Unlock()
	// Then for the peer in peers array just send others each a sendVoteRequest
	for i := 0; i < len(rf.peers); i++ { // for each server in the peers array of ClientEnd
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				// fmt.Printf("Candidate = %v is sending Vote Request to server = %v, and args = %+v \n", rf.me, i, args)
				rf.sendRequestVote(i, &args, &reply) // write the requestVote RPC function upper
			}(i)

		} // the rf's state is candidate for each peer he will get a reply, here call sendRequestVote fucntion

	}
}

// definition of callAppendEntries RPC
// after being elected as leader, immediately send appendentries request to followers
func (rf *Raft) callAppendEntries() { // this rf here is representing the leader
	// timeout
	rf.mu.Lock()
	defer rf.mu.Unlock()
	initialIndex := rf.log[0].LogIndex
	N := rf.commitIndex // index of highest log entry known to be committed (initialize as 0, increase monotonically)
	last_index := rf.log[len(rf.log)-1].LogIndex
	for i := rf.commitIndex + 1; i <= last_index; i++ {
		num_count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				num_count++

			}
		}
		if num_count > len(rf.peers)/2 {
			N = i
		}

	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == LEADER { // rf as the leader
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1 // first initialize as the one preceding the new one
			// fmt.Printf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< check >>>>>>>>>>>>>>>>>>>>>>>>> PrevLogIndex = %v \n", args.PrevLogIndex)
			args.PrevLogTerm = rf.log[args.PrevLogIndex-initialIndex].LogTerm // the exact PrevLogTerm on PrevLogIndex

			args.Entries = make([]Log, len(rf.log[args.PrevLogIndex+1-initialIndex:])) // log entries to store
			copy(args.Entries, rf.log[args.PrevLogIndex+1-initialIndex:])
			args.LeaderCommit = rf.commitIndex
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				// fmt.Printf("Leader = %v is sending AppendEntries Request to server = %v, and args = %+v \n", rf.me, i, args)
				rf.sendAppendEntries(i, args, &reply)
			}(i, args)
		}
		// so u r the leader, uhah!

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// start from state follower
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, Log{LogTerm: 0}) // initialize with an empty log entries for each server
	rf.chanApply = applyCh
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHeartbeatreply = make(chan bool, 100)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(rand.Int63()%200+200) * time.Millisecond):
					//DPrintf("The server %d elapsed time out, becomes a candidate", rf.me)
					rf.state = CANDIDATE
				}
			case CANDIDATE: // deal with the case candidate, sendVoteRequest to other severs
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist() // ?
				rf.mu.Unlock()
				// DPrintf("Candidate %d, currentTerm is %d, voteCount is %d", rf.me, rf.currentTerm, rf.voteCount)
				// send vote requests to other servers
				go rf.callsendVoteRequest()
				select {
				case <-time.After(time.Duration(rand.Int63()%200+200) * time.Millisecond):
				case <-rf.chanHeartbeat:
					rf.state = FOLLOWER
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = LEADER
					//DPrintf("%v is leader", rf.me)
					rf.nextIndex = make([]int, len(rf.peers))  // for each server, index of the next log entry to send to that server
					rf.matchIndex = make([]int, len(rf.peers)) // for each server, index of highest log entry known to be replicated on server
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			case LEADER:
				//DPrintf("Server %d becomes leader, and term is %v", rf.me, rf.currentTerm)
				rf.callAppendEntries() // send appenentry request to stop time out for followers
				time.Sleep(HiberInterval)

			}

		}

	}()
	go func() {
		// put rf.log into the applyChan
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				initialIndex := rf.log[0].LogIndex
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-initialIndex].Command}
					applyCh <- msg
					rf.lastApplied = i
				}

				rf.mu.Unlock()
			}

		}
	}()

	return rf
}
