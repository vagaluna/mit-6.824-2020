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

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

type LogEntry struct {
	Term int
	Content interface{}
}

type VoteResult int

const (
	Granted VoteResult = iota
	Denied
	NewTerm
)

const HeartbeatTimeout = time.Millisecond * 125

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state	  State
	log 	  []LogEntry
	currentTerm      int
	votedFor  int
	commitIndex int              // Start from 1

	nextIndex []int
	matchIndex []int

	heartbeatChan chan int
	followChan    chan int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

func electionTimeout() time.Duration {
	return time.Duration(rand.Intn(250) + 500) * time.Millisecond
}

func (rf *Raft) setState(state State) {
	rf.state = state
	switch state {
	case Follower:
		go rf.runAsFollower()
	case Candidate:
		go rf.runAsCandidate()
	case Leader:
		go rf.runAsLeader()
	}
}

func (rf *Raft) describe() string {
	var s string
	switch rf.state {
	case Follower:
		s = "F"
	case Leader:
		s = "L"
	case Candidate:
		s = "C"
	}
	return fmt.Sprintf("%v[%v][%v]", rf.me, rf.currentTerm, s)
}

func (rf *Raft) logf(format string, a ...interface{}) {
	format2 := rf.describe() + " " + format
	log.Printf(format2, a...)
}

func (rf *Raft) lastTermIndex() (int, int) {
	if n := len(rf.log); n == 0 {
		return 0, -1
	} else {
		return rf.log[n - 1].Term, n - 1
	}
}

func (rf *Raft) runAsFollower() {
	rf.logf("started running as Follower")
	for !rf.killed() {
		timer := time.NewTimer(electionTimeout())
		select {
		case <- rf.heartbeatChan:
			timer.Stop()
		case <- timer.C:
			rf.mu.Lock()
			rf.setState(Candidate)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) startElection() <- chan int {
	rf.mu.Lock()
	rf.updateTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.mu.Unlock()

	voteChan := make(chan int, len(rf.peers) - 1)

	rf.logf("start election")
	rf.broadcastRequestVote(func(reply *RequestVoteReply) {
		rf.mu.Lock()
		if reply.Granted {
			voteChan <- 1
		} else if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
			if rf.state != Follower {
				rf.convertToFollower()
			}
		}
		rf.mu.Unlock()
	})

	return voteChan
}

func (rf *Raft) runAsCandidate() {
	rf.logf("started running as Candidate")
	for !rf.killed() {
		voteChan := rf.startElection()
		timer := time.NewTimer(electionTimeout())
		votes := 1
		CandidateLoop:
		for {
			select {
			case <-timer.C:
				rf.logf("election timeout before getting enough votes")
				break CandidateLoop
			case <-voteChan:
				rf.logf("got vote")
				votes += 1
				if votes * 2 > len(rf.peers) {
					rf.mu.Lock()
					rf.setState(Leader)
					rf.mu.Unlock()
					return
				}
			case <-rf.followChan:
				return
			}
		}
	}
}

func (rf *Raft) runAsLeader() {
	rf.logf("started running as Leader")
	rf.mu.Lock()
	_, lastIndex := rf.lastTermIndex()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex + 1
	}
	rf.mu.Unlock()

	for !rf.killed() {
		timer := time.NewTimer(HeartbeatTimeout)
		select {
		case <-timer.C:
			rf.broadcastAppendEntries(func(follower int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				rf.mu.Lock()
				if reply.Success {
					if rf.state == Leader {
						rf.nextIndex[follower] += len(args.Entries)
					}
				} else if reply.Term > rf.currentTerm {
					rf.updateTerm(reply.Term)
					if rf.state != Follower {
						rf.convertToFollower()
					}
				}
				rf.mu.Unlock()
			})
		case <-rf.followChan:
			return
		}
	}
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) convertToFollower() {
	// Raft must not be running as Follower
	rf.logf("convert to Follower")
	rf.setState(Follower)
	rf.followChan <- 1
}

func compareLog(term1, index1, term2, index2 int) int {
	if term1 < term2 {
		return -1
	} else if term1 > term2 {
		return 1
	} else {
		if index1 < index2 {
			return -1
		} else if index1 > index2 {
			return 1
		} else {
			return 0
		}
	}
}


func (rf *Raft) voteForCandidate(candidateId int, candidateLastTerm int, candidateLastIndex int) bool {
	// Raft must be in Follower state when voting for others
	if rf.votedFor >= 0 && rf.votedFor != candidateId {
		return false
	}

	lastTerm, lastIndex := rf.lastTermIndex()
	vote := compareLog(candidateLastTerm, candidateLastIndex, lastTerm, lastIndex) >= 0
	if vote {
		rf.votedFor = candidateId
		rf.heartbeatChan <- 1
	}
	return vote
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogTerm int
	LastLogIndex int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Granted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.logf("received RequestVote from %v[%v]", args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	if term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Granted = false
		rf.logf("refused RequestVote from %v[%v]", args.CandidateId, args.Term)
		return
	}

	if term > rf.currentTerm {
		rf.updateTerm(term)
		if rf.state != Follower {
			rf.convertToFollower()
		}
	}

	reply.Term = rf.currentTerm
	reply.Granted = rf.voteForCandidate(args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	rf.logf("vote to %v[%v] - %v", args.CandidateId, args.Term, reply.Granted)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logf("received AppendEntries from %v[%v]", args.LeaderId, args.Term)
	term := args.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.logf("refused AppendEntries from %v[%v]", args.LeaderId, args.Term)
		return
	}

	switch rf.state {
	case Follower:
		if term > rf.currentTerm {
			rf.updateTerm(term)
		}
		rf.heartbeatChan <- 1
	case Candidate:
		if term > rf.currentTerm {
			rf.updateTerm(term)
		}
		rf.convertToFollower()
	case Leader:
		if term > rf.currentTerm {
			rf.updateTerm(term)
			rf.convertToFollower()
		} else {
			log.Fatalln("Received AppendEntries call when running as Leader, should never happen")
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote(callback func (*RequestVoteReply)) {
	lastTerm, lastIndex := rf.lastTermIndex()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogTerm: lastTerm,
		LastLogIndex: lastIndex,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				callback(&reply)
			}
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries(callback func (int, *AppendEntriesArgs, *AppendEntriesReply)) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		var prevTerm int
		next := rf.nextIndex[i]
		if next >= 1 {
			prevTerm = rf.log[next - 1].Term
		} else {
			prevTerm = -1
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[next:],
			LeaderCommit: rf.commitIndex,
		}

		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				callback(server, &args, &reply)
			}
		}(i)
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	rf.logf("is killed")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.state = Follower

	rf.log = []LogEntry{}
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = -1
	rf.dead = 0

	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)

	rf.heartbeatChan = make(chan int)
	rf.followChan = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runAsFollower()

	return rf
}
