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
	"bytes"
	"log"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role int

const (
	Follower = Role(iota)
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    uint
	Command interface{}
}

type PersistedState struct {
	CurrentTerm       uint
	VotedFor          *uint
	Log               []LogEntry
	LastIncludedIndex uint
	LastIncludedTerm  uint
}

type VolatileState struct {
	CommitIndex   uint
	LastApplied   uint
	LastHeartbeat time.Time
	Role          Role
	VoteCount     uint32
}

type VolatileLeaderState struct {
	NextIndex  []uint
	MatchIndex []uint
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        uint                // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg

	persistedState *PersistedState
	volatileState  *VolatileState
	leaderState    *VolatileLeaderState
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := int(rf.persistedState.CurrentTerm)
	isleader := rf.volatileState.Role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.persistedState)
	if err != nil {
		log.Fatal(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// persistWithSnapshot saves both raft state and snapshot
func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.persistedState)
	if err != nil {
		log.Fatal(err)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.persistedState = &PersistedState{
			CurrentTerm:       0,
			VotedFor:          nil,
			Log:               []LogEntry{{Term: 0, Command: nil}},
			LastIncludedIndex: 0,
			LastIncludedTerm:  0,
		}
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&rf.persistedState)
	if err != nil {
		log.Fatal(err)
	}
}

// getLogIndex converts a Raft log index to a slice index
func (rf *Raft) getLogIndex(raftIndex uint) int {
	return int(raftIndex - rf.persistedState.LastIncludedIndex)
}

// getRaftIndex converts a slice index to a Raft log index
func (rf *Raft) getRaftIndex(sliceIndex int) uint {
	return uint(sliceIndex) + rf.persistedState.LastIncludedIndex
}

// getLastLogIndex returns the last log index in Raft terms
func (rf *Raft) getLastLogIndex() uint {
	return rf.persistedState.LastIncludedIndex + uint(len(rf.persistedState.Log)-1)
}

// getLastLogTerm returns the term of the last log entry
func (rf *Raft) getLastLogTerm() uint {
	if len(rf.persistedState.Log) == 0 {
		return rf.persistedState.LastIncludedTerm
	}
	return rf.persistedState.Log[len(rf.persistedState.Log)-1].Term
}

// getLogEntry returns the log entry at the given Raft index (not slice index)
func (rf *Raft) getLogEntry(raftIndex uint) LogEntry {
	sliceIndex := rf.getLogIndex(raftIndex)
	if sliceIndex < 0 || sliceIndex >= len(rf.persistedState.Log) {
		// This shouldn't happen in normal operation
		return LogEntry{Term: 0, Command: nil}
	}
	return rf.persistedState.Log[sliceIndex]
}

// getLogTerm returns the term of the log entry at the given Raft index
func (rf *Raft) getLogTerm(raftIndex uint) uint {
	if raftIndex == rf.persistedState.LastIncludedIndex {
		return rf.persistedState.LastIncludedTerm
	}
	if raftIndex < rf.persistedState.LastIncludedIndex {
		return 0
	}
	sliceIndex := rf.getLogIndex(raftIndex)
	if sliceIndex < 0 || sliceIndex >= len(rf.persistedState.Log) {
		return 0
	}
	return rf.persistedState.Log[sliceIndex].Term
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Can't snapshot entries that haven't been committed yet
	if uint(index) <= rf.persistedState.LastIncludedIndex {
		// Already have a snapshot at least this far
		return
	}

	if uint(index) > rf.volatileState.CommitIndex {
		// Can't snapshot uncommitted entries
		return
	}

	// Get the term of the last included entry
	lastIncludedTerm := rf.getLogTerm(uint(index))

	// Trim the log - keep entries after index
	// The new log[0] will be a dummy entry representing the snapshot
	sliceIndex := rf.getLogIndex(uint(index))
	if sliceIndex < len(rf.persistedState.Log) {
		newLog := make([]LogEntry, len(rf.persistedState.Log)-sliceIndex)
		copy(newLog, rf.persistedState.Log[sliceIndex:])
		rf.persistedState.Log = newLog
	} else {
		// All entries are in the snapshot, keep just a dummy entry
		rf.persistedState.Log = []LogEntry{{Term: lastIncludedTerm, Command: nil}}
	}

	rf.persistedState.LastIncludedIndex = uint(index)
	rf.persistedState.LastIncludedTerm = lastIncludedTerm

	rf.persistWithSnapshot(snapshot)
}

type RequestVoteArgs struct {
	Term         uint
	CandidateId  uint
	LastLogIndex uint
	LastLogTerm  uint
}

type RequestVoteReply struct {
	Term        uint
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistedState.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	if args.Term > rf.persistedState.CurrentTerm {
		rf.volatileState.Role = Follower
		rf.persistedState.CurrentTerm = args.Term
		rf.persistedState.VotedFor = nil
		rf.persist()
	}

	if rf.persistedState.VotedFor != nil && *rf.persistedState.VotedFor != args.CandidateId {
		return
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if !logIsUpToDate {
		return
	}

	rf.persistedState.VotedFor = &args.CandidateId
	rf.volatileState.LastHeartbeat = time.Now()
	rf.persist()
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         uint
	LeaderId     uint
	PrevLogIndex uint
	PrevLogTerm  uint
	Entries      []LogEntry
	LeaderCommit uint
}

type AppendEntriesReply struct {
	Term          uint
	Success       bool
	ConflictTerm  uint
	ConflictIndex uint
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.persistedState.CurrentTerm
	reply.Success = false

	if args.Term < rf.persistedState.CurrentTerm {
		return
	}

	rf.volatileState.LastHeartbeat = time.Now()

	if args.Term >= rf.persistedState.CurrentTerm {
		rf.volatileState.Role = Follower
		if args.Term > rf.persistedState.CurrentTerm {
			rf.persistedState.CurrentTerm = args.Term
			rf.persistedState.VotedFor = nil
			rf.persist()
		}
	}

	// If PrevLogIndex is before our snapshot, check if we can still handle this
	if args.PrevLogIndex < rf.persistedState.LastIncludedIndex {
		// PrevLogIndex is in the snapshot - check if entries overlap with our log
		if args.PrevLogIndex+uint(len(args.Entries)) <= rf.persistedState.LastIncludedIndex {
			// All entries are already in our snapshot, just succeed
			reply.Success = true
			return
		}
		// Some entries overlap, trim entries that are in snapshot
		entriesStart := rf.persistedState.LastIncludedIndex - args.PrevLogIndex
		args.Entries = args.Entries[entriesStart:]
		args.PrevLogIndex = rf.persistedState.LastIncludedIndex
		args.PrevLogTerm = rf.persistedState.LastIncludedTerm
	}

	// Log consistency check
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = 0
		return
	}

	// Check if entry at PrevLogIndex has matching term
	prevLogTerm := rf.getLogTerm(args.PrevLogIndex)
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		// Find the first index with ConflictTerm
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.persistedState.LastIncludedIndex && rf.getLogTerm(conflictIndex-1) == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	// Append new entries, check for conflicts
	logChanged := false
	if len(args.Entries) > 0 {
		insertRaftIndex := args.PrevLogIndex + 1
		newIndex := 0

		// Find first conflict
		for newIndex < len(args.Entries) && insertRaftIndex <= rf.getLastLogIndex() {
			if rf.getLogTerm(insertRaftIndex) != args.Entries[newIndex].Term {
				// Delete conflicting entry and all that follow
				sliceIndex := rf.getLogIndex(insertRaftIndex)
				rf.persistedState.Log = rf.persistedState.Log[:sliceIndex]
				logChanged = true
				break
			}
			insertRaftIndex++
			newIndex++
		}

		// Append any remaining new entries
		if newIndex < len(args.Entries) {
			rf.persistedState.Log = append(rf.persistedState.Log, args.Entries[newIndex:]...)
			logChanged = true
		}
	}

	if logChanged {
		rf.persist()
	}

	if args.LeaderCommit > rf.volatileState.CommitIndex {
		newCommitIndex := args.LeaderCommit
		lastNewEntry := rf.getLastLogIndex()
		if newCommitIndex > lastNewEntry {
			newCommitIndex = lastNewEntry
		}
		rf.volatileState.CommitIndex = newCommitIndex
	}

	reply.Success = true
}

// InstallSnapshot RPC arguments
type InstallSnapshotArgs struct {
	Term              uint
	LeaderId          uint
	LastIncludedIndex uint
	LastIncludedTerm  uint
	Data              []byte
}

// InstallSnapshot RPC reply
type InstallSnapshotReply struct {
	Term uint
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.persistedState.CurrentTerm

	if args.Term < rf.persistedState.CurrentTerm {
		rf.mu.Unlock()
		return
	}

	rf.volatileState.LastHeartbeat = time.Now()

	if args.Term > rf.persistedState.CurrentTerm {
		rf.volatileState.Role = Follower
		rf.persistedState.CurrentTerm = args.Term
		rf.persistedState.VotedFor = nil
	}

	// Ignore if we already have a snapshot at a higher or equal index
	// or if we've already applied past this point
	if args.LastIncludedIndex <= rf.persistedState.LastIncludedIndex ||
		args.LastIncludedIndex <= rf.volatileState.LastApplied {
		rf.mu.Unlock()
		return
	}

	// If we have log entries covered by the snapshot, discard them
	// Check if snapshot is within our current log
	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		// Keep entries after the snapshot
		sliceIndex := rf.getLogIndex(args.LastIncludedIndex)
		if sliceIndex >= 0 && sliceIndex < len(rf.persistedState.Log) {
			newLog := make([]LogEntry, len(rf.persistedState.Log)-sliceIndex)
			copy(newLog, rf.persistedState.Log[sliceIndex:])
			rf.persistedState.Log = newLog
		} else {
			rf.persistedState.Log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
		}
	} else {
		rf.persistedState.Log = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	}

	rf.persistedState.LastIncludedIndex = args.LastIncludedIndex
	rf.persistedState.LastIncludedTerm = args.LastIncludedTerm

	if args.LastIncludedIndex > rf.volatileState.CommitIndex {
		rf.volatileState.CommitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.volatileState.LastApplied {
		rf.volatileState.LastApplied = args.LastIncludedIndex
	}

	rf.persistWithSnapshot(args.Data)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}

	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}

func (rf *Raft) requestVoteFromPeer(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.persistedState.CurrentTerm {
		rf.volatileState.Role = Follower
		rf.volatileState.LastHeartbeat = time.Now()
		rf.persistedState.CurrentTerm = reply.Term
		rf.persistedState.VotedFor = nil
		rf.persist()
		return
	}

	if rf.volatileState.Role != Candidate || args.Term != rf.persistedState.CurrentTerm {
		return
	}

	if reply.VoteGranted {
		rf.volatileState.VoteCount += 1
		if rf.volatileState.VoteCount > uint32(len(rf.peers)/2) {
			rf.volatileState.Role = Leader
			lastLogIndex := rf.getLastLogIndex()
			rf.leaderState = &VolatileLeaderState{
				NextIndex:  make([]uint, len(rf.peers)),
				MatchIndex: make([]uint, len(rf.peers)),
			}
			for i := range rf.peers {
				rf.leaderState.NextIndex[i] = lastLogIndex + 1
				rf.leaderState.MatchIndex[i] = 0
			}
			rf.leaderState.MatchIndex[rf.me] = lastLogIndex
			rf.sendHeartbeat()
		}
	}

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := int(rf.persistedState.CurrentTerm)
	isLeader := rf.volatileState.Role == Leader

	if !isLeader {
		return index, term, isLeader
	}

	entry := LogEntry{
		Term:    rf.persistedState.CurrentTerm,
		Command: command,
	}
	rf.persistedState.Log = append(rf.persistedState.Log, entry)
	rf.persist()

	raftIndex := rf.getLastLogIndex()
	index = int(raftIndex)

	rf.leaderState.MatchIndex[rf.me] = raftIndex
	rf.leaderState.NextIndex[rf.me] = raftIndex + 1

	for i := range rf.peers {
		if uint(i) == rf.me {
			continue
		}
		rf.sendLogToFollower(i)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestAllVotes() {
	voteArgs := &RequestVoteArgs{
		Term:         rf.persistedState.CurrentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTerm(),
		LastLogIndex: rf.getLastLogIndex(),
	}
	for i := range rf.peers {
		if uint(i) == rf.me {
			continue
		}
		go rf.requestVoteFromPeer(i, voteArgs)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.persistedState.CurrentTerm {
		rf.volatileState.Role = Follower
		rf.volatileState.LastHeartbeat = time.Now()
		rf.persistedState.CurrentTerm = reply.Term
		rf.persistedState.VotedFor = nil
		rf.persist()
		return
	}

	if rf.volatileState.Role != Leader || args.Term != rf.persistedState.CurrentTerm {
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + uint(len(args.Entries))
		newNextIndex := newMatchIndex + 1

		if newMatchIndex > rf.leaderState.MatchIndex[server] {
			rf.leaderState.MatchIndex[server] = newMatchIndex
		}
		if newNextIndex > rf.leaderState.NextIndex[server] {
			rf.leaderState.NextIndex[server] = newNextIndex
		}

		rf.updateCommitIndex()
	} else {
		if reply.ConflictTerm > 0 {
			lastIndexOfTerm := rf.persistedState.LastIncludedIndex
			found := false
			for i := rf.getLastLogIndex(); i > rf.persistedState.LastIncludedIndex; i-- {
				if rf.getLogTerm(i) == reply.ConflictTerm {
					lastIndexOfTerm = i
					found = true
					break
				}
			}

			if found {
				rf.leaderState.NextIndex[server] = lastIndexOfTerm + 1
			} else {
				rf.leaderState.NextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.leaderState.NextIndex[server] = reply.ConflictIndex
		}

		// If nextIndex points to an entry we don't have in the snapshot,
		// we'll need to send InstallSnapshot on the next heartbeat.
		// Set it to LastIncludedIndex to trigger.
		if rf.leaderState.NextIndex[server] <= rf.persistedState.LastIncludedIndex {
			rf.leaderState.NextIndex[server] = rf.persistedState.LastIncludedIndex
		}

		if rf.leaderState.NextIndex[server] < 1 {
			rf.leaderState.NextIndex[server] = 1
		}
	}

}

// sendInstallSnapshot sends a snapshot to a lagging follower
func (rf *Raft) sendInstallSnapshot(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.persistedState.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.persistedState.LastIncludedIndex,
		LastIncludedTerm:  rf.persistedState.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	go func() {
		reply := &InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.persistedState.CurrentTerm {
			rf.volatileState.Role = Follower
			rf.volatileState.LastHeartbeat = time.Now()
			rf.persistedState.CurrentTerm = reply.Term
			rf.persistedState.VotedFor = nil
			rf.persist()
			return
		}

		if rf.volatileState.Role != Leader || args.Term != rf.persistedState.CurrentTerm {
			return
		}

		if args.LastIncludedIndex+1 > rf.leaderState.NextIndex[server] {
			rf.leaderState.NextIndex[server] = args.LastIncludedIndex + 1
		}
		if args.LastIncludedIndex > rf.leaderState.MatchIndex[server] {
			rf.leaderState.MatchIndex[server] = args.LastIncludedIndex
		}
	}()
}

// sendLogToFollower sends either AppendEntries or InstallSnapshot to a follower
// Must be called with lock held
func (rf *Raft) sendLogToFollower(server int) {
	nextIndex := rf.leaderState.NextIndex[server]

	if nextIndex <= rf.persistedState.LastIncludedIndex {
		rf.sendInstallSnapshot(server)
		return
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.getLogTerm(prevLogIndex)

	var entries []LogEntry
	if nextIndex <= rf.getLastLogIndex() {
		// Get entries from nextIndex to end of log
		startSliceIndex := rf.getLogIndex(nextIndex)
		if startSliceIndex >= 0 && startSliceIndex < len(rf.persistedState.Log) {
			entries = make([]LogEntry, len(rf.persistedState.Log)-startSliceIndex)
			copy(entries, rf.persistedState.Log[startSliceIndex:])
		}
	} else {
		entries = make([]LogEntry, 0)
	}

	args := &AppendEntriesArgs{
		Term:         rf.persistedState.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.volatileState.CommitIndex,
	}

	go rf.sendAppendEntries(server, args)
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if uint(i) == rf.me {
			continue
		}
		rf.sendLogToFollower(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.getLastLogIndex(); n > rf.volatileState.CommitIndex && n > rf.persistedState.LastIncludedIndex; n-- {
		term := rf.getLogTerm(n)
		if term != rf.persistedState.CurrentTerm {
			continue
		}

		count := 1 // Count self
		for i := range rf.peers {
			if uint(i) != rf.me && rf.leaderState.MatchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.volatileState.CommitIndex = n
			break
		}
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		// Calculate what we need to apply
		lastApplied := rf.volatileState.LastApplied
		commitIndex := rf.volatileState.CommitIndex
		lastIncludedIndex := rf.persistedState.LastIncludedIndex

		if lastApplied < lastIncludedIndex {
			// Snapshot installed, skip ahead
			rf.volatileState.LastApplied = lastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		if lastApplied >= commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		nextApply := lastApplied + 1

		// Make sure we're not trying to apply something in the snapshot
		if nextApply <= lastIncludedIndex {
			rf.volatileState.LastApplied = lastIncludedIndex
			rf.mu.Unlock()
			continue
		}

		sliceIndex := rf.getLogIndex(nextApply)
		if sliceIndex <= 0 || sliceIndex >= len(rf.persistedState.Log) {
			// Entry not in log
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.persistedState.Log[sliceIndex].Command,
			CommandIndex: int(nextApply),
		}

		rf.volatileState.LastApplied = nextApply
		rf.mu.Unlock()

		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) ticker() {
	heartbeatInterval := 50 * time.Millisecond

	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.volatileState.Role
		rf.mu.Unlock()

		if role == Leader {
			rf.mu.Lock()
			rf.sendHeartbeat()
			rf.mu.Unlock()
			time.Sleep(heartbeatInterval)
		} else {
			electionTimeout := time.Duration(150+rand.Int63()%150) * time.Millisecond
			time.Sleep(heartbeatInterval)

			rf.mu.Lock()
			timeSinceHeartbeat := time.Since(rf.volatileState.LastHeartbeat)
			if rf.volatileState.Role != Leader && timeSinceHeartbeat >= electionTimeout {
				// Start election
				rf.persistedState.CurrentTerm += 1
				rf.persistedState.VotedFor = &rf.me
				rf.persist()
				rf.volatileState.VoteCount = 1
				rf.volatileState.Role = Candidate
				rf.volatileState.LastHeartbeat = time.Now()
				rf.requestAllVotes()
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = uint(me)
	rf.applyCh = applyCh

	rf.volatileState = &VolatileState{
		LastHeartbeat: time.Now(),
	}
	rf.leaderState = &VolatileLeaderState{}

	rf.readPersist(persister.ReadRaftState())

	rf.volatileState.LastApplied = rf.persistedState.LastIncludedIndex
	rf.volatileState.CommitIndex = rf.persistedState.LastIncludedIndex

	go rf.ticker()
	go rf.applier()

	return rf
}
