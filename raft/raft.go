// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	peers []uint64

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// electionTimeSeed is used to generate electionTimeout.
	electionTimeSeed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// peersCopy is peers after remove current node.
	peersCopy := make([]uint64, 0, len(c.peers)-1)
	for _, peer := range c.peers {
		if peer == c.ID {
			continue
		}

		peersCopy = append(peersCopy, peer)
	}

	// generate election timeout by given election seed.
	electionTimeout := c.ElectionTick + rand.Intn(c.ElectionTick)

	raft := &Raft{
		id:    c.ID,
		peers: peersCopy,

		electionTimeSeed: c.ElectionTick,
		electionTimeout:  electionTimeout,
		heartbeatTimeout: c.HeartbeatTick,

		Prs:   map[uint64]*Progress{},
		votes: map[uint64]bool{},
	}

	raft.becomeFollower(0, None)

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateLeader {
		r.leaderTick()
	} else {
		r.normalTick()
	}
}

// normalTick is tick for Follower|Candidate.
func (r *Raft) normalTick() {
	r.electionElapsed++

	if r.electionTimeout == r.electionElapsed {
		r.electionElapsed = 0
		r.electionTimeout = r.electionTimeSeed + rand.Intn(r.electionTimeSeed)

		r.doElection()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		if isMessageFromLeader(m) {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	default:
		return errors.New("raft state error")
	}
}

// handleVoteRequest raft status may be Follower|Candidate|Leader when called.
func (r *Raft) handleVoteRequest(m pb.Message) {
	if m.Term < r.Term {
		r.sendRejectVoteResponse(m.From)
		return
	}

	if m.Term == r.Term && r.Vote != None && r.Vote != m.From {
		r.sendRejectVoteResponse(m.From)
		return
	}

	r.Vote = m.From
	r.sendAcceptVoteResponse(m.From)
}

// sendAcceptVoteResponse send accept VoteResposne to toID.
func (r *Raft) sendAcceptVoteResponse(toID uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
		To:      toID,

		Reject: false,
	})
}

// sendRejectVoteResponse send reject VoteResposne to toID.
func (r *Raft) sendRejectVoteResponse(toID uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		Term:    r.Term,
		To:      toID,

		Reject: true,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
