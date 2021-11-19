package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None

	r.electionElapsed = 0
}

// step for Follower.
func (r *Raft) stepFollower(msg pb.Message) error {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(msg)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	}
	return nil
}
