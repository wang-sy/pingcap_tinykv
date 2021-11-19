package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader

	// TODO (SaiyuWang): propose a noop entry.

	r.heartbeatElapsed = 0
}

// leaderTick.
func (r *Raft) leaderTick() {
	r.heartbeatElapsed++

	if r.heartbeatTimeout == r.heartbeatElapsed {
		r.heartbeatElapsed = 0
		r.broadcastHeartbeats()
	}
}

// step for Leader.
func (r *Raft) stepLeader(msg pb.Message) error {
	switch msg.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(msg)
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeats()
	}
	return nil
}

// broadcastHeartbeats sends heartbeat RPC to peers.
func (r *Raft) broadcastHeartbeats() {
	for _, peer := range r.peers {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
		})
	}
}

// isMessageFromLeader check if msg from leader.
func isMessageFromLeader(msg pb.Message) bool {
	return msg.MsgType == pb.MessageType_MsgAppend ||
		msg.MsgType == pb.MessageType_MsgSnapshot ||
		msg.MsgType == pb.MessageType_MsgHeartbeat
}
