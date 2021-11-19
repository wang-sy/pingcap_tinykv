package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++

	if len(r.peers) == 0 {
		r.becomeLeader()
	}

	r.Vote = r.id
	r.votes = map[uint64]bool{r.id: true}
}

// doElection make raft a candidate and request for vote.
func (r *Raft) doElection() {
	r.becomeCandidate()
	r.broadcastVoteRequest()
}

// step for Candidate.
func (r *Raft) stepCandidate(msg pb.Message) error {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(msg)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(msg)
	case pb.MessageType_MsgAppend:
		if msg.Term >= r.Term {
			r.becomeFollower(msg.Term, r.Term)
		}
		r.handleAppendEntries(msg)
	}
	return nil
}

// broadcastVoteRequest sends VoteRequest RPC to peers.
func (r *Raft) broadcastVoteRequest() {
	for _, peer := range r.peers {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
		})
	}
}

// handleVoteResponse candidate resolve votes from peers.
func (r *Raft) handleVoteResponse(msg pb.Message) {
	if msg.Reject {
		return
	}

	r.votes[msg.From] = true
	if len(r.votes) > (len(r.peers)+1)/2 {
		r.becomeLeader()
	}
}
