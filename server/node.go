package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
)

type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

type Node struct {
	pb.UnimplementedAuctionServiceServer
	pb.UnimplementedNodeServiceServer

	id       string
	port     string
	role     NodeRole
	term     int64
	votedFor string

	// Auction state
	auctions map[string]*pb.AuctionState
	mu       sync.RWMutex

	// Cluster configuration
	peers          []string
	lastHeartbeat  time.Time
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker

	// gRPC clients for peer communication
	peerClients map[string]pb.NodeServiceClient
}

func NewNode(id, port string, peers []string) *Node {
	node := &Node{
		id:             id,
		port:           port,
		role:           Follower,
		term:           0,
		auctions:       make(map[string]*pb.AuctionState),
		peers:          peers,
		lastHeartbeat:  time.Now(),
		peerClients:    make(map[string]pb.NodeServiceClient),
		heartbeatTimer: time.NewTicker(1 * time.Second),
	}

	for _, peer := range peers {
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to peer %s: %v", peer, err)
			continue
		}
		node.peerClients[peer] = pb.NewNodeServiceClient(conn)
	}

	return node
}

func (n *Node) Start() {
	lis, err := net.Listen("tcp", n.port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAuctionServiceServer(grpcServer, n)
	pb.RegisterNodeServiceServer(grpcServer, n)

	go func() {
		log.Printf("Node %s listening on %s", n.id, n.port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	go n.runElectionLoop()
}

func (n *Node) runElectionLoop() {
	for {
		n.resetElectionTimer()

		select {
		case <-n.electionTimer.C:
			n.mu.RLock()
			role := n.role
			n.mu.RUnlock()

			if role != Leader {
				log.Printf("Node %s election timeout — starting election", n.id)
				n.startElection()
			}

		case <-n.heartbeatTimer.C:
			n.mu.RLock()
			if n.role == Leader {
				n.mu.RUnlock()
				n.sendHeartbeats()
			} else {
				n.mu.RUnlock()
			}
		}
	}
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
	n.electionTimer = time.NewTimer(timeout)
}

func (n *Node) startElection() {
	// Node 3 never becomes a leader
	if n.id == "node3" {
		log.Printf("Node %s is a passive follower", n.id)
		n.resetElectionTimer()
		return
	}

	n.mu.Lock()
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	currentTerm := n.term
	n.mu.Unlock()

	log.Printf("Node %s starting election for term %d", n.id, currentTerm)

	votes := 1 // self-vote
	var voteMu sync.Mutex
	voteChan := make(chan bool, len(n.peerClients))

	for peer, client := range n.peerClients {
		go func(p string, c pb.NodeServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			resp, err := c.RequestVote(ctx, &pb.RequestVoteRequest{
				CandidateId: n.id,
				Term:        currentTerm,
			})
			if err != nil {
				voteChan <- false
				return
			}

			if resp.VoteGranted {
				voteChan <- true
			} else {
				voteChan <- false
			}
		}(peer, client)
	}

	timeout := time.After(300 * time.Millisecond)

	for i := 0; i < len(n.peerClients); i++ {
		select {
		case granted := <-voteChan:
			if granted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
				if votes > len(n.peers)/2 {
					n.becomeLeader()
					return
				}
			}
		case <-timeout:
			log.Printf("Node %s election timed out", n.id)
			n.resetElectionTimer()
			return
		}
	}

	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Candidate {
		return
	}

	n.role = Leader
	log.Printf("Node %s became LEADER for term %d", n.id, n.term)
	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.role != Leader {
		n.mu.RUnlock()
		return
	}
	currentTerm := n.term
	n.mu.RUnlock()

	for _, client := range n.peerClients {
		go func(c pb.NodeServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			c.Heartbeat(ctx, &pb.HeartbeatRequest{
				NodeId:   n.id,
				Term:     currentTerm,
				IsLeader: true,
			})
		}(client)
	}

	n.replicateState()
}

func (n *Node) replicateState() {
	n.mu.RLock()
	auctions := make([]*pb.AuctionState, 0, len(n.auctions))
	for _, auction := range n.auctions {
		auctions = append(auctions, auction)
	}
	currentTerm := n.term
	n.mu.RUnlock()

	for _, client := range n.peerClients {
		go func(c pb.NodeServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			c.ReplicateState(ctx, &pb.ReplicateStateRequest{
				NodeId:   n.id,
				Term:     currentTerm,
				Auctions: auctions,
			})
		}(client)
	}
}

// --- RPC Handlers ---

func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term >= n.term {
		n.term = req.Term
		if n.role != Follower {
			n.role = Follower
			log.Printf("Node %s became FOLLOWER (heartbeat from %s)", n.id, req.NodeId)
		}
		n.lastHeartbeat = time.Now()
		n.resetElectionTimer()
	}

	return &pb.HeartbeatResponse{Success: true, Term: n.term}, nil
}

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return &pb.RequestVoteResponse{VoteGranted: false, Term: n.term}, nil
	}

	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		n.role = Follower
	}

	// node3 always votes for others, never itself
	if n.id == "node3" && req.CandidateId == n.id {
		return &pb.RequestVoteResponse{VoteGranted: false, Term: n.term}, nil
	}

	voteGranted := false
	if n.votedFor == "" || n.votedFor == req.CandidateId {
		voteGranted = true
		n.votedFor = req.CandidateId
		n.resetElectionTimer()
		log.Printf("Node %s voted for %s in term %d", n.id, req.CandidateId, req.Term)
	}

	return &pb.RequestVoteResponse{VoteGranted: voteGranted, Term: n.term}, nil
}

func (n *Node) ReplicateState(ctx context.Context, req *pb.ReplicateStateRequest) (*pb.ReplicateStateResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term >= n.term {
		for _, auction := range req.Auctions {
			n.auctions[auction.AuctionId] = auction
		}
		log.Printf("Node %s replicated %d auctions from leader", n.id, len(req.Auctions))
	}

	return &pb.ReplicateStateResponse{Success: true}, nil
}

// --- Auction RPCs ---

func (n *Node) CreateAuction(ctx context.Context, req *pb.CreateAuctionRequest) (*pb.CreateAuctionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return &pb.CreateAuctionResponse{Success: false, Message: "Not the leader node"}, nil
	}

	auctionID := fmt.Sprintf("auction-%d", time.Now().UnixNano())
	auction := &pb.AuctionState{
		AuctionId:         auctionID,
		ItemName:          req.ItemName,
		StartingPrice:     req.StartingPrice,
		CurrentHighestBid: req.StartingPrice,
		HighestBidder:     "",
		CreatedAt:         time.Now().Unix(),
		EndTime:           time.Now().Add(time.Duration(req.DurationSeconds) * time.Second).Unix(),
		Status:            "ACTIVE",
		BidHistory:        []*pb.Bid{},
	}

	n.auctions[auctionID] = auction
	log.Printf("Created auction %s for item: %s", auctionID, req.ItemName)
	go n.replicateState()

	return &pb.CreateAuctionResponse{AuctionId: auctionID, Success: true, Message: "Auction created successfully"}, nil
}

func (n *Node) PlaceBid(ctx context.Context, req *pb.PlaceBidRequest) (*pb.PlaceBidResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return &pb.PlaceBidResponse{Success: false, Message: "Not the leader node"}, nil
	}

	auction, exists := n.auctions[req.AuctionId]
	if !exists {
		return &pb.PlaceBidResponse{Success: false, Message: "Auction not found"}, nil
	}

	if auction.Status != "ACTIVE" {
		return &pb.PlaceBidResponse{Success: false, Message: "Auction is not active"}, nil
	}

	if time.Now().Unix() > auction.EndTime {
		auction.Status = "CLOSED"
		return &pb.PlaceBidResponse{Success: false, Message: "Auction has ended"}, nil
	}

	if req.Amount <= auction.CurrentHighestBid {
		return &pb.PlaceBidResponse{Success: false, Message: "Bid must be higher than current highest bid", CurrentHighestBid: auction.CurrentHighestBid}, nil
	}

	auction.CurrentHighestBid = req.Amount
	auction.HighestBidder = req.BidderId
	auction.BidHistory = append(auction.BidHistory, &pb.Bid{
		BidderId:  req.BidderId,
		Amount:    req.Amount,
		Timestamp: time.Now().Unix(),
	})

	log.Printf("Bid placed on %s: %s bid $%.2f", req.AuctionId, req.BidderId, req.Amount)
	go n.replicateState()

	return &pb.PlaceBidResponse{Success: true, Message: "Bid placed successfully", CurrentHighestBid: req.Amount}, nil
}

func (n *Node) CloseAuction(ctx context.Context, req *pb.CloseAuctionRequest) (*pb.CloseAuctionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return &pb.CloseAuctionResponse{Success: false, Message: "Not the leader node"}, nil
	}

	auction, exists := n.auctions[req.AuctionId]
	if !exists {
		return &pb.CloseAuctionResponse{Success: false, Message: "Auction not found"}, nil
	}

	auction.Status = "CLOSED"
	log.Printf("Closed auction %s. Winner: %s with bid $%.2f", req.AuctionId, auction.HighestBidder, auction.CurrentHighestBid)
	go n.replicateState()

	return &pb.CloseAuctionResponse{Success: true, Message: "Auction closed successfully", WinnerId: auction.HighestBidder, WinningBid: auction.CurrentHighestBid}, nil
}

func (n *Node) GetAuctionStatus(ctx context.Context, req *pb.GetAuctionStatusRequest) (*pb.GetAuctionStatusResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	auction, exists := n.auctions[req.AuctionId]
	if !exists {
		return &pb.GetAuctionStatusResponse{}, fmt.Errorf("auction not found")
	}

	return &pb.GetAuctionStatusResponse{
		AuctionId:         auction.AuctionId,
		ItemName:          auction.ItemName,
		CurrentHighestBid: auction.CurrentHighestBid,
		HighestBidder:     auction.HighestBidder,
		Status:            auction.Status,
		EndTime:           auction.EndTime,
	}, nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: go run node.go <node-id> <port> [peer1] [peer2] ...")
	}

	nodeID := os.Args[1]
	port := ":" + os.Args[2]
	peers := os.Args[3:]

	node := NewNode(nodeID, port, peers)
	node.Start()
	select {}
}
