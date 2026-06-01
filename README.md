# Distributed Auction System

A distributed auction system implemented in Go using gRPC for inter-node and client-node communication, along with a lightweight web interface. The project demonstrates key distributed systems concepts such as leader election, state replication, and fault-tolerant auction management.

## Features

- Leader-based auction lifecycle management
- Create, bid on, and close auctions
- Distributed leader election
- Heartbeat-based node monitoring
- State replication across cluster nodes
- gRPC communication between nodes
- Web-based user interface
- Programmatic client abstraction
- Educational implementation inspired by Raft consensus concepts

## Architecture Overview

The system consists of multiple auction nodes forming a distributed cluster.

### Cluster Nodes

Each node:

- Participates in leader election
- Maintains auction state
- Sends and receives heartbeats
- Replicates auction data across peers

### Leader Responsibilities

Only the elected leader can:

- Create auctions
- Accept bids
- Close auctions
- Replicate updates to follower nodes

### Web Server

The web server acts as a gateway between users and the cluster by:

- Serving the frontend UI
- Forwarding requests through gRPC
- Returning auction results to users

## Project Structure

```text
distributed-auction-system/
│
├── auction.proto           # gRPC service definitions
├── node.go                 # Cluster node implementation
├── web-server.go           # HTTP server and API gateway
├── client.go               # Client abstraction
├── index.html              # Web UI
├── start-cluster.bat       # Windows startup script
├── proto/
│   ├── auction.pb.go
│   └── auction_grpc.pb.go
└── go.mod
```


## gRPC Services
### AuctionService

```protobuf
CreateAuction
PlaceBid
CloseAuction
GetAuctionStatus
```

### NodeService

```protobuf
Heartbeat
ReplicateState
RequestVote
```

## HTTP API Endpoints

### Create Auction

```http
POST /api/create
```

### Place Bid

```http
POST /api/bid
```

### Get Auction Status

```http
GET /api/status?auctionId=<id>
```

### Close Auction

```http
POST /api/close
```

## Running the Project
### Quick Start (Windows)

Run:

```bash
start-cluster.bat
```

This will:

1. Start three cluster nodes
2. Launch the web server
3. Open the web interface

### Manual Setup

Start Node 1:

```bash
go run node.go node1 8001 localhost:8002 localhost:8003
```

Start Node 2:

```bash
go run node.go node2 8002 localhost:8001 localhost:8003
```

Start Node 3:

```bash
go run node.go node3 8003 localhost:8001 localhost:8002
```

Start the Web Server:

```bash
go run web-server.go
```

Open:

```text
http://localhost:8080
```

## Cluster Configuration

Default node addresses:

```text
localhost:8001
localhost:8002
localhost:8003
```

The web server communicates with these nodes via gRPC.

## Limitations

- In-memory state only (no persistence)
- Simplified consensus mechanism
- No crash recovery
- No distributed log replication
- Limited fault tolerance
- Intended for educational and demonstration purposes

## Protocol Buffers

API contracts are defined in:

```text
auction.proto
```

Generated files:

```text
proto/auction.pb.go
proto/auction_grpc.pb.go
```