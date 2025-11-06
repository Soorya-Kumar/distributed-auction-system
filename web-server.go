package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
)

type WebServer struct {
	nodeAddresses []string
}

func NewWebServer(nodeAddresses []string) *WebServer {
	return &WebServer{
		nodeAddresses: nodeAddresses,
	}
}
func (ws *WebServer) getClient() (pb.AuctionServiceClient, *grpc.ClientConn, error) {
	nodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}

	for _, addr := range nodes {
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Second))
		if err == nil {
			client := pb.NewAuctionServiceClient(conn)
			return client, conn, nil
		}
	}
	return nil, nil, fmt.Errorf("Could not connect to any node")
}

func (ws *WebServer) handleCreateAuction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	itemName := r.FormValue("itemName")
	startingPrice, _ := strconv.ParseFloat(r.FormValue("startingPrice"), 64)
	duration, _ := strconv.ParseInt(r.FormValue("duration"), 10, 64)

	// Try each node directly
	var lastErr error
	for _, address := range ws.nodeAddresses {
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			lastErr = fmt.Errorf("connection failed: %v", err)
			continue
		}
		defer conn.Close()

		client := pb.NewAuctionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.CreateAuction(ctx, &pb.CreateAuctionRequest{
			ItemName:        itemName,
			StartingPrice:   startingPrice,
			DurationSeconds: duration,
		})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("request failed: %v", err)
			continue
		}

		if resp.Success {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":   true,
				"auctionId": resp.AuctionId,
				"message":   "Auction created successfully",
			})
			return
		}

		lastErr = fmt.Errorf(resp.Message)
		// If not the leader, try next node
		if resp.Message == "Not the leader node" {
			log.Printf("Node %s is not leader, trying next node...", address)
			continue
		}
		// For other errors, stop trying
		break
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": fmt.Sprintf("All nodes tried. Last error: %v", lastErr),
	})
}

func (ws *WebServer) handlePlaceBid(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	auctionID := r.FormValue("auctionId")
	bidderID := r.FormValue("bidderId")
	amount, _ := strconv.ParseFloat(r.FormValue("amount"), 64)

	// Try each node directly
	var lastErr error
	for _, address := range ws.nodeAddresses {
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			lastErr = fmt.Errorf("connection failed: %v", err)
			continue
		}
		defer conn.Close()

		client := pb.NewAuctionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.PlaceBid(ctx, &pb.PlaceBidRequest{
			AuctionId: auctionID,
			BidderId:  bidderID,
			Amount:    amount,
		})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("request failed: %v", err)
			continue
		}

		if resp.Success {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":           true,
				"currentHighestBid": resp.CurrentHighestBid,
				"message":           "Bid placed successfully",
			})
			return
		}

		lastErr = fmt.Errorf(resp.Message)
		if resp.Message == "Not the leader node" {
			continue
		}
		break
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": fmt.Sprintf("%v", lastErr),
	})
}

func (ws *WebServer) handleGetStatus(w http.ResponseWriter, r *http.Request) {
	auctionID := r.URL.Query().Get("auctionId")

	client, conn, err := ws.getClient()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": "Could not connect to any node",
		})
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetAuctionStatus(ctx, &pb.GetAuctionStatusRequest{
		AuctionId: auctionID,
	})

	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":           true,
		"auctionId":         resp.AuctionId,
		"itemName":          resp.ItemName,
		"currentHighestBid": resp.CurrentHighestBid,
		"highestBidder":     resp.HighestBidder,
		"status":            resp.Status,
		"endTime":           resp.EndTime,
	})
}

func (ws *WebServer) handleCloseAuction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	auctionID := r.FormValue("auctionId")

	// Try each node directly
	var lastErr error
	for _, address := range ws.nodeAddresses {
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			lastErr = fmt.Errorf("connection failed: %v", err)
			continue
		}
		defer conn.Close()

		client := pb.NewAuctionServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.CloseAuction(ctx, &pb.CloseAuctionRequest{
			AuctionId: auctionID,
		})
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("request failed: %v", err)
			continue
		}

		if resp.Success {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":    true,
				"winnerId":   resp.WinnerId,
				"winningBid": resp.WinningBid,
				"message":    "Auction closed successfully",
			})
			return
		}

		lastErr = fmt.Errorf(resp.Message)
		if resp.Message == "Not the leader node" {
			continue
		}
		break
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"message": fmt.Sprintf("%v", lastErr),
	})
}

func main() {
	nodes := []string{"localhost:8001", "localhost:8002", "localhost:8003"}
	ws := NewWebServer(nodes)

	// Serve static HTML
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// API endpoints
	http.HandleFunc("/api/create", ws.handleCreateAuction)
	http.HandleFunc("/api/bid", ws.handlePlaceBid)
	http.HandleFunc("/api/status", ws.handleGetStatus)
	http.HandleFunc("/api/close", ws.handleCloseAuction)

	log.Println("Web server starting on http://localhost:8080")
	log.Println("Open your browser and go to: http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
