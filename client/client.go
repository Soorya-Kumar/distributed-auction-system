package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "auction/proto"

	"google.golang.org/grpc"
)

type AuctionClient struct {
	nodeAddresses []string
	currentLeader int
}

func NewAuctionClient(nodeAddresses []string) *AuctionClient {
	return &AuctionClient{
		nodeAddresses: nodeAddresses,
		currentLeader: 0,
	}
}

func (c *AuctionClient) getClient() (pb.AuctionServiceClient, *grpc.ClientConn, error) {
	// Try to connect to nodes starting with the current leader
	for i := 0; i < len(c.nodeAddresses); i++ {
		idx := (c.currentLeader + i) % len(c.nodeAddresses)
		address := c.nodeAddresses[idx]

		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
		if err != nil {
			log.Printf("Failed to connect to %s: %v", address, err)
			continue
		}

		c.currentLeader = idx
		return pb.NewAuctionServiceClient(conn), conn, nil
	}

	return nil, nil, fmt.Errorf("could not connect to any node")
}

func (c *AuctionClient) CreateAuction(itemName string, startingPrice float64, durationSec int64) (string, error) {
	client, conn, err := c.getClient()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CreateAuction(ctx, &pb.CreateAuctionRequest{
		ItemName:        itemName,
		StartingPrice:   startingPrice,
		DurationSeconds: durationSec,
	})

	if err != nil {
		return "", err
	}

	if !resp.Success {
		if resp.Message == "Not the leader node" {
			// Try next node
			c.currentLeader = (c.currentLeader + 1) % len(c.nodeAddresses)
			return c.CreateAuction(itemName, startingPrice, durationSec)
		}
		return "", fmt.Errorf("failed to create auction: %s", resp.Message)
	}

	return resp.AuctionId, nil
}

func (c *AuctionClient) PlaceBid(auctionID, bidderID string, amount float64) error {
	client, conn, err := c.getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.PlaceBid(ctx, &pb.PlaceBidRequest{
		AuctionId: auctionID,
		BidderId:  bidderID,
		Amount:    amount,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		if resp.Message == "Not the leader node" {
			// Try next node
			c.currentLeader = (c.currentLeader + 1) % len(c.nodeAddresses)
			return c.PlaceBid(auctionID, bidderID, amount)
		}
		return fmt.Errorf("failed to place bid: %s", resp.Message)
	}

	fmt.Printf("Bid placed successfully! Current highest bid: $%.2f\n", resp.CurrentHighestBid)
	return nil
}

func (c *AuctionClient) CloseAuction(auctionID string) error {
	client, conn, err := c.getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.CloseAuction(ctx, &pb.CloseAuctionRequest{
		AuctionId: auctionID,
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		if resp.Message == "Not the leader node" {
			// Try next node
			c.currentLeader = (c.currentLeader + 1) % len(c.nodeAddresses)
			return c.CloseAuction(auctionID)
		}
		return fmt.Errorf("failed to close auction: %s", resp.Message)
	}

	fmt.Printf("Auction closed! Winner: %s with bid: $%.2f\n", resp.WinnerId, resp.WinningBid)
	return nil
}

func (c *AuctionClient) GetStatus(auctionID string) error {
	client, conn, err := c.getClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetAuctionStatus(ctx, &pb.GetAuctionStatusRequest{
		AuctionId: auctionID,
	})

	if err != nil {
		return err
	}

	fmt.Printf("\n=== Auction Status ===\n")
	fmt.Printf("Auction ID: %s\n", resp.AuctionId)
	fmt.Printf("Item: %s\n", resp.ItemName)
	fmt.Printf("Current Highest Bid: $%.2f\n", resp.CurrentHighestBid)
	fmt.Printf("Highest Bidder: %s\n", resp.HighestBidder)
	fmt.Printf("Status: %s\n", resp.Status)
	fmt.Printf("End Time: %s\n", time.Unix(resp.EndTime, 0).Format("2006-01-02 15:04:05"))
	fmt.Printf("====================\n\n")

	return nil
}
