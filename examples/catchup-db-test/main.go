package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mstream "github.com/haowjy/meridian-stream-go"
)

// Simulated database storage
type TurnBlock struct {
	ID      string
	Type    string
	Content string
}

var database = []TurnBlock{}

func main() {
	fmt.Println("=== Catchup with Database Test ===\n")

	// Create registry
	registry := mstream.NewRegistry()

	// Create stream with catchup function
	workFunc := func(ctx context.Context, send func(mstream.Event)) error {
		// Simulate thinking phase (events 1-5)
		for i := 1; i <= 5; i++ {
			data, _ := json.Marshal(map[string]interface{}{
				"type":  "thinking",
				"delta": fmt.Sprintf("thinking chunk %d", i),
			})
			send(mstream.Event{
				ID:   fmt.Sprintf("%d", i),
				Type: "thinking_delta",
				Data: data,
			})
			time.Sleep(200 * time.Millisecond)
		}

		// Simulate text phase (events 6-10)
		for i := 6; i <= 10; i++ {
			data, _ := json.Marshal(map[string]interface{}{
				"type":  "text",
				"delta": fmt.Sprintf("text chunk %d", i),
			})
			send(mstream.Event{
				ID:   fmt.Sprintf("%d", i),
				Type: "text_delta",
				Data: data,
			})
			time.Sleep(200 * time.Millisecond)
		}

		return nil
	}

	streamID := "test-db-catchup-456"
	stream := mstream.NewStream(streamID, workFunc,
		// Set catchup function that queries "database"
		mstream.WithCatchup(func(sid string, lastEventID string) ([]mstream.Event, error) {
			fmt.Printf("\n[CATCHUP] Querying database for events after %s...\n", lastEventID)

			// Query database for turnblocks
			var catchupEvents []mstream.Event
			for i, block := range database {
				// Create aggregated catchup event
				data, _ := json.Marshal(map[string]interface{}{
					"type":    block.Type,
					"content": block.Content,
					"note":    "This is an aggregated block from database",
				})

				catchupEvents = append(catchupEvents, mstream.Event{
					ID:   fmt.Sprintf("catchup-%d", i+1),
					Type: "turnblock_catchup",
					Data: data,
				})
			}

			fmt.Printf("[CATCHUP] Returning %d aggregated events from database\n\n", len(catchupEvents))
			return catchupEvents, nil
		}),
	)

	registry.Register(stream)
	stream.Start()

	// Simulate Meridian's persistence workflow
	go func() {
		// Wait for thinking phase to complete
		time.Sleep(1500 * time.Millisecond)

		fmt.Println("\n[PERSIST] Saving thinking turnblock to database...")
		database = append(database, TurnBlock{
			ID:      "block-1",
			Type:    "thinking",
			Content: "Aggregated thinking: chunks 1-5",
		})

		// Clear buffer after persisting
		stream.ClearBuffer()
		fmt.Printf("[PERSIST] Buffer cleared. Current buffer size: %d\n", stream.BufferSize())

		// Wait for text phase to complete
		time.Sleep(1500 * time.Millisecond)

		fmt.Println("\n[PERSIST] Saving text turnblock to database...")
		database = append(database, TurnBlock{
			ID:      "block-2",
			Type:    "text",
			Content: "Aggregated text: chunks 6-10",
		})

		// Clear buffer again
		stream.ClearBuffer()
		fmt.Printf("[PERSIST] Buffer cleared. Current buffer size: %d\n", stream.BufferSize())
	}()

	// Client 1: Gets events 1-3, then disconnects
	fmt.Println("Client 1: Connecting early...")
	client1Chan := stream.AddClient("client1")

	receivedEvents := 0
	lastEventID := ""

	go func() {
		for event := range client1Chan {
			receivedEvents++
			lastEventID = event.ID
			fmt.Printf("Client 1: Received event %s (%s)\n", event.ID, event.Type)

			if receivedEvents == 3 {
				fmt.Println("Client 1: Disconnecting after 3 events...\n")
				stream.RemoveClient("client1")
				return
			}
		}
	}()

	// Wait for stream to complete
	time.Sleep(3 * time.Second)

	// Client 1 reconnects AFTER buffer was cleared
	fmt.Printf("\n=== Client Reconnection Test ===\n")
	fmt.Printf("Client 1: Reconnecting with Last-Event-ID: %s\n", lastEventID)
	fmt.Printf("Buffer now has %d events (was cleared after DB persist)\n", stream.BufferSize())

	// Simulate getting catchup events (what the handler does)
	catchupEvents := stream.GetCatchupEvents(lastEventID)
	fmt.Printf("\nClient 1: Received %d total events for catchup:\n", len(catchupEvents))
	for _, event := range catchupEvents {
		fmt.Printf("  - Event %s (%s)\n", event.ID, event.Type)
		if event.Type == "turnblock_catchup" {
			fmt.Printf("    Data: %s\n", string(event.Data))
		}
	}

	fmt.Printf("\n=== Final State ===\n")
	fmt.Printf("Stream status: %s\n", stream.Status())
	fmt.Printf("Buffer size: %d events\n", stream.BufferSize())
	fmt.Printf("Database has %d turnblocks\n", len(database))
}
