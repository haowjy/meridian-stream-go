package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	mstream "github.com/haowjy/meridian-stream-go"
)

func main() {
	fmt.Println("=== Catchup Test ===\n")

	// Create registry
	registry := mstream.NewRegistry()

	// Create stream with slow events
	workFunc := func(ctx context.Context, send func(mstream.Event)) error {
		for i := 1; i <= 10; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				data, _ := json.Marshal(map[string]interface{}{
					"count": i,
					"time":  time.Now().Format(time.RFC3339),
				})

				event := mstream.Event{
					ID:   fmt.Sprintf("%d", i),
					Type: "counter",
					Data: data,
				}

				send(event)
				time.Sleep(500 * time.Millisecond)
			}
		}
		return nil
	}

	streamID := "test-catchup-123"
	stream := mstream.NewStream(streamID, workFunc)
	registry.Register(stream)
	stream.Start()

	// Client 1: Connect early, get events 1-3, then disconnect
	fmt.Println("Client 1: Connecting...")
	client1Chan := stream.AddClient("client1")

	receivedEvents := 0
	lastEventID := ""

	go func() {
		for event := range client1Chan {
			receivedEvents++
			lastEventID = event.ID
			fmt.Printf("Client 1: Received event %s\n", event.ID)

			if receivedEvents == 3 {
				fmt.Println("Client 1: Disconnecting after 3 events...")
				stream.RemoveClient("client1")
				return
			}
		}
	}()

	// Wait for client 1 to receive 3 events and disconnect
	time.Sleep(2 * time.Second)

	fmt.Printf("\nClient 1 disconnected. Last event received: %s\n", lastEventID)
	fmt.Printf("Stream buffer now has %d events\n\n", stream.BufferSize())

	// Wait a bit more so stream generates more events
	time.Sleep(2 * time.Second)

	// Client 1 reconnects with Last-Event-ID
	fmt.Printf("Client 1: Reconnecting with Last-Event-ID: %s\n", lastEventID)

	// Get catchup events
	catchupEvents := stream.GetEventsSince(lastEventID)
	fmt.Printf("Client 1: Received %d catchup events:\n", len(catchupEvents))
	for _, event := range catchupEvents {
		fmt.Printf("  - Event %s (from catchup buffer)\n", event.ID)
	}

	// Now connect to live stream
	client1Chan = stream.AddClient("client1-reconnected")
	fmt.Println("\nClient 1: Now receiving live events...")

	go func() {
		for event := range client1Chan {
			fmt.Printf("Client 1: Received event %s (live)\n", event.ID)
		}
		fmt.Println("Client 1: Stream complete")
	}()

	// Wait for stream to complete
	time.Sleep(4 * time.Second)

	fmt.Printf("\n=== Final State ===\n")
	fmt.Printf("Stream status: %s\n", stream.Status())
	fmt.Printf("Buffer size: %d events\n", stream.BufferSize())

	// Simulate clearing buffer after persisting to database
	fmt.Println("\nSimulating: Persisting events to database...")
	stream.ClearBuffer()
	fmt.Printf("Buffer cleared. New size: %d\n", stream.BufferSize())
}
