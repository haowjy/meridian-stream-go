package main

import (
	"context"
	"fmt"
	"time"

	mstream "github.com/haowjy/meridian-stream"
)

func main() {
	fmt.Println("=== Simple Streaming Example ===\n")

	// Create a stream with simple counter logic
	stream := mstream.NewStream("demo-stream", func(ctx context.Context, send func(mstream.Event)) error {
		fmt.Println("Stream started, sending 10 events...")

		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Send event
				event := mstream.NewEvent([]byte(fmt.Sprintf("Event %d", i))).
					WithID(fmt.Sprintf("%d", i)).
					WithType("counter")

				send(event)
				fmt.Printf("Sent: Event %d\n", i)

				time.Sleep(500 * time.Millisecond)
			}
		}

		fmt.Println("Stream completed!")
		return nil
	})

	// Start the stream
	stream.Start()

	// Connect a client
	clientID := "client-1"
	eventChan := stream.AddClient(clientID)

	fmt.Printf("\nClient '%s' connected\n\n", clientID)

	// Read events
	for event := range eventChan {
		fmt.Printf("Client received: %s (ID: %s, Type: %s)\n", event.Data, event.ID, event.Type)
	}

	fmt.Printf("\nFinal status: %s\n", stream.Status())
	fmt.Printf("Client count: %d\n", stream.ClientCount())
}
