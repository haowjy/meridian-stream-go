package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mstream "github.com/haowjy/meridian-stream"
)

// Simulated database with slow writes
type SlowDB struct {
	mu     sync.Mutex
	blocks []string
}

func (db *SlowDB) Save(events []mstream.Event) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	fmt.Println("[DB] Starting slow persist (2 seconds)...")
	time.Sleep(2 * time.Second) // Simulate slow DB write

	db.blocks = append(db.blocks, fmt.Sprintf("block-%d", len(db.blocks)+1))
	fmt.Println("[DB] Persist complete!")
	return nil
}

func (db *SlowDB) GetBlocks() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	return append([]string{}, db.blocks...)
}

func main() {
	fmt.Println("=== Testing Race Condition Between Clear and Persist ===\n")

	db := &SlowDB{}

	// Test 1: WRONG - Race condition
	fmt.Println("--- Test 1: WRONG WAY (Clear before persist completes) ---")
	testWrongWay(db)

	fmt.Println("\n\n--- Test 2: CORRECT WAY (Using PersistAndClear) ---")
	testCorrectWay(db)
}

func testWrongWay(db *SlowDB) {
	registry := mstream.NewRegistry()

	workFunc := func(ctx context.Context, send func(mstream.Event)) error {
		for i := 1; i <= 5; i++ {
			data, _ := json.Marshal(map[string]int{"count": i})
			send(mstream.Event{
				ID:   fmt.Sprintf("%d", i),
				Type: "event",
				Data: data,
			})
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	}

	stream := mstream.NewStream("test-wrong", workFunc,
		mstream.WithCatchup(func(streamID, lastEventID string) ([]mstream.Event, error) {
			fmt.Printf("[CATCHUP] Querying DB for events after %s...\n", lastEventID)
			blocks := db.GetBlocks()
			fmt.Printf("[CATCHUP] Found %d blocks in DB\n", len(blocks))

			if len(blocks) == 0 {
				fmt.Println("[CATCHUP] ❌ DB is empty! Events were lost!")
			}

			// Return catchup events
			events := []mstream.Event{}
			for i, block := range blocks {
				events = append(events, mstream.Event{
					ID:   fmt.Sprintf("catchup-%d", i+1),
					Type: "catchup",
					Data: []byte(block),
				})
			}
			return events, nil
		}),
	)

	registry.Register(stream)
	stream.Start()

	// Wait for stream to generate events
	time.Sleep(1200 * time.Millisecond)

	// ❌ WRONG: Clear buffer, THEN persist (race condition)
	go func() {
		fmt.Println("\n[WRONG] Clearing buffer first...")
		stream.ClearBuffer()
		fmt.Printf("[WRONG] Buffer cleared. Size: %d\n\n", stream.BufferSize())

		// Simulate getting snapshot AFTER clear (too late!)
		events := stream.SnapshotBuffer()
		fmt.Printf("[WRONG] Persisting %d events (should be 5, but buffer was cleared!)\n", len(events))
		db.Save(events) // This will be empty!
	}()

	// Client reconnects while persist is happening
	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n[CLIENT] Reconnecting with Last-Event-ID: 3")
	catchupEvents := stream.GetCatchupEvents("3")
	fmt.Printf("[CLIENT] Received %d catchup events\n", len(catchupEvents))

	if len(catchupEvents) == 0 {
		fmt.Println("[CLIENT] ❌ LOST EVENTS - buffer was cleared and DB doesn't have them yet!")
	}

	time.Sleep(2 * time.Second) // Wait for persist to finish
}

func testCorrectWay(db *SlowDB) {
	db.blocks = nil // Reset DB

	registry := mstream.NewRegistry()

	workFunc := func(ctx context.Context, send func(mstream.Event)) error {
		for i := 1; i <= 5; i++ {
			data, _ := json.Marshal(map[string]int{"count": i})
			send(mstream.Event{
				ID:   fmt.Sprintf("%d", i),
				Type: "event",
				Data: data,
			})
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	}

	stream := mstream.NewStream("test-correct", workFunc,
		mstream.WithCatchup(func(streamID, lastEventID string) ([]mstream.Event, error) {
			fmt.Printf("[CATCHUP] Querying DB for events after %s...\n", lastEventID)
			blocks := db.GetBlocks()
			fmt.Printf("[CATCHUP] Found %d blocks in DB\n", len(blocks))

			if len(blocks) > 0 {
				fmt.Println("[CATCHUP] ✅ DB has the events!")
			}

			// Return catchup events
			events := []mstream.Event{}
			for i, block := range blocks {
				events = append(events, mstream.Event{
					ID:   fmt.Sprintf("catchup-%d", i+1),
					Type: "catchup",
					Data: []byte(block),
				})
			}
			return events, nil
		}),
	)

	registry.Register(stream)
	stream.Start()

	// Wait for stream to generate events
	time.Sleep(1200 * time.Millisecond)

	// ✅ CORRECT: Use PersistAndClear
	go func() {
		fmt.Println("\n[CORRECT] Using PersistAndClear...")
		err := stream.PersistAndClear(func(events []mstream.Event) error {
			fmt.Printf("[CORRECT] Persisting %d events first...\n", len(events))
			return db.Save(events)
		})
		if err != nil {
			fmt.Printf("[CORRECT] Error: %v\n", err)
		}
		fmt.Printf("[CORRECT] Buffer cleared after persist. Size: %d\n", stream.BufferSize())
	}()

	// Client reconnects while persist is happening
	time.Sleep(500 * time.Millisecond)
	fmt.Println("\n[CLIENT] Reconnecting with Last-Event-ID: 3")
	catchupEvents := stream.GetCatchupEvents("3")
	fmt.Printf("[CLIENT] Received %d catchup events\n", len(catchupEvents))

	if len(catchupEvents) > 0 {
		fmt.Println("[CLIENT] ✅ SUCCESS - got events from buffer (persist not complete yet)")
		for _, evt := range catchupEvents {
			fmt.Printf("  - Event %s\n", evt.ID)
		}
	}

	time.Sleep(2 * time.Second) // Wait for persist to finish

	// Now client reconnects AFTER buffer cleared
	fmt.Println("\n[CLIENT] Reconnecting again after buffer cleared...")
	catchupEvents = stream.GetCatchupEvents("3")
	fmt.Printf("[CLIENT] Received %d catchup events\n", len(catchupEvents))

	if len(catchupEvents) > 0 {
		fmt.Println("[CLIENT] ✅ SUCCESS - got events from DB!")
	}
}
