package rstream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Integration test: Full workflow with multiple concurrent streams and clients
func TestIntegration_MultiStreamMultiClient(t *testing.T) {
	registry := NewRegistry()

	// Start cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.StartCleanup(ctx)

	// Create 3 streams
	numStreams := 3
	streamsCreated := make([]string, numStreams)

	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		streamsCreated[i] = streamID

		workFunc := func(streamIdx int) WorkFunc {
			return func(ctx context.Context, send func(Event)) error {
				for j := 0; j < 10; j++ {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						send(Event{
							ID:   fmt.Sprintf("%d-%d", streamIdx, j),
							Type: "event",
							Data: []byte(fmt.Sprintf("stream %d, event %d", streamIdx, j)),
						})
						time.Sleep(10 * time.Millisecond)
					}
				}
				// Delay before completing to ensure clients finish reading
				time.Sleep(50 * time.Millisecond)
				return nil
			}
		}(i)

		stream := NewStream(streamID, workFunc)
		if err := registry.Register(stream); err != nil {
			t.Fatalf("failed to register stream %s: %v", streamID, err)
		}
		// Don't start yet - wait for clients to connect
	}

	// Each stream has 2 clients
	var wg sync.WaitGroup
	clientResults := make(map[string]int)
	var resultsMu sync.Mutex
	clientsReady := make(chan bool, numStreams*2) // 3 streams × 2 clients = 6

	for i := 0; i < numStreams; i++ {
		streamID := streamsCreated[i]
		stream := registry.Get(streamID)

		if stream == nil {
			t.Fatalf("stream %s not found", streamID)
		}

		for c := 0; c < 2; c++ {
			clientID := fmt.Sprintf("%s-client-%d", streamID, c)
			wg.Add(1)

			go func(sid, cid string, s *Stream) {
				defer wg.Done()

				ch := s.AddClient(cid)
				clientsReady <- true // Signal client is registered
				count := 0
				for range ch {
					count++
				}

				resultsMu.Lock()
				clientResults[cid] = count
				resultsMu.Unlock()
			}(streamID, clientID, stream)
		}
	}

	// Wait for all clients to register before starting streams
	for i := 0; i < numStreams*2; i++ {
		<-clientsReady
	}

	// Now start all streams after clients are connected
	for i := 0; i < numStreams; i++ {
		stream := registry.Get(streamsCreated[i])
		stream.Start()
	}

	// Wait for all clients to finish
	wg.Wait()

	// Verify results
	expectedEventsPerClient := 10
	for clientID, count := range clientResults {
		if count != expectedEventsPerClient {
			t.Errorf("client %s received %d events, expected %d", clientID, count, expectedEventsPerClient)
		}
	}

	// Verify all streams completed
	for _, streamID := range streamsCreated {
		stream := registry.Get(streamID)
		if stream == nil {
			t.Errorf("stream %s disappeared from registry", streamID)
			continue
		}

		if stream.Status() != StatusComplete {
			t.Errorf("stream %s has status %s, expected complete", streamID, stream.Status())
		}
	}
}

// Integration test: Reconnection workflow with catchup
func TestIntegration_ReconnectionWithCatchup(t *testing.T) {
	// Simulated database
	var database []Event
	var dbMu sync.Mutex

	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 1; i <= 20; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				send(Event{
					ID:   fmt.Sprintf("%d", i),
					Type: "message",
					Data: []byte(fmt.Sprintf("event %d", i)),
				})
				time.Sleep(10 * time.Millisecond)
			}
		}
		return nil
	}

	stream := NewStream("test-stream", workFunc,
		WithCatchup(func(streamID, lastEventID string) ([]Event, error) {
			dbMu.Lock()
			defer dbMu.Unlock()

			var catchupEvents []Event
			for i, event := range database {
				catchupEvents = append(catchupEvents, Event{
					ID:   fmt.Sprintf("catchup-%d", i),
					Type: "catchup",
					Data: []byte(fmt.Sprintf("aggregated: %s", event.Data)),
				})
			}
			return catchupEvents, nil
		}),
	)

	stream.Start()

	// Client 1: Get first 5 events, then disconnect
	client1Chan := stream.AddClient("client1")
	client1Events := []Event{}

	for i := 0; i < 5; i++ {
		event := <-client1Chan
		client1Events = append(client1Events, event)
	}
	stream.RemoveClient("client1")

	lastEventID := client1Events[len(client1Events)-1].ID

	// Wait for more events and persist to "database"
	time.Sleep(100 * time.Millisecond)

	dbMu.Lock()
	snapshot := stream.SnapshotBuffer()
	database = append(database, snapshot...)
	dbMu.Unlock()

	stream.ClearBuffer()

	// Client 1 reconnects with Last-Event-ID
	catchupEvents := stream.GetCatchupEvents(lastEventID)

	if len(catchupEvents) == 0 {
		t.Error("expected catchup events, got none")
	}

	// Verify catchup events came from database
	foundCatchup := false
	for _, event := range catchupEvents {
		if event.Type == "catchup" {
			foundCatchup = true
			break
		}
	}

	if !foundCatchup {
		t.Error("expected to find catchup events from database")
	}

	// Wait for stream to complete
	time.Sleep(200 * time.Millisecond)

	if stream.Status() != StatusComplete {
		t.Errorf("expected stream to complete, got status %s", stream.Status())
	}
}

// Integration test: PersistAndClear with concurrent reconnections
func TestIntegration_PersistAndClearRace(t *testing.T) {
	var persistedData []Event
	var persistMu sync.Mutex

	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 1; i <= 50; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				send(Event{
					ID:   fmt.Sprintf("%d", i),
					Data: []byte(fmt.Sprintf("event %d", i)),
				})
				time.Sleep(5 * time.Millisecond)
			}
		}
		return nil
	}

	stream := NewStream("test-stream", workFunc,
		WithCatchup(func(streamID, lastEventID string) ([]Event, error) {
			persistMu.Lock()
			defer persistMu.Unlock()

			return []Event{
				{ID: "catchup-1", Data: []byte("aggregated events")},
			}, nil
		}),
	)

	stream.Start()

	var wg sync.WaitGroup

	// Goroutine 1: Periodic persist and clear
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			time.Sleep(80 * time.Millisecond)

			err := stream.PersistAndClear(func(events []Event) error {
				persistMu.Lock()
				persistedData = append(persistedData, events...)
				persistMu.Unlock()
				time.Sleep(20 * time.Millisecond) // Simulate slow DB
				return nil
			})

			if err != nil {
				t.Errorf("persist error: %v", err)
			}
		}
	}()

	// Goroutines 2-4: Clients reconnecting with catchup
	for c := 0; c < 3; c++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()

			for i := 0; i < 5; i++ {
				time.Sleep(50 * time.Millisecond)

				// Simulate reconnection
				lastID := fmt.Sprintf("%d", i*10)
				events := stream.GetCatchupEvents(lastID)

				// Should always get events (either from buffer or catchup)
				if len(events) == 0 && stream.Status() == StatusRunning {
					// Only error if stream is still running
					t.Errorf("client %d got 0 events during reconnect %d", clientNum, i)
				}
			}
		}(c)
	}

	wg.Wait()

	// Verify data was persisted
	persistMu.Lock()
	totalPersisted := len(persistedData)
	persistMu.Unlock()

	if totalPersisted == 0 {
		t.Error("expected some data to be persisted")
	}

	// Verify stream completed
	if stream.Status() != StatusComplete {
		t.Errorf("expected stream to complete, got status %s", stream.Status())
	}
}

// Integration test: High concurrency stress test
func TestIntegration_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	registry := NewRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.StartCleanup(ctx)

	numStreams := 20
	clientsPerStream := 5

	var wg sync.WaitGroup

	// Create many streams
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)

		workFunc := func(ctx context.Context, send func(Event)) error {
			for j := 0; j < 20; j++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					send(NewEvent([]byte("data")))
					time.Sleep(5 * time.Millisecond)
				}
			}
			return nil
		}

		stream := NewStream(streamID, workFunc)
		registry.Register(stream)
		stream.Start()

		// Add multiple clients per stream
		for c := 0; c < clientsPerStream; c++ {
			wg.Add(1)
			go func(s *Stream) {
				defer wg.Done()
				ch := s.AddClient(fmt.Sprintf("client-%d", c))
				for range ch {
					// Consume
				}
			}(stream)
		}
	}

	// Wait for all to complete
	wg.Wait()

	// Verify all completed
	completedCount := 0
	for i := 0; i < numStreams; i++ {
		streamID := fmt.Sprintf("stream-%d", i)
		stream := registry.Get(streamID)
		if stream != nil && stream.Status() == StatusComplete {
			completedCount++
		}
	}

	if completedCount != numStreams {
		t.Errorf("expected %d completed streams, got %d", numStreams, completedCount)
	}
}
