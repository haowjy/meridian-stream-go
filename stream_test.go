package rstream

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStream_NewStream(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	stream := NewStream("test-1", workFunc)

	if stream.id != "test-1" {
		t.Errorf("expected id 'test-1', got %s", stream.id)
	}

	if stream.Status() != StatusPending {
		t.Errorf("expected status pending, got %s", stream.Status())
	}

	if stream.bufferSize != 20 {
		t.Errorf("expected default buffer size 20, got %d", stream.bufferSize)
	}
}

func TestStream_StartAndComplete(t *testing.T) {
	eventCount := 0
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 0; i < 5; i++ {
			send(NewEvent([]byte("test")).WithID(string(rune(i + '0'))))
			eventCount++
		}
		return nil
	}

	stream := NewStream("test-2", workFunc)
	stream.Start()

	// Wait for completion
	time.Sleep(100 * time.Millisecond)

	if stream.Status() != StatusComplete {
		t.Errorf("expected status complete, got %s", stream.Status())
	}

	if eventCount != 5 {
		t.Errorf("expected 5 events sent, got %d", eventCount)
	}
}

func TestStream_MultipleClients(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 0; i < 10; i++ {
			send(NewEvent([]byte("test")).WithID(string(rune(i + '0'))))
			time.Sleep(10 * time.Millisecond)
		}
		// Small delay to ensure all events are consumed before channel closes
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	stream := NewStream("test-3", workFunc)

	// Add 3 clients BEFORE starting stream
	var wg sync.WaitGroup
	clientCounts := make([]int, 3)
	clientsReady := make(chan bool, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		clientID := i
		go func() {
			defer wg.Done()
			ch := stream.AddClient(string(rune(clientID + 'A')))
			clientsReady <- true // Signal client is registered
			for range ch {
				clientCounts[clientID]++
			}
		}()
	}

	// Wait for all clients to register before starting stream
	for i := 0; i < 3; i++ {
		<-clientsReady
	}

	// Now start stream - all clients are registered
	stream.Start()

	wg.Wait()

	// All clients should receive all events
	for i, count := range clientCounts {
		if count != 10 {
			t.Errorf("client %d received %d events, expected 10", i, count)
		}
	}
}

func TestStream_Cancel(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				send(NewEvent([]byte("test")))
				time.Sleep(10 * time.Millisecond)
			}
		}
		return nil
	}

	stream := NewStream("test-4", workFunc)
	stream.Start()

	time.Sleep(50 * time.Millisecond)
	stream.Cancel()
	time.Sleep(50 * time.Millisecond)

	status := stream.Status()
	if status != StatusError && status != StatusCancelled {
		t.Errorf("expected status error or cancelled, got %s", status)
	}
}

func TestStream_GetCatchupEvents_BufferOnly(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 1; i <= 5; i++ {
			send(Event{
				ID:   string(rune(i + '0')),
				Data: []byte("test"),
			})
		}
		return nil
	}

	stream := NewStream("test-5", workFunc)
	stream.Start()
	time.Sleep(100 * time.Millisecond)

	// Event in buffer - GetCatchupEvents should find it and return events after it
	events := stream.GetCatchupEvents("2")
	if len(events) != 3 {
		t.Errorf("expected 3 events from buffer, got %d", len(events))
	}
}

func TestStream_GetCatchupEvents_WithCatchupFunc(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		// Send first batch
		for i := 1; i <= 3; i++ {
			send(Event{
				ID:   string(rune(i + '0')),
				Data: []byte("batch1"),
			})
			time.Sleep(20 * time.Millisecond)
		}
		// Wait before sending second batch
		time.Sleep(100 * time.Millisecond)
		// Send second batch
		for i := 4; i <= 6; i++ {
			send(Event{
				ID:   string(rune(i + '0')),
				Data: []byte("batch2"),
			})
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}

	catchupCalled := false
	stream := NewStream("test-6", workFunc,
		WithCatchup(func(streamID, lastEventID string) ([]Event, error) {
			catchupCalled = true
			return []Event{
				{ID: "catchup-1", Data: []byte("old")},
				{ID: "catchup-2", Data: []byte("old")},
			}, nil
		}),
	)

	stream.Start()
	time.Sleep(100 * time.Millisecond) // Wait for first batch

	// Clear buffer to force catchup
	stream.ClearBuffer()

	// Wait for second batch to start
	time.Sleep(50 * time.Millisecond)

	// Request events not in buffer
	events := stream.GetCatchupEvents("0")

	if !catchupCalled {
		t.Error("catchup function was not called")
	}

	// Should have: 2 catchup + at least some from batch2
	if len(events) < 2 {
		t.Errorf("expected at least 2 catchup events, got %d", len(events))
	}

	if events[0].ID != "catchup-1" {
		t.Errorf("expected first event ID 'catchup-1', got %s", events[0].ID)
	}

	if events[1].ID != "catchup-2" {
		t.Errorf("expected second event ID 'catchup-2', got %s", events[1].ID)
	}
}

func TestStream_PersistAndClear_Success(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 1; i <= 5; i++ {
			send(Event{ID: string(rune(i + '0')), Data: []byte("test")})
		}
		return nil
	}

	stream := NewStream("test-7", workFunc)
	stream.Start()
	time.Sleep(100 * time.Millisecond)

	persistedCount := 0
	err := stream.PersistAndClear(func(events []Event) error {
		persistedCount = len(events)
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if persistedCount != 5 {
		t.Errorf("expected to persist 5 events, got %d", persistedCount)
	}

	if stream.BufferSize() != 0 {
		t.Errorf("expected buffer cleared, got size %d", stream.BufferSize())
	}
}

func TestStream_PersistAndClear_Error(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		send(Event{ID: "1", Data: []byte("test")})
		return nil
	}

	stream := NewStream("test-8", workFunc)
	stream.Start()
	time.Sleep(100 * time.Millisecond)

	// Persist fails
	err := stream.PersistAndClear(func(events []Event) error {
		return context.DeadlineExceeded
	})

	if err == nil {
		t.Error("expected error from persist function")
	}

	// Buffer should NOT be cleared on error
	if stream.BufferSize() == 0 {
		t.Error("buffer should not be cleared when persist fails")
	}
}

func TestStream_ConcurrentAccess(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 0; i < 100; i++ {
			send(Event{ID: string(rune(i + '0')), Data: []byte("test")})
			time.Sleep(1 * time.Millisecond)
		}
		return nil
	}

	stream := NewStream("test-9", workFunc)
	stream.Start()

	var wg sync.WaitGroup

	// Test concurrent client operations (Stream-specific behavior)
	// Multiple clients reading simultaneously
	clientCounts := make([]int, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch := stream.AddClient(string(rune(id + 'A')))
			count := 0
			for range ch {
				count++
			}
			clientCounts[id] = count
		}(i)
	}

	// Concurrent AddClient/RemoveClient operations
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientID := string(rune(id + 100))
			ch := stream.AddClient(clientID)
			time.Sleep(10 * time.Millisecond)
			for range ch {
				// Consume a few events
			}
			stream.RemoveClient(clientID)
		}(i)
	}

	wg.Wait()

	// Verify all clients received events (exact count may vary due to timing)
	for i, count := range clientCounts {
		if count == 0 {
			t.Errorf("client %d received 0 events, expected some", i)
		}
	}
}

func TestStream_Timeout(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				send(NewEvent([]byte("test")))
				time.Sleep(50 * time.Millisecond)
			}
		}
		return nil
	}

	stream := NewStream("test-10", workFunc,
		WithTimeout(200*time.Millisecond),
	)
	stream.Start()

	time.Sleep(300 * time.Millisecond)

	if stream.Status() != StatusError {
		t.Errorf("expected status error due to timeout, got %s", stream.Status())
	}
}

func TestStream_OnCompleteHook(t *testing.T) {
	completeCalled := false
	var completedID string

	workFunc := func(ctx context.Context, send func(Event)) error {
		send(NewEvent([]byte("test")))
		return nil
	}

	stream := NewStream("test-11", workFunc,
		WithOnComplete(func(id string) {
			completeCalled = true
			completedID = id
		}),
	)

	stream.Start()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled {
		t.Error("onComplete hook was not called")
	}

	if completedID != "test-11" {
		t.Errorf("expected stream ID 'test-11', got %s", completedID)
	}
}

func TestStream_OnErrorHook(t *testing.T) {
	errorCalled := false
	var errorID string

	workFunc := func(ctx context.Context, send func(Event)) error {
		return context.DeadlineExceeded
	}

	stream := NewStream("test-12", workFunc,
		WithOnError(func(id string, err error) {
			errorCalled = true
			errorID = id
		}),
	)

	stream.Start()
	time.Sleep(100 * time.Millisecond)

	if !errorCalled {
		t.Error("onError hook was not called")
	}

	if errorID != "test-12" {
		t.Errorf("expected stream ID 'test-12', got %s", errorID)
	}
}
