package rstream

import (
	"context"
	"testing"
	"time"
)

func TestRegistry_NewRegistry(t *testing.T) {
	registry := NewRegistry()

	if registry.streams == nil {
		t.Error("expected streams map to be initialized")
	}

	if registry.cleanupInterval != 1*time.Minute {
		t.Errorf("expected default cleanup interval 1 minute, got %v", registry.cleanupInterval)
	}

	if registry.retentionPeriod != 10*time.Minute {
		t.Errorf("expected default retention period 10 minutes, got %v", registry.retentionPeriod)
	}
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	registry := NewRegistry()

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	stream := NewStream("stream-1", workFunc)
	err := registry.Register(stream)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	retrieved := registry.Get("stream-1")
	if retrieved == nil {
		t.Error("expected to retrieve stream, got nil")
	}

	if retrieved.ID() != "stream-1" {
		t.Errorf("expected stream ID 'stream-1', got %s", retrieved.ID())
	}
}

func TestRegistry_RegisterDuplicate(t *testing.T) {
	registry := NewRegistry()

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	stream1 := NewStream("stream-1", workFunc)
	stream2 := NewStream("stream-1", workFunc) // Same ID

	err1 := registry.Register(stream1)
	if err1 != nil {
		t.Errorf("unexpected error on first register: %v", err1)
	}

	err2 := registry.Register(stream2)
	if err2 == nil {
		t.Error("expected error when registering duplicate stream ID")
	}
}

func TestRegistry_GetNonExistent(t *testing.T) {
	registry := NewRegistry()

	retrieved := registry.Get("non-existent")
	if retrieved != nil {
		t.Error("expected nil for non-existent stream")
	}
}

func TestRegistry_Remove(t *testing.T) {
	registry := NewRegistry()

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	stream := NewStream("stream-1", workFunc)
	registry.Register(stream)

	registry.Remove("stream-1")

	retrieved := registry.Get("stream-1")
	if retrieved != nil {
		t.Error("expected stream to be removed")
	}
}

func TestRegistry_Count(t *testing.T) {
	registry := NewRegistry()

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	// Register 3 streams
	for i := 1; i <= 3; i++ {
		stream := NewStream(string(rune(i+'0')), workFunc)
		registry.Register(stream)
	}

	count := registry.Count()
	if count != 3 {
		t.Errorf("expected 3 streams, got %d", count)
	}
}

func TestRegistry_CleanupCompleted(t *testing.T) {
	registry := NewRegistry(
		WithCleanupInterval(50*time.Millisecond),
		WithRetentionPeriod(100*time.Millisecond),
	)

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	// Create and complete a stream
	stream := NewStream("stream-1", workFunc)
	registry.Register(stream)
	stream.Start()
	time.Sleep(50 * time.Millisecond)

	// Verify it's registered
	if registry.Get("stream-1") == nil {
		t.Error("stream should be registered")
	}

	// Start cleanup goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.StartCleanup(ctx)

	// Wait for retention period + cleanup cycle
	time.Sleep(200 * time.Millisecond)

	// Stream should be cleaned up
	if registry.Get("stream-1") != nil {
		t.Error("completed stream should be cleaned up after retention period")
	}
}

func TestRegistry_CleanupRunning(t *testing.T) {
	registry := NewRegistry(
		WithCleanupInterval(50*time.Millisecond),
		WithRetentionPeriod(100*time.Millisecond),
	)

	workFunc := func(ctx context.Context, send func(Event)) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	// Create a long-running stream
	stream := NewStream("stream-1", workFunc)
	registry.Register(stream)
	stream.Start()

	// Start cleanup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.StartCleanup(ctx)

	// Wait past retention period
	time.Sleep(200 * time.Millisecond)

	// Running stream should NOT be cleaned up
	if registry.Get("stream-1") == nil {
		t.Error("running stream should not be cleaned up")
	}
}

func TestRegistry_ConcurrentOperations(t *testing.T) {
	registry := NewRegistry()

	workFunc := func(ctx context.Context, send func(Event)) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	// Concurrent registration
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			stream := NewStream(string(rune(id+'0')), workFunc)
			registry.Register(stream)
			done <- true
		}(i)
	}

	// Wait for all registrations
	for i := 0; i < 10; i++ {
		<-done
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(id int) {
			registry.Get(string(rune(id + '0')))
			done <- true
		}(i)
	}

	// Wait for all reads
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all registered
	count := registry.Count()
	if count != 10 {
		t.Errorf("expected 10 streams, got %d", count)
	}
}
