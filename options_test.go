package rstream

import (
	"context"
	"testing"
	"time"
)

func TestWithBufferSize(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	// Default buffer size
	stream := NewStream("test-default", workFunc)
	if stream.bufferSize != 20 {
		t.Errorf("expected default buffer size 20, got %d", stream.bufferSize)
	}

	// Custom buffer size
	stream = NewStream("test-custom", workFunc, WithBufferSize(100))
	if stream.bufferSize != 100 {
		t.Errorf("expected buffer size 100, got %d", stream.bufferSize)
	}
}

func TestWithTimeout(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	// Default timeout (zero)
	stream := NewStream("test-default", workFunc)
	if stream.timeout != 0 {
		t.Errorf("expected default timeout 0, got %v", stream.timeout)
	}

	// Custom timeout
	stream = NewStream("test-custom", workFunc, WithTimeout(5*time.Second))
	if stream.timeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", stream.timeout)
	}
}

func TestWithOnComplete(t *testing.T) {
	called := false
	var capturedID string

	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	onComplete := func(streamID string) {
		called = true
		capturedID = streamID
	}

	stream := NewStream("test-complete", workFunc, WithOnComplete(onComplete))
	stream.Start()

	// Wait for completion
	time.Sleep(50 * time.Millisecond)

	if !called {
		t.Error("expected onComplete to be called")
	}
	if capturedID != "test-complete" {
		t.Errorf("expected stream ID 'test-complete', got %s", capturedID)
	}
}

func TestWithOnError(t *testing.T) {
	called := false
	var capturedErr error

	workFunc := func(ctx context.Context, send func(Event)) error {
		return context.DeadlineExceeded
	}

	onError := func(streamID string, err error) {
		called = true
		capturedErr = err
	}

	stream := NewStream("test-error", workFunc, WithOnError(onError))
	stream.Start()

	// Wait for error
	time.Sleep(50 * time.Millisecond)

	if !called {
		t.Error("expected onError to be called")
	}
	if capturedErr != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded error, got %v", capturedErr)
	}
}

func TestWithContext(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	workStarted := make(chan struct{})
	workFunc := func(ctx context.Context, send func(Event)) error {
		close(workStarted)
		<-ctx.Done()
		return ctx.Err()
	}

	stream := NewStream("test-context", workFunc, WithContext(parentCtx))
	stream.Start()

	// Wait for work to start
	<-workStarted

	// Cancel parent context
	parentCancel()

	// Wait for stream to react
	time.Sleep(50 * time.Millisecond)

	status := stream.Status()
	if status != StatusError && status != StatusCancelled {
		t.Errorf("expected status error or cancelled after parent cancel, got %s", status)
	}
}

func TestWithCatchup(t *testing.T) {
	catchupCalled := false
	var capturedStreamID, capturedLastEventID string

	catchupFunc := func(streamID string, lastEventID string) ([]Event, error) {
		catchupCalled = true
		capturedStreamID = streamID
		capturedLastEventID = lastEventID
		return []Event{NewEvent([]byte("catchup-data")).WithID("catchup-1")}, nil
	}

	workFunc := func(ctx context.Context, send func(Event)) error {
		send(NewEvent([]byte("live-data")).WithID("1"))
		return nil
	}

	stream := NewStream("test-catchup", workFunc, WithCatchup(catchupFunc))

	if stream.catchupFunc == nil {
		t.Error("expected catchupFunc to be set")
	}

	// Trigger catchup by requesting events from unknown ID via GetCatchupEvents
	// (GetEventsSince only checks buffer, GetCatchupEvents invokes catchupFunc)
	events := stream.GetCatchupEvents("unknown-id")

	if !catchupCalled {
		t.Error("expected catchup function to be called")
	}
	if capturedStreamID != "test-catchup" {
		t.Errorf("expected stream ID 'test-catchup', got %s", capturedStreamID)
	}
	if capturedLastEventID != "unknown-id" {
		t.Errorf("expected lastEventID 'unknown-id', got %s", capturedLastEventID)
	}
	if len(events) != 1 {
		t.Errorf("expected 1 catchup event, got %d", len(events))
	}
}

func TestWithEventIDs(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	// Default: event IDs disabled
	stream := NewStream("test-default", workFunc)
	if stream.enableEventIDs {
		t.Error("expected enableEventIDs to be false by default")
	}

	// Enabled
	stream = NewStream("test-enabled", workFunc, WithEventIDs(true))
	if !stream.enableEventIDs {
		t.Error("expected enableEventIDs to be true")
	}

	// Explicitly disabled
	stream = NewStream("test-disabled", workFunc, WithEventIDs(false))
	if stream.enableEventIDs {
		t.Error("expected enableEventIDs to be false")
	}
}

func TestWithBuffer(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		return nil
	}

	customBuffer := NewInMemoryBuffer()

	stream := NewStream("test-buffer", workFunc, WithBuffer(customBuffer))

	// The custom buffer should be set
	if stream.buffer != customBuffer {
		t.Error("expected custom buffer to be set")
	}
}

func TestMultipleOptions(t *testing.T) {
	completeCalled := false
	errorCalled := false

	workFunc := func(ctx context.Context, send func(Event)) error {
		send(NewEvent([]byte("test")))
		return nil
	}

	stream := NewStream("test-multi", workFunc,
		WithBufferSize(50),
		WithTimeout(10*time.Second),
		WithEventIDs(true),
		WithOnComplete(func(id string) { completeCalled = true }),
		WithOnError(func(id string, err error) { errorCalled = true }),
	)

	// Verify all options were applied
	if stream.bufferSize != 50 {
		t.Errorf("expected buffer size 50, got %d", stream.bufferSize)
	}
	if stream.timeout != 10*time.Second {
		t.Errorf("expected timeout 10s, got %v", stream.timeout)
	}
	if !stream.enableEventIDs {
		t.Error("expected enableEventIDs to be true")
	}

	stream.Start()
	time.Sleep(50 * time.Millisecond)

	if !completeCalled {
		t.Error("expected onComplete to be called")
	}
	if errorCalled {
		t.Error("expected onError NOT to be called for successful stream")
	}
}
