package rstream

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// mockSSEWriter implements SSEWriter for testing
type mockSSEWriter struct {
	buffer *bytes.Buffer
}

func newMockSSEWriter() *mockSSEWriter {
	return &mockSSEWriter{
		buffer: &bytes.Buffer{},
	}
}

func (m *mockSSEWriter) Write(p []byte) (n int, err error) {
	return m.buffer.Write(p)
}

func (m *mockSSEWriter) Flush() error {
	return nil
}

func (m *mockSSEWriter) String() string {
	return m.buffer.String()
}

func TestStreamSSE_BasicStreaming(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 1; i <= 3; i++ {
			send(Event{
				ID:   string(rune(i + '0')),
				Type: "message",
				Data: []byte("test"),
			})
			time.Sleep(50 * time.Millisecond) // Slow down to ensure SSE can read
		}
		return nil
	}

	stream := NewStream("test-1", workFunc)
	stream.Start()

	writer := newMockSSEWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := StreamSSE(ctx, writer, stream)
	if err != nil && err != context.DeadlineExceeded {
		t.Errorf("unexpected error: %v", err)
	}

	output := writer.String()

	// Check for SSE format (use more flexible matching)
	if !strings.Contains(output, "id:") {
		t.Errorf("expected event ID in output, got: %s", output)
	}

	if !strings.Contains(output, "event: message") {
		t.Errorf("expected event type in output, got: %s", output)
	}

	if !strings.Contains(output, "data: test") {
		t.Errorf("expected event data in output, got: %s", output)
	}
}

func TestStreamSSE_Keepalive(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	stream := NewStream("test-2", workFunc)
	stream.Start()

	writer := newMockSSEWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := StreamSSE(ctx, writer, stream,
		WithKeepalive(50*time.Millisecond),
	)

	if err != context.DeadlineExceeded {
		t.Errorf("expected deadline exceeded, got %v", err)
	}

	output := writer.String()

	// Should have keepalive comments
	if !strings.Contains(output, ": keepalive") {
		t.Error("expected keepalive comments in output")
	}
}

func TestStreamSSE_Catchup(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		for i := 4; i <= 6; i++ {
			send(Event{
				ID:   string(rune(i + '0')),
				Data: []byte("new"),
			})
		}
		return nil
	}

	stream := NewStream("test-3", workFunc,
		WithCatchup(func(streamID, lastEventID string) ([]Event, error) {
			return []Event{
				{ID: "catchup-1", Data: []byte("old-1")},
				{ID: "catchup-2", Data: []byte("old-2")},
			}, nil
		}),
	)

	stream.Start()
	time.Sleep(50 * time.Millisecond)
	stream.ClearBuffer()

	writer := newMockSSEWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Request with Last-Event-ID
	err := StreamSSE(ctx, writer, stream,
		WithLastEventID("3"),
	)

	if err != nil && err != context.DeadlineExceeded {
		t.Errorf("unexpected error: %v", err)
	}

	output := writer.String()

	// Should have catchup events
	if !strings.Contains(output, "id: catchup-1") {
		t.Error("expected catchup event in output")
	}

	if !strings.Contains(output, "data: old-1") {
		t.Error("expected catchup data in output")
	}
}

func TestStreamSSE_EmptyLastEventID(t *testing.T) {
	workFunc := func(ctx context.Context, send func(Event)) error {
		send(Event{ID: "1", Data: []byte("test")})
		return nil
	}

	stream := NewStream("test-4", workFunc)
	stream.Start()
	time.Sleep(50 * time.Millisecond)

	writer := newMockSSEWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Empty Last-Event-ID should not trigger catchup
	err := StreamSSE(ctx, writer, stream,
		WithLastEventID(""),
	)

	if err != nil && err != context.DeadlineExceeded {
		t.Errorf("unexpected error: %v", err)
	}

	output := writer.String()
	if strings.Contains(output, "catchup") {
		t.Error("should not have catchup events with empty Last-Event-ID")
	}
}

func TestWriteSSEEvent_AllFields(t *testing.T) {
	writer := newMockSSEWriter()

	event := Event{
		ID:    "event-1",
		Type:  "message",
		Data:  []byte("hello world"),
		Retry: 5000,
	}

	err := writeSSEEvent(writer, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	output := writer.String()

	expectedParts := []string{
		"id: event-1",
		"event: message",
		"retry: 5000",
		"data: hello world",
	}

	for _, part := range expectedParts {
		if !strings.Contains(output, part) {
			t.Errorf("expected output to contain %q, got: %s", part, output)
		}
	}
}

func TestWriteSSEEvent_MinimalFields(t *testing.T) {
	writer := newMockSSEWriter()

	event := Event{
		Data: []byte("minimal"),
	}

	err := writeSSEEvent(writer, event)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	output := writer.String()

	if strings.Contains(output, "id:") {
		t.Error("should not have ID field for minimal event")
	}

	if strings.Contains(output, "event:") {
		t.Error("should not have event field for minimal event")
	}

	if !strings.Contains(output, "data: minimal") {
		t.Error("expected data field in output")
	}
}
