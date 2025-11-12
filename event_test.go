package rstream

import (
	"testing"
	"time"
)

func TestEvent_NewEvent(t *testing.T) {
	data := []byte("test data")
	event := NewEvent(data)

	if string(event.Data) != string(data) {
		t.Errorf("expected data %s, got %s", data, event.Data)
	}

	if event.Timestamp.IsZero() {
		t.Error("expected timestamp to be set")
	}

	if event.ID != "" {
		t.Error("expected ID to be empty")
	}

	if event.Type != "" {
		t.Error("expected Type to be empty")
	}
}

func TestEvent_BuilderPattern(t *testing.T) {
	event := NewEvent([]byte("test")).
		WithID("event-1").
		WithType("message").
		WithRetry(5000)

	if event.ID != "event-1" {
		t.Errorf("expected ID 'event-1', got %s", event.ID)
	}

	if event.Type != "message" {
		t.Errorf("expected Type 'message', got %s", event.Type)
	}

	if event.Retry != 5000 {
		t.Errorf("expected Retry 5000, got %d", event.Retry)
	}
}

func TestEvent_ChainedBuilders(t *testing.T) {
	// Test that builder methods return new instances (value semantics)
	base := NewEvent([]byte("test"))
	event1 := base.WithID("1")
	event2 := base.WithID("2")

	if event1.ID == event2.ID {
		t.Error("expected independent event instances")
	}

	if base.ID != "" {
		t.Error("expected base event to remain unchanged")
	}
}

func TestEvent_StructInitialization(t *testing.T) {
	now := time.Now()
	event := Event{
		Data:      []byte("test"),
		ID:        "custom-id",
		Type:      "custom-type",
		Retry:     3000,
		Timestamp: now,
	}

	if string(event.Data) != "test" {
		t.Errorf("expected data 'test', got %s", event.Data)
	}

	if event.ID != "custom-id" {
		t.Errorf("expected ID 'custom-id', got %s", event.ID)
	}

	if event.Type != "custom-type" {
		t.Errorf("expected Type 'custom-type', got %s", event.Type)
	}

	if event.Retry != 3000 {
		t.Errorf("expected Retry 3000, got %d", event.Retry)
	}

	if !event.Timestamp.Equal(now) {
		t.Error("expected timestamp to match")
	}
}
