package rstream

import "sync"

// Buffer defines the interface for event storage.
// Implementations must be thread-safe for concurrent access.
type Buffer interface {
	// Add appends an event to the buffer
	Add(event Event)

	// GetAll returns all events currently in the buffer
	GetAll() []Event

	// GetSince returns events after the given event ID.
	// Returns nil if lastEventID is empty, not found, or there are no events after it.
	GetSince(lastEventID string) []Event

	// Clear removes all events from the buffer
	Clear()

	// Size returns the number of events in the buffer
	Size() int

	// Snapshot returns a copy of all events (useful for persistence)
	Snapshot() []Event
}

// InMemoryBuffer is the default in-memory implementation of Buffer.
// It stores events in a slice protected by a RWMutex.
type InMemoryBuffer struct {
	events []Event
	mu     sync.RWMutex
}

// NewInMemoryBuffer creates a new in-memory buffer
func NewInMemoryBuffer() *InMemoryBuffer {
	return &InMemoryBuffer{
		events: make([]Event, 0),
	}
}

// Add appends an event to the buffer
func (b *InMemoryBuffer) Add(event Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
}

// GetAll returns all events in the buffer
func (b *InMemoryBuffer) GetAll() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return copy to prevent external modification
	result := make([]Event, len(b.events))
	copy(result, b.events)
	return result
}

// GetSince returns events after the given event ID.
// Returns nil if lastEventID is not found or there are no events after it.
func (b *InMemoryBuffer) GetSince(lastEventID string) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if lastEventID == "" {
		return nil
	}

	// Find the last event
	lastIndex := -1
	for i, event := range b.events {
		if event.ID == lastEventID {
			lastIndex = i
			break
		}
	}

	// Event not found or no events after it
	if lastIndex == -1 || lastIndex+1 >= len(b.events) {
		return nil
	}

	// Return events after lastIndex
	remaining := b.events[lastIndex+1:]
	result := make([]Event, len(remaining))
	copy(result, remaining)
	return result
}

// Clear removes all events from the buffer
func (b *InMemoryBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = make([]Event, 0)
}

// Size returns the number of events in the buffer
func (b *InMemoryBuffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.events)
}

// Snapshot returns a copy of all events
func (b *InMemoryBuffer) Snapshot() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Event, len(b.events))
	copy(result, b.events)
	return result
}
