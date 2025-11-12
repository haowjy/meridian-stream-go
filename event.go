package rstream

import "time"

// Event represents a streaming event that can be sent to clients
type Event struct {
	// Data is the raw event payload
	Data []byte

	// ID is an optional event identifier for Last-Event-ID tracking
	ID string

	// Type is an optional event type for SSE event field
	Type string

	// Retry is optional retry timeout in milliseconds
	Retry int

	// Timestamp when event was created
	Timestamp time.Time
}

// NewEvent creates a simple event with just data
func NewEvent(data []byte) Event {
	return Event{
		Data:      data,
		Timestamp: time.Now(),
	}
}

// WithID sets the event ID (builder pattern)
func (e Event) WithID(id string) Event {
	e.ID = id
	return e
}

// WithType sets the event type (builder pattern)
func (e Event) WithType(typ string) Event {
	e.Type = typ
	return e
}

// WithRetry sets the retry timeout in milliseconds (builder pattern)
func (e Event) WithRetry(retry int) Event {
	e.Retry = retry
	return e
}
