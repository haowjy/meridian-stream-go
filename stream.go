package rstream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkFunc is the function that produces streaming events
// It receives a context and a send function to emit events
type WorkFunc func(ctx context.Context, send func(Event)) error

// Status represents the stream's current state
type Status string

const (
	StatusPending   Status = "pending"   // Created but not started
	StatusRunning   Status = "running"   // Currently executing
	StatusComplete  Status = "complete"  // Finished successfully
	StatusError     Status = "error"     // Encountered error
	StatusCancelled Status = "cancelled" // Cancelled by user
)

// CompleteFunc is called when execution completes successfully
type CompleteFunc func(streamID string)

// ErrorFunc is called when execution encounters an error
type ErrorFunc func(streamID string, err error)

// CatchupFunc retrieves events from external storage (e.g., database)
// when the requested lastEventID is not found in the in-memory buffer.
// It should return aggregated "catchup" events that represent all missed content.
// Return empty slice if no catchup is needed or available.
type CatchupFunc func(streamID string, lastEventID string) ([]Event, error)

// Stream manages a single streaming goroutine and broadcasts
// events to multiple connected clients
type Stream struct {
	id         string
	ctx        context.Context
	cancelFunc context.CancelFunc

	// The work function that produces events
	workFunc WorkFunc

	// Client management
	clients   map[string]chan Event
	clientsMu sync.RWMutex

	// Event buffer for catchup (pluggable storage)
	buffer Buffer

	// Catchup coordination (prevents race between DB query and buffer read)
	catchupMu sync.RWMutex

	// Status tracking
	status   Status
	statusMu sync.RWMutex
	err      error

	// Configuration
	bufferSize       int
	timeout          time.Duration
	enableEventIDs   bool  // DEBUG mode: emit event IDs in SSE stream
	eventIDCounter   int64 // Sequential counter for event IDs (when enabled)
	eventIDMu        sync.Mutex // Protects eventIDCounter for thread-safe generation

	// Hooks
	onComplete CompleteFunc
	onError    ErrorFunc
	catchupFunc CatchupFunc
}

// NewStream creates a new stream
func NewStream(id string, workFunc WorkFunc, opts ...StreamOption) *Stream {
	ctx, cancel := context.WithCancel(context.Background())

	s := &Stream{
		id:         id,
		ctx:        ctx,
		cancelFunc: cancel,
		workFunc:   workFunc,
		clients:    make(map[string]chan Event),
		buffer:     NewInMemoryBuffer(), // Default in-memory buffer
		status:     StatusPending,
		bufferSize: 20, // Default channel buffer size
		timeout:    0,  // No timeout by default
	}

	// Apply options
	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Start begins executing the work function in a goroutine
func (s *Stream) Start() {
	s.statusMu.Lock()
	if s.status != StatusPending {
		s.statusMu.Unlock()
		return // Already started
	}
	s.status = StatusRunning
	s.statusMu.Unlock()

	go s.execute()
}

// execute runs the work function and manages lifecycle
func (s *Stream) execute() {
	// Apply timeout if configured
	ctx := s.ctx
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	// Run work function
	err := s.workFunc(ctx, s.broadcast)

	// Update status
	s.statusMu.Lock()
	if err != nil {
		s.status = StatusError
		s.err = err
	} else if ctx.Err() == context.Canceled {
		s.status = StatusCancelled
	} else {
		s.status = StatusComplete
	}
	s.statusMu.Unlock()

	// Call hooks
	if err != nil && s.onError != nil {
		s.onError(s.id, err)
	} else if s.status == StatusComplete && s.onComplete != nil {
		s.onComplete(s.id)
	}

	// Close all client channels
	s.closeAllClients()
}

// broadcast sends an event to all connected clients and stores in buffer
func (s *Stream) broadcast(event Event) {
	// Generate event ID if DEBUG mode is enabled
	if s.enableEventIDs && event.ID == "" {
		s.eventIDMu.Lock()
		s.eventIDCounter++
		event.ID = fmt.Sprintf("%d", s.eventIDCounter)
		s.eventIDMu.Unlock()
	}

	// Store in buffer for catchup
	s.buffer.Add(event)

	// Broadcast to connected clients
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	for _, ch := range s.clients {
		select {
		case ch <- event:
			// Successfully sent
		default:
			// Channel full, skip (client will reconnect for catchup)
		}
	}
}

// AddClient registers a new client and returns their event channel
func (s *Stream) AddClient(clientID string) <-chan Event {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	ch := make(chan Event, s.bufferSize)
	s.clients[clientID] = ch

	return ch
}

// RemoveClient unregisters a client
func (s *Stream) RemoveClient(clientID string) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	if ch, exists := s.clients[clientID]; exists {
		close(ch)
		delete(s.clients, clientID)
	}
}

// Cancel stops the stream
func (s *Stream) Cancel() {
	s.cancelFunc()
}

// Status returns the current status
func (s *Stream) Status() Status {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.status
}

// Error returns the error if status is Error
func (s *Stream) Error() error {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.err
}

// ID returns the stream ID
func (s *Stream) ID() string {
	return s.id
}

// ClientCount returns the number of connected clients
func (s *Stream) ClientCount() int {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	return len(s.clients)
}

// closeAllClients closes all client channels
func (s *Stream) closeAllClients() {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	for clientID, ch := range s.clients {
		close(ch)
		delete(s.clients, clientID)
	}
}

// GetEventsSince returns all events after the given event ID
// Returns nil if lastEventID is empty or not found
func (s *Stream) GetEventsSince(lastEventID string) []Event {
	return s.buffer.GetSince(lastEventID)
}

// GetCatchupEvents retrieves events for reconnection, using either the in-memory
// buffer or the catchup function (for database-backed replay).
//
// Flow:
// 1. If lastEventID is empty (first connection): call catchup function + return full buffer
// 2. Check if lastEventID exists in buffer
//    - If found: return events from buffer
//    - If not found: call catchup function, then append current buffer
//
// The catchupMu ensures atomic view of persisted blocks (via catchupFunc) and buffer state,
// preventing race conditions where buffer is cleared between DB query and buffer read.
func (s *Stream) GetCatchupEvents(lastEventID string) []Event {
	// Lock for entire catchup operation to prevent races with buffer clear
	s.catchupMu.RLock()
	defer s.catchupMu.RUnlock()

	if lastEventID == "" {
		// First connection - get all events from beginning
		var catchupEvents []Event
		if s.catchupFunc != nil {
			catchupEvents, _ = s.catchupFunc(s.id, "")
		}
		currentBuffer := s.buffer.GetAll()
		return append(catchupEvents, currentBuffer...)
	}

	// First, try to find in buffer
	bufferEvents := s.buffer.GetSince(lastEventID)

	// Found in buffer, return those
	if len(bufferEvents) > 0 {
		return bufferEvents
	}

	// Not in buffer - call catchup function if available
	var catchupEvents []Event
	if s.catchupFunc != nil {
		catchupEvents, _ = s.catchupFunc(s.id, lastEventID)
	}

	// Get current buffer state (events that arrived after catchup)
	currentBuffer := s.buffer.GetAll()

	// Return catchup events + current buffer
	return append(catchupEvents, currentBuffer...)
}

// SnapshotBuffer returns a copy of the current buffer without clearing it.
// Use this to get events for persistence, then call ClearBuffer() after
// successfully persisting to ensure the catchup function can find them.
func (s *Stream) SnapshotBuffer() []Event {
	return s.buffer.Snapshot()
}

// ClearBuffer clears the event buffer.
// IMPORTANT: Only call this AFTER persisting events to your database,
// otherwise clients reconnecting during persistence will lose events.
//
// Recommended pattern:
//   events := stream.SnapshotBuffer()
//   db.SaveTurnBlock(events)  // Persist FIRST
//   stream.ClearBuffer()      // Clear AFTER
//
// The catchupMu write lock prevents concurrent GetCatchupEvents() calls
// from seeing inconsistent state during buffer clear.
func (s *Stream) ClearBuffer() {
	s.catchupMu.Lock()
	defer s.catchupMu.Unlock()
	s.buffer.Clear()
}

// PersistAndClear atomically snapshots the buffer, persists it using the provided
// function, and then clears the buffer. This ensures correct ordering to prevent
// race conditions where clients reconnecting during persistence lose events.
//
// The persist function receives a snapshot of the current buffer and should
// return an error if persistence fails. The function MUST block until the database
// commit completes (or fails). If an error is returned, the buffer is NOT cleared.
//
// The catchupMu write lock is held during both persistence and buffer clear,
// ensuring GetCatchupEvents() sees a consistent view (either events in buffer
// OR events in database, never missing).
//
// Example:
//   err := stream.PersistAndClear(func(events []Event) error {
//       return db.SaveTurnBlock(turnID, events)
//   })
func (s *Stream) PersistAndClear(persistFn func([]Event) error) error {
	// Snapshot buffer (no lock needed for snapshot)
	events := s.SnapshotBuffer()

	if len(events) == 0 {
		return nil // Nothing to persist
	}

	// Lock to ensure atomicity with GetCatchupEvents()
	s.catchupMu.Lock()
	defer s.catchupMu.Unlock()

	// Persist FIRST (blocks until database commit)
	if err := persistFn(events); err != nil {
		return err
	}

	// Only clear if persistence succeeded
	// Note: ClearBuffer() normally acquires catchupMu, but we already hold it
	// So we call buffer.Clear() directly to avoid deadlock
	s.buffer.Clear()
	return nil
}

// BufferSize returns the number of events currently in the buffer
func (s *Stream) BufferSize() int {
	return s.buffer.Size()
}
