package rstream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Registry manages multiple streams with automatic cleanup
type Registry struct {
	streams map[string]*Stream
	mu      sync.RWMutex

	// Cleanup configuration
	cleanupInterval time.Duration
	retentionPeriod time.Duration

	// Completion tracking
	completionTimes map[string]time.Time
	completionMu    sync.RWMutex
}

// RegistryOption configures a Registry
type RegistryOption func(*Registry)

// WithCleanupInterval sets how often cleanup runs
func WithCleanupInterval(interval time.Duration) RegistryOption {
	return func(r *Registry) {
		r.cleanupInterval = interval
	}
}

// WithRetentionPeriod sets how long to keep completed streams
func WithRetentionPeriod(period time.Duration) RegistryOption {
	return func(r *Registry) {
		r.retentionPeriod = period
	}
}

// NewRegistry creates a new registry
func NewRegistry(opts ...RegistryOption) *Registry {
	r := &Registry{
		streams:         make(map[string]*Stream),
		completionTimes: make(map[string]time.Time),
		cleanupInterval: 1 * time.Minute,
		retentionPeriod: 10 * time.Minute,
	}

	// Apply options
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Register adds a stream to the registry
func (r *Registry) Register(stream *Stream) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.streams[stream.ID()]; exists {
		return fmt.Errorf("stream with ID %s already exists", stream.ID())
	}

	r.streams[stream.ID()] = stream

	// Set up completion tracking hooks
	stream.onComplete = func(id string) {
		r.markCompleted(id)
	}
	stream.onError = func(id string, err error) {
		r.markCompleted(id)
	}

	return nil
}

// Get retrieves a stream by ID
func (r *Registry) Get(id string) *Stream {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.streams[id]
}

// Remove removes a stream from the registry
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.streams, id)

	r.completionMu.Lock()
	delete(r.completionTimes, id)
	r.completionMu.Unlock()
}

// Count returns the number of registered streams
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.streams)
}

// StartCleanup starts the background cleanup goroutine
func (r *Registry) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(r.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.cleanup()
		}
	}
}

// markCompleted tracks when a stream completes
func (r *Registry) markCompleted(id string) {
	// Look up stream without holding write lock to avoid blocking other readers
	r.mu.RLock()
	stream, exists := r.streams[id]
	r.mu.RUnlock()

	if !exists {
		return
	}

	// Clear in-memory buffer now that the stream has reached a terminal state.
	// All persisted content is in the database; we don't need the buffer for replay.
	stream.ClearBuffer()

	// Track completion time - cleanup goroutine will remove after retentionPeriod
	r.completionMu.Lock()
	r.completionTimes[id] = time.Now()
	r.completionMu.Unlock()
}

// cleanup removes old completed streams
func (r *Registry) cleanup() {
	now := time.Now()
	var toRemove []string

	r.mu.RLock()
	for id, stream := range r.streams {
		status := stream.Status()

		// Only cleanup terminal states
		if status == StatusComplete || status == StatusError || status == StatusCancelled {
			r.completionMu.RLock()
			completedAt, exists := r.completionTimes[id]
			r.completionMu.RUnlock()

			if exists && now.Sub(completedAt) > r.retentionPeriod {
				toRemove = append(toRemove, id)
			}
		}
	}
	r.mu.RUnlock()

	// Remove old streams
	for _, id := range toRemove {
		r.Remove(id)
	}
}
