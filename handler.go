package rstream

import (
	"context"
	"fmt"
	"io"
	"time"
)

// SSEWriter wraps any writer with Flush capability
type SSEWriter interface {
	io.Writer
	Flush() error
}

// StreamOption configures SSE streaming
type SSEOption func(*sseConfig)

type sseConfig struct {
	keepaliveInterval time.Duration
	lastEventID       string
}

// WithKeepalive sets the keepalive interval
func WithKeepalive(interval time.Duration) SSEOption {
	return func(c *sseConfig) {
		c.keepaliveInterval = interval
	}
}

// WithLastEventID sets the Last-Event-ID for reconnection catchup
func WithLastEventID(id string) SSEOption {
	return func(c *sseConfig) {
		c.lastEventID = id
	}
}

// StreamSSE streams events from a stream to an SSE client
func StreamSSE(ctx context.Context, w SSEWriter, stream *Stream, opts ...SSEOption) error {
	// Default config
	config := &sseConfig{
		keepaliveInterval: 15 * time.Second,
	}

	// Apply options
	for _, opt := range opts {
		opt(config)
	}

	// Generate client ID
	clientID := generateClientID()

	// Register client
	eventChan := stream.AddClient(clientID)
	defer stream.RemoveClient(clientID)

	// Send catchup events if Last-Event-ID is provided
	// This will check in-memory buffer first, then fall back to catchup function
	if config.lastEventID != "" {
		catchupEvents := stream.GetCatchupEvents(config.lastEventID)
		for _, event := range catchupEvents {
			if err := writeSSEEvent(w, event); err != nil {
				return err
			}
		}
	}

	// Setup keepalive ticker
	ticker := time.NewTicker(config.keepaliveInterval)
	defer ticker.Stop()

	// Stream events
	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				// Channel closed, stream complete
				return nil
			}

			if err := writeSSEEvent(w, event); err != nil {
				return err
			}

		case <-ticker.C:
			// Send keepalive comment
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return err
			}
			if err := w.Flush(); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// writeSSEEvent formats and writes an SSE event
func writeSSEEvent(w SSEWriter, event Event) error {
	// Write event ID if present
	if event.ID != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", event.ID); err != nil {
			return err
		}
	}

	// Write event type if present
	if event.Type != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
			return err
		}
	}

	// Write retry if present
	if event.Retry > 0 {
		if _, err := fmt.Fprintf(w, "retry: %d\n", event.Retry); err != nil {
			return err
		}
	}

	// Write data
	if _, err := fmt.Fprintf(w, "data: %s\n\n", event.Data); err != nil {
		return err
	}

	return w.Flush()
}

// generateClientID generates a unique client ID
func generateClientID() string {
	return fmt.Sprintf("client-%d", time.Now().UnixNano())
}
