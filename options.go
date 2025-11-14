package rstream

import (
	"context"
	"time"
)

// StreamOption configures a Stream
type StreamOption func(*Stream)

// WithBufferSize sets the channel buffer size for clients
func WithBufferSize(size int) StreamOption {
	return func(s *Stream) {
		s.bufferSize = size
	}
}

// WithTimeout sets a timeout for execution
func WithTimeout(timeout time.Duration) StreamOption {
	return func(s *Stream) {
		s.timeout = timeout
	}
}

// WithOnComplete sets a callback for successful completion
func WithOnComplete(fn CompleteFunc) StreamOption {
	return func(s *Stream) {
		s.onComplete = fn
	}
}

// WithOnError sets a callback for errors
func WithOnError(fn ErrorFunc) StreamOption {
	return func(s *Stream) {
		s.onError = fn
	}
}

// WithContext sets a custom parent context
func WithContext(ctx context.Context) StreamOption {
	return func(s *Stream) {
		s.ctx, s.cancelFunc = context.WithCancel(ctx)
	}
}

// WithCatchup sets a catchup function for database-backed event replay.
// The function is called when a client reconnects with a Last-Event-ID that's
// not found in the in-memory buffer (e.g., buffer was cleared after persisting to DB).
//
// The catchup function should return aggregated "catchup" events representing
// all missed content. These can use special event IDs (e.g., "catchup-1", "catchup-2")
// to differentiate from regular streaming events.
func WithCatchup(fn CatchupFunc) StreamOption {
	return func(s *Stream) {
		s.catchupFunc = fn
	}
}

// WithBuffer sets a custom buffer implementation.
// Use this to replace the default in-memory buffer with Redis, Memcached,
// or any other storage backend that implements the Buffer interface.
//
// Example with Redis:
//   stream := NewStream(id, workFunc,
//       WithBuffer(NewRedisBuffer(redisClient, streamID)),
//   )
func WithBuffer(buffer Buffer) StreamOption {
	return func(s *Stream) {
		s.buffer = buffer
	}
}

// WithEventIDs enables DEBUG mode where sequential event IDs are emitted
// in the SSE stream. This is useful for debugging and development, but should
// typically be disabled in production for simplicity and performance.
//
// When enabled:
//   - Events will have sequential IDs: "1", "2", "3", ...
//   - SSE clients receive "id:" field for Last-Event-ID tracking
//   - Slightly more memory usage and processing
//
// When disabled (default):
//   - No event IDs emitted
//   - Catchup still works based on turn/block IDs from database
//   - Simpler, faster, less memory
//
// Example:
//   stream := NewStream(id, workFunc,
//       WithEventIDs(config.Debug),
//   )
func WithEventIDs(enabled bool) StreamOption {
	return func(s *Stream) {
		s.enableEventIDs = enabled
	}
}
