package rstream

import (
	"errors"
	"strings"
	"sync"
)

// Interjection buffer errors
var (
	ErrInterjectionEmpty    = errors.New("interjection content is empty")
	ErrInterjectionTooLarge = errors.New("interjection buffer exceeds maximum size")
)

// InterjectionBuffer manages a single interjection buffer for an assistant turn.
// Thread-safe for concurrent access from HTTP handlers and streaming goroutines.
// At most one interjection buffer exists per streaming assistant turn.
type InterjectionBuffer interface {
	// Append adds content to the existing buffer with a delimiter.
	// Returns ErrInterjectionEmpty if content is whitespace-only.
	// Returns ErrInterjectionTooLarge if result exceeds maxBytes.
	Append(content string) error

	// Replace overwrites the buffer with new content.
	// Returns ErrInterjectionEmpty if content is whitespace-only.
	// Returns ErrInterjectionTooLarge if content exceeds maxBytes.
	Replace(content string) error

	// Peek returns the current buffer content without clearing.
	// Returns (content, true) if non-empty, ("", false) if empty.
	Peek() (string, bool)

	// DrainAndClear atomically returns and clears the buffer.
	// Returns (content, true) if non-empty, ("", false) if empty.
	// This is the primary method used at injection points.
	DrainAndClear() (string, bool)

	// Clear empties the buffer without returning content.
	Clear()

	// Length returns the current buffer length in bytes.
	Length() int
}

// InterjectionCombiner defines how multiple interjection appends are combined.
type InterjectionCombiner interface {
	// Combine joins existing and incoming interjection content.
	Combine(existing, incoming string) string
}

// DefaultMaxInterjectionBytes is the default maximum buffer size (64KB).
// Large enough for substantial user input, small enough to prevent abuse.
const DefaultMaxInterjectionBytes = 64 * 1024

// InMemoryInterjectionBuffer is a thread-safe in-memory interjection buffer.
type InMemoryInterjectionBuffer struct {
	mu       sync.RWMutex
	content  string
	maxBytes int
	combiner InterjectionCombiner
}

// NewInMemoryInterjectionBuffer creates a new interjection buffer with default settings.
func NewInMemoryInterjectionBuffer() *InMemoryInterjectionBuffer {
	return &InMemoryInterjectionBuffer{
		maxBytes: DefaultMaxInterjectionBytes,
		combiner: &SimpleInterjectionCombiner{},
	}
}

// NewInMemoryInterjectionBufferWithOptions creates a buffer with custom settings.
func NewInMemoryInterjectionBufferWithOptions(maxBytes int, combiner InterjectionCombiner) *InMemoryInterjectionBuffer {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxInterjectionBytes
	}
	if combiner == nil {
		combiner = &SimpleInterjectionCombiner{}
	}
	return &InMemoryInterjectionBuffer{
		maxBytes: maxBytes,
		combiner: combiner,
	}
}

// Append adds content to the existing buffer with a delimiter.
func (b *InMemoryInterjectionBuffer) Append(content string) error {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ErrInterjectionEmpty
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var newContent string
	if b.content == "" {
		newContent = trimmed
	} else {
		newContent = b.combiner.Combine(b.content, trimmed)
	}

	if len(newContent) > b.maxBytes {
		return ErrInterjectionTooLarge
	}

	b.content = newContent
	return nil
}

// Replace overwrites the buffer with new content.
func (b *InMemoryInterjectionBuffer) Replace(content string) error {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return ErrInterjectionEmpty
	}

	if len(trimmed) > b.maxBytes {
		return ErrInterjectionTooLarge
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.content = trimmed
	return nil
}

// Peek returns the current buffer content without clearing.
func (b *InMemoryInterjectionBuffer) Peek() (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.content == "" {
		return "", false
	}
	return b.content, true
}

// DrainAndClear atomically returns and clears the buffer.
func (b *InMemoryInterjectionBuffer) DrainAndClear() (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.content == "" {
		return "", false
	}

	content := b.content
	b.content = ""
	return content, true
}

// Clear empties the buffer without returning content.
func (b *InMemoryInterjectionBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content = ""
}

// Length returns the current buffer length in bytes.
func (b *InMemoryInterjectionBuffer) Length() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.content)
}

// SimpleInterjectionCombiner joins interjections with a visual delimiter.
type SimpleInterjectionCombiner struct{}

// interjectionDelimiter is the visual separator between appended interjections.
// Simple newline keeps the UI indicator clean when multiple messages are queued.
const interjectionDelimiter = "\n"

// Combine joins existing and incoming interjection content.
func (c *SimpleInterjectionCombiner) Combine(existing, incoming string) string {
	return existing + interjectionDelimiter + incoming
}

// InterjectionRegistry tracks interjection buffers by assistant turn ID.
// Used by the streaming service to manage buffers across HTTP requests.
type InterjectionRegistry struct {
	buffers sync.Map // map[turnID]*InMemoryInterjectionBuffer
}

// NewInterjectionRegistry creates a new interjection registry.
func NewInterjectionRegistry() *InterjectionRegistry {
	return &InterjectionRegistry{}
}

// GetOrCreate returns the buffer for a turn, creating one if needed.
func (r *InterjectionRegistry) GetOrCreate(turnID string) *InMemoryInterjectionBuffer {
	actual, _ := r.buffers.LoadOrStore(turnID, NewInMemoryInterjectionBuffer())
	return actual.(*InMemoryInterjectionBuffer)
}

// Get returns the buffer for a turn if it exists.
func (r *InterjectionRegistry) Get(turnID string) (*InMemoryInterjectionBuffer, bool) {
	if v, ok := r.buffers.Load(turnID); ok {
		return v.(*InMemoryInterjectionBuffer), true
	}
	return nil, false
}

// Remove deletes the buffer for a turn.
func (r *InterjectionRegistry) Remove(turnID string) {
	r.buffers.Delete(turnID)
}
