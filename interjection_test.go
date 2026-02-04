package rstream

import (
	"strings"
	"sync"
	"testing"
)

func TestInMemoryInterjectionBuffer_Append(t *testing.T) {
	t.Run("append to empty buffer", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		err := buf.Append("hello")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		content, ok := buf.Peek()
		if !ok || content != "hello" {
			t.Errorf("expected 'hello', got '%s' (ok=%v)", content, ok)
		}
	})

	t.Run("append multiple times", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("first")
		_ = buf.Append("second")
		content, ok := buf.Peek()
		if !ok || content != "first\nsecond" {
			t.Errorf("expected 'first\\nsecond', got '%s'", content)
		}
	})

	t.Run("append trims whitespace", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("  hello  ")
		content, _ := buf.Peek()
		if content != "hello" {
			t.Errorf("expected 'hello', got '%s'", content)
		}
	})

	t.Run("append empty content returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		err := buf.Append("")
		if err != ErrInterjectionEmpty {
			t.Errorf("expected ErrInterjectionEmpty, got %v", err)
		}
	})

	t.Run("append whitespace-only returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		err := buf.Append("   \t\n   ")
		if err != ErrInterjectionEmpty {
			t.Errorf("expected ErrInterjectionEmpty, got %v", err)
		}
	})

	t.Run("append exceeding max size returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(10, nil)
		err := buf.Append("12345678901") // 11 bytes
		if err != ErrInterjectionTooLarge {
			t.Errorf("expected ErrInterjectionTooLarge, got %v", err)
		}
	})

	t.Run("append combined exceeding max size returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(10, nil)
		_ = buf.Append("12345") // 5 bytes
		err := buf.Append("67890")
		// "12345" + "\n" + "67890" = 11 bytes
		if err != ErrInterjectionTooLarge {
			t.Errorf("expected ErrInterjectionTooLarge, got %v", err)
		}
		// Buffer should still have original content
		content, _ := buf.Peek()
		if content != "12345" {
			t.Errorf("buffer should retain original content, got '%s'", content)
		}
	})
}

func TestInMemoryInterjectionBuffer_Replace(t *testing.T) {
	t.Run("replace empty buffer", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		err := buf.Replace("hello")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		content, ok := buf.Peek()
		if !ok || content != "hello" {
			t.Errorf("expected 'hello', got '%s'", content)
		}
	})

	t.Run("replace existing content", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("old content")
		err := buf.Replace("new content")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		content, _ := buf.Peek()
		if content != "new content" {
			t.Errorf("expected 'new content', got '%s'", content)
		}
	})

	t.Run("replace trims whitespace", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Replace("  hello  ")
		content, _ := buf.Peek()
		if content != "hello" {
			t.Errorf("expected 'hello', got '%s'", content)
		}
	})

	t.Run("replace empty content returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		err := buf.Replace("")
		if err != ErrInterjectionEmpty {
			t.Errorf("expected ErrInterjectionEmpty, got %v", err)
		}
	})

	t.Run("replace exceeding max size returns error", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(10, nil)
		_ = buf.Append("old")
		err := buf.Replace("12345678901") // 11 bytes
		if err != ErrInterjectionTooLarge {
			t.Errorf("expected ErrInterjectionTooLarge, got %v", err)
		}
		// Buffer should still have original content
		content, _ := buf.Peek()
		if content != "old" {
			t.Errorf("buffer should retain original content, got '%s'", content)
		}
	})
}

func TestInMemoryInterjectionBuffer_Peek(t *testing.T) {
	t.Run("peek empty buffer returns false", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		content, ok := buf.Peek()
		if ok || content != "" {
			t.Errorf("expected ('', false), got ('%s', %v)", content, ok)
		}
	})

	t.Run("peek does not clear buffer", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("hello")
		_, _ = buf.Peek()
		_, _ = buf.Peek()
		content, ok := buf.Peek()
		if !ok || content != "hello" {
			t.Errorf("peek should not clear buffer, got ('%s', %v)", content, ok)
		}
	})
}

func TestInMemoryInterjectionBuffer_DrainAndClear(t *testing.T) {
	t.Run("drain empty buffer returns false", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		content, ok := buf.DrainAndClear()
		if ok || content != "" {
			t.Errorf("expected ('', false), got ('%s', %v)", content, ok)
		}
	})

	t.Run("drain clears buffer", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("hello")
		content, ok := buf.DrainAndClear()
		if !ok || content != "hello" {
			t.Errorf("expected ('hello', true), got ('%s', %v)", content, ok)
		}
		// Second drain should return empty
		content2, ok2 := buf.DrainAndClear()
		if ok2 || content2 != "" {
			t.Errorf("expected ('', false) after drain, got ('%s', %v)", content2, ok2)
		}
	})
}

func TestInMemoryInterjectionBuffer_Clear(t *testing.T) {
	buf := NewInMemoryInterjectionBuffer()
	_ = buf.Append("hello")
	buf.Clear()
	content, ok := buf.Peek()
	if ok || content != "" {
		t.Errorf("expected ('', false) after clear, got ('%s', %v)", content, ok)
	}
}

func TestInMemoryInterjectionBuffer_Length(t *testing.T) {
	t.Run("empty buffer has zero length", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		if buf.Length() != 0 {
			t.Errorf("expected 0, got %d", buf.Length())
		}
	})

	t.Run("length matches content bytes", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("hello")
		if buf.Length() != 5 {
			t.Errorf("expected 5, got %d", buf.Length())
		}
	})

	t.Run("length includes delimiter", func(t *testing.T) {
		buf := NewInMemoryInterjectionBuffer()
		_ = buf.Append("a")
		_ = buf.Append("b")
		// "a" + "\n" + "b" = 3 bytes
		if buf.Length() != 3 {
			t.Errorf("expected 3, got %d", buf.Length())
		}
	})
}

func TestInMemoryInterjectionBuffer_ThreadSafety(t *testing.T) {
	buf := NewInMemoryInterjectionBuffer()
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent appends
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = buf.Append("x")
		}()
	}

	// Concurrent peeks
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = buf.Peek()
		}()
	}

	// Concurrent length checks
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = buf.Length()
		}()
	}

	wg.Wait()

	// Buffer should have some content (not empty due to concurrent appends)
	content, ok := buf.Peek()
	if !ok || content == "" {
		t.Error("buffer should have content after concurrent appends")
	}
}

func TestNewInMemoryInterjectionBufferWithOptions(t *testing.T) {
	t.Run("uses provided maxBytes", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(100, nil)
		err := buf.Append(strings.Repeat("x", 101))
		if err != ErrInterjectionTooLarge {
			t.Errorf("expected ErrInterjectionTooLarge, got %v", err)
		}
	})

	t.Run("uses default for zero maxBytes", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(0, nil)
		// Should use default (64KB), so 100 bytes should work
		err := buf.Append(strings.Repeat("x", 100))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("uses default for negative maxBytes", func(t *testing.T) {
		buf := NewInMemoryInterjectionBufferWithOptions(-1, nil)
		// Should use default (64KB), so 100 bytes should work
		err := buf.Append(strings.Repeat("x", 100))
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// customCombiner for testing custom combiners
type customCombiner struct{}

func (c *customCombiner) Combine(existing, incoming string) string {
	return existing + " | " + incoming
}

func TestCustomInterjectionCombiner(t *testing.T) {
	buf := NewInMemoryInterjectionBufferWithOptions(DefaultMaxInterjectionBytes, &customCombiner{})
	_ = buf.Append("first")
	_ = buf.Append("second")
	content, _ := buf.Peek()
	if content != "first | second" {
		t.Errorf("expected 'first | second', got '%s'", content)
	}
}

func TestInterjectionRegistry_GetOrCreate(t *testing.T) {
	t.Run("creates new buffer for unknown ID", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		buf := reg.GetOrCreate("turn-1")
		if buf == nil {
			t.Error("expected non-nil buffer")
		}
	})

	t.Run("returns same buffer for same ID", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		buf1 := reg.GetOrCreate("turn-1")
		buf2 := reg.GetOrCreate("turn-1")
		if buf1 != buf2 {
			t.Error("expected same buffer instance")
		}
	})

	t.Run("returns different buffers for different IDs", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		buf1 := reg.GetOrCreate("turn-1")
		buf2 := reg.GetOrCreate("turn-2")
		if buf1 == buf2 {
			t.Error("expected different buffer instances")
		}
	})
}

func TestInterjectionRegistry_Get(t *testing.T) {
	t.Run("returns nil for unknown ID", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		buf, ok := reg.Get("unknown")
		if ok || buf != nil {
			t.Errorf("expected (nil, false), got (%v, %v)", buf, ok)
		}
	})

	t.Run("returns buffer for known ID", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		_ = reg.GetOrCreate("turn-1")
		buf, ok := reg.Get("turn-1")
		if !ok || buf == nil {
			t.Errorf("expected (buf, true), got (%v, %v)", buf, ok)
		}
	})
}

func TestInterjectionRegistry_Remove(t *testing.T) {
	t.Run("removes existing buffer", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		_ = reg.GetOrCreate("turn-1")
		reg.Remove("turn-1")
		buf, ok := reg.Get("turn-1")
		if ok || buf != nil {
			t.Errorf("expected (nil, false) after remove, got (%v, %v)", buf, ok)
		}
	})

	t.Run("remove non-existent ID is no-op", func(t *testing.T) {
		reg := NewInterjectionRegistry()
		// Should not panic
		reg.Remove("unknown")
	})
}

func TestInterjectionRegistry_ThreadSafety(t *testing.T) {
	reg := NewInterjectionRegistry()
	var wg sync.WaitGroup
	iterations := 100

	// Concurrent GetOrCreate
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := reg.GetOrCreate("turn-shared")
			_ = buf.Append("x")
		}(i)
	}

	// Concurrent Get
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = reg.Get("turn-shared")
		}()
	}

	// Concurrent GetOrCreate with different IDs
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = reg.GetOrCreate("turn-unique-" + string(rune('0'+id%10)))
		}(i)
	}

	wg.Wait()

	// Shared buffer should exist
	buf, ok := reg.Get("turn-shared")
	if !ok || buf == nil {
		t.Error("shared buffer should exist after concurrent access")
	}
}
