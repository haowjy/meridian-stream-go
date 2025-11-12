package rstream

import (
	"sync"
	"testing"
)

func TestInMemoryBuffer_NewBuffer(t *testing.T) {
	buffer := NewInMemoryBuffer()

	if buffer == nil {
		t.Fatal("NewInMemoryBuffer returned nil")
	}

	if buffer.Size() != 0 {
		t.Errorf("new buffer size = %d, expected 0", buffer.Size())
	}

	if events := buffer.GetAll(); len(events) != 0 {
		t.Errorf("new buffer GetAll() returned %d events, expected 0", len(events))
	}
}

func TestInMemoryBuffer_Add(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Add single event
	buffer.Add(Event{ID: "1", Data: []byte("test")})

	if buffer.Size() != 1 {
		t.Errorf("buffer size = %d after Add, expected 1", buffer.Size())
	}

	// Add multiple events
	buffer.Add(Event{ID: "2", Data: []byte("test2")})
	buffer.Add(Event{ID: "3", Data: []byte("test3")})

	if buffer.Size() != 3 {
		t.Errorf("buffer size = %d after 3 adds, expected 3", buffer.Size())
	}
}

func TestInMemoryBuffer_GetAll(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Empty buffer
	events := buffer.GetAll()
	if len(events) != 0 {
		t.Errorf("GetAll on empty buffer returned %d events, expected 0", len(events))
	}

	// Add events
	buffer.Add(Event{ID: "1", Data: []byte("one")})
	buffer.Add(Event{ID: "2", Data: []byte("two")})
	buffer.Add(Event{ID: "3", Data: []byte("three")})

	events = buffer.GetAll()
	if len(events) != 3 {
		t.Fatalf("GetAll returned %d events, expected 3", len(events))
	}

	// Verify order is preserved
	if events[0].ID != "1" || events[1].ID != "2" || events[2].ID != "3" {
		t.Errorf("GetAll returned events in wrong order: %v", events)
	}

	// Verify it's a copy (modifying returned slice doesn't affect buffer)
	events[0].ID = "modified"
	newEvents := buffer.GetAll()
	if newEvents[0].ID == "modified" {
		t.Error("GetAll did not return a copy, buffer was modified")
	}
}

func TestInMemoryBuffer_GetSince(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Setup events
	for i := 1; i <= 10; i++ {
		buffer.Add(Event{ID: string(rune(i + '0')), Data: []byte("test")})
	}

	tests := []struct {
		name          string
		lastEventID   string
		expectedCount int
		expectedFirst string // ID of first event in result
	}{
		{
			name:          "from middle (event 5)",
			lastEventID:   "5",
			expectedCount: 5,
			expectedFirst: "6",
		},
		{
			name:          "from start (event 1)",
			lastEventID:   "1",
			expectedCount: 9,
			expectedFirst: "2",
		},
		{
			name:          "from end (event 10)",
			lastEventID:   string(rune(10 + '0')),
			expectedCount: 0,
			expectedFirst: "",
		},
		{
			name:          "empty ID",
			lastEventID:   "",
			expectedCount: 0,
			expectedFirst: "",
		},
		{
			name:          "non-existent ID",
			lastEventID:   "999",
			expectedCount: 0,
			expectedFirst: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := buffer.GetSince(tt.lastEventID)

			if len(events) != tt.expectedCount {
				t.Errorf("GetSince(%q) returned %d events, expected %d",
					tt.lastEventID, len(events), tt.expectedCount)
			}

			if tt.expectedCount > 0 && events[0].ID != tt.expectedFirst {
				t.Errorf("first event ID = %q, expected %q", events[0].ID, tt.expectedFirst)
			}
		})
	}
}

func TestInMemoryBuffer_Clear(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Add events
	buffer.Add(Event{ID: "1", Data: []byte("test")})
	buffer.Add(Event{ID: "2", Data: []byte("test")})
	buffer.Add(Event{ID: "3", Data: []byte("test")})

	if buffer.Size() != 3 {
		t.Fatalf("setup failed, buffer size = %d", buffer.Size())
	}

	// Clear buffer
	buffer.Clear()

	if buffer.Size() != 0 {
		t.Errorf("buffer size = %d after Clear, expected 0", buffer.Size())
	}

	if events := buffer.GetAll(); len(events) != 0 {
		t.Errorf("GetAll after Clear returned %d events, expected 0", len(events))
	}

	// Clear on empty buffer should not panic
	buffer.Clear()
	if buffer.Size() != 0 {
		t.Errorf("buffer size = %d after double Clear, expected 0", buffer.Size())
	}
}

func TestInMemoryBuffer_Size(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Empty buffer
	if buffer.Size() != 0 {
		t.Errorf("empty buffer size = %d, expected 0", buffer.Size())
	}

	// Add events one by one
	for i := 1; i <= 5; i++ {
		buffer.Add(Event{ID: string(rune(i + '0')), Data: []byte("test")})
		if buffer.Size() != i {
			t.Errorf("after adding %d events, size = %d", i, buffer.Size())
		}
	}

	// Clear and verify
	buffer.Clear()
	if buffer.Size() != 0 {
		t.Errorf("size after Clear = %d, expected 0", buffer.Size())
	}
}

func TestInMemoryBuffer_Snapshot(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Empty buffer
	snapshot := buffer.Snapshot()
	if len(snapshot) != 0 {
		t.Errorf("Snapshot of empty buffer returned %d events, expected 0", len(snapshot))
	}

	// Add events
	buffer.Add(Event{ID: "1", Data: []byte("one")})
	buffer.Add(Event{ID: "2", Data: []byte("two")})
	buffer.Add(Event{ID: "3", Data: []byte("three")})

	snapshot = buffer.Snapshot()

	if len(snapshot) != 3 {
		t.Fatalf("Snapshot returned %d events, expected 3", len(snapshot))
	}

	// Verify it's a copy (modifying snapshot doesn't affect buffer)
	snapshot[0].ID = "modified"
	newSnapshot := buffer.Snapshot()
	if newSnapshot[0].ID == "modified" {
		t.Error("Snapshot did not return a copy, buffer was modified")
	}

	// Verify Snapshot and GetAll return the same data
	all := buffer.GetAll()
	if len(all) != len(snapshot) {
		t.Errorf("Snapshot and GetAll returned different lengths")
	}

	for i := range all {
		if all[i].ID != newSnapshot[i].ID {
			t.Errorf("Snapshot and GetAll differ at index %d", i)
		}
	}
}

func TestInMemoryBuffer_ConcurrentAccess(t *testing.T) {
	buffer := NewInMemoryBuffer()
	var wg sync.WaitGroup

	// Concurrent Adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				buffer.Add(Event{
					ID:   string(rune(id*100 + j)),
					Data: []byte("concurrent test"),
				})
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = buffer.GetAll()
				_ = buffer.Size()
			}
		}()
	}

	wg.Wait()

	// Should have 10 goroutines × 100 events = 1000 events
	if buffer.Size() != 1000 {
		t.Errorf("after concurrent adds, size = %d, expected 1000", buffer.Size())
	}

	// Concurrent Clear and Add
	wg.Add(2)
	go func() {
		defer wg.Done()
		buffer.Clear()
	}()
	go func() {
		defer wg.Done()
		buffer.Add(Event{ID: "test", Data: []byte("test")})
	}()
	wg.Wait()

	// Should have either 0 or 1 events (race is okay, just shouldn't crash)
	size := buffer.Size()
	if size != 0 && size != 1 {
		t.Errorf("after concurrent Clear/Add, size = %d, expected 0 or 1", size)
	}
}

func TestInMemoryBuffer_GetSince_ConcurrentWithAdd(t *testing.T) {
	buffer := NewInMemoryBuffer()

	// Pre-populate with some events
	for i := 1; i <= 5; i++ {
		buffer.Add(Event{ID: string(rune(i + '0')), Data: []byte("test")})
	}

	var wg sync.WaitGroup

	// Concurrent GetSince
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = buffer.GetSince("3")
			}
		}()
	}

	// Concurrent Add
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				buffer.Add(Event{
					ID:   string(rune(id*50 + j + 100)),
					Data: []byte("concurrent"),
				})
			}
		}(i)
	}

	wg.Wait()

	// Should have 5 initial + (3 goroutines × 50 events) = 155 events
	expectedSize := 5 + (3 * 50)
	if buffer.Size() != expectedSize {
		t.Errorf("after concurrent operations, size = %d, expected %d", buffer.Size(), expectedSize)
	}
}

func TestInMemoryBuffer_EdgeCases(t *testing.T) {
	buffer := NewInMemoryBuffer()

	t.Run("GetSince on empty buffer", func(t *testing.T) {
		events := buffer.GetSince("anything")
		if events != nil {
			t.Error("GetSince on empty buffer should return nil")
		}
	})

	t.Run("GetAll on empty buffer", func(t *testing.T) {
		events := buffer.GetAll()
		if len(events) != 0 {
			t.Error("GetAll on empty buffer should return empty slice")
		}
	})

	t.Run("Snapshot on empty buffer", func(t *testing.T) {
		snapshot := buffer.Snapshot()
		if len(snapshot) != 0 {
			t.Error("Snapshot on empty buffer should return empty slice")
		}
	})

	t.Run("Clear on empty buffer", func(t *testing.T) {
		// Should not panic
		buffer.Clear()
		if buffer.Size() != 0 {
			t.Error("buffer should still be empty after Clear")
		}
	})

	t.Run("Single event buffer", func(t *testing.T) {
		buffer.Add(Event{ID: "only", Data: []byte("one")})

		if buffer.Size() != 1 {
			t.Errorf("size = %d, expected 1", buffer.Size())
		}

		// GetSince with the only event should return nil
		events := buffer.GetSince("only")
		if events != nil {
			t.Error("GetSince with last event should return nil")
		}

		// GetAll should return the event
		all := buffer.GetAll()
		if len(all) != 1 || all[0].ID != "only" {
			t.Error("GetAll should return the single event")
		}
	})
}
