# meridian-stream-go

**Resilient SSE streaming for distributed Go applications.**

Multi-client SSE streaming with reconnection support. One goroutine → many clients. Works across multiple server instances.

Built for [Meridian](https://meridian-flow.com) - an AI-powered writing platform

## Features

- ✅ **Multi-client support** - One stream → many SSE connections
- ✅ **Reconnection with catchup** - Clients resume seamlessly
- ✅ **Framework agnostic** - Works with any Go HTTP framework
- ✅ **Automatic cleanup** - Memory-safe goroutine lifecycle management

## Quick Start

```go
import (
    "context"
    "fmt"
    "time"

    mstream "github.com/haowjy/meridian-stream-go"
)

func main() {
    // Create a stream
    stream := mstream.NewStream("stream-123", func(ctx context.Context, send func(mstream.Event)) error {
        // Your streaming logic
        for i := 0; i < 10; i++ {
            send(mstream.NewEvent([]byte(fmt.Sprintf("Event %d", i))))
            time.Sleep(1 * time.Second)
        }
        return nil
    })

    // Start streaming
    stream.Start()

    // Connect a client
    eventChan := stream.AddClient("client-1")
    for event := range eventChan {
        fmt.Printf("Received: %s\n", event.Data)
    }
}
```

## With net/http

```go
import (
    "net/http"
    mstream "github.com/haowjy/meridian-stream-go"
    nethttpadapter "github.com/haowjy/meridian-stream-go/adapters/nethttp"
)

func main() {
    registry := mstream.NewRegistry()

    // Start cleanup goroutine
    go registry.StartCleanup(context.Background())

    // Create stream endpoint
    http.HandleFunc("POST /streams", func(w http.ResponseWriter, r *http.Request) {
        streamID := generateID()

        stream := mstream.NewStream(streamID, func(ctx context.Context, send func(mstream.Event)) error {
            // Your streaming logic here
            return nil
        })

        registry.Register(stream)
        stream.Start()

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "stream_id": streamID,
            "stream_url": fmt.Sprintf("/streams/%s", streamID),
        })
    })

    // SSE endpoint (Go 1.22+ routing)
    http.HandleFunc("GET /streams/{id}", nethttpadapter.Handler(registry))

    http.ListenAndServe(":3000", nil)
}
```

## With Reconnection/Catchup

The library provides **two-tier catchup**: in-memory buffer for recent events, with database fallback for cleared events.

### How It Works

1. **In-memory buffer**: Fast catchup for recent events still in buffer
2. **Database fallback**: When buffer is cleared, query database for aggregated blocks
3. **Automatic**: net/http adapter handles `Last-Event-ID` header automatically

### Client Side

```javascript
// Browser EventSource automatically sends Last-Event-ID on reconnection
const eventSource = new EventSource('/streams/abc123');

// Handle both live deltas and aggregated catchup blocks
eventSource.addEventListener('text_delta', (e) => {
    const delta = JSON.parse(e.data);
    editor.insertText(delta.text);  // Animate character by character
});

eventSource.addEventListener('turnblock_catchup', (e) => {
    const block = JSON.parse(e.data);
    editor.insertText(block.content);  // Insert entire block at once
});
```

### Server Side

```go
// Set up catchup function for database-backed replay
stream := mstream.NewStream(id, workFunc,
    mstream.WithCatchup(func(streamID, lastEventID string) ([]mstream.Event, error) {
        // Query database for turnblocks after lastEventID
        turnblocks := db.GetTurnBlocksAfter(streamID, lastEventID)

        // Return aggregated catchup events
        events := []mstream.Event{}
        for i, block := range turnblocks {
            data, _ := json.Marshal(block)
            events = append(events, mstream.Event{
                ID:   fmt.Sprintf("catchup-%d", i+1),
                Type: "turnblock_catchup",
                Data: data,
            })
        }
        return events, nil
    }),
)

// Option 1: PersistAndClear (recommended - prevents race conditions)
err := stream.PersistAndClear(func(events []mstream.Event) error {
    return db.SaveTurnBlock(turnID, events)
})

// Option 2: Manual persist-then-clear (if you need custom error handling)
events := stream.SnapshotBuffer()
if err := db.SaveTurnBlock(turnID, events); err != nil {
    // Handle error - buffer NOT cleared yet
    return err
}
stream.ClearBuffer()  // Only clear after successful persist

// The adapter automatically handles reconnection:
http.HandleFunc("GET /streams/{id}", nethttpadapter.Handler(registry))
```

### Catchup Flow

```
Client reconnects with Last-Event-ID: 5

1. Check in-memory buffer
   - If event 5 found → return events 6-10 from buffer
   - If event 5 NOT found (cleared) → call catchup function

2. Catchup function queries database
   - Returns aggregated blocks as special "catchup" events
   - IDs: "catchup-1", "catchup-2", etc.

3. Append current buffer
   - Add any new events that arrived during catchup

4. Send to client:
   id: catchup-1
   event: turnblock_catchup
   data: {"content": "aggregated text"}

   id: catchup-2
   event: turnblock_catchup
   data: {"content": "more aggregated text"}

   id: 15
   event: text_delta
   data: {"delta": "H"}

   id: 16
   event: text_delta
   data: {"delta": "i"}
```

### Manual Buffer Management

```go
// Get events (with automatic catchup fallback)
events := stream.GetCatchupEvents("event-42")

// Check buffer size
size := stream.BufferSize()

// Clear buffer after persisting to DB
stream.ClearBuffer()
```

## Advanced Features

### DEBUG Mode

Enable sequential event IDs for debugging and development:

```bash
# Enable DEBUG mode
export DEBUG=true
```

**When enabled:**
- Events get sequential IDs: `"1"`, `"2"`, `"3"`, etc.
- Helps trace exact event order in logs
- Useful for debugging race conditions and event sequencing

**In production:**
- Set `DEBUG=false` (or omit the variable)
- No event IDs generated (better performance, simpler)
- Block sequence numbers provide ordering when needed

**Example output:**

```
DEBUG=true:
event: block_start
id: 1
data: {"block_index": 0}

event: block_delta
id: 2
data: {"text": "Hello"}

DEBUG=false:
event: block_start
data: {"block_index": 0}

event: block_delta
data: {"text": "Hello"}
```

### Thread Safety Guarantees

The library provides multiple levels of thread safety:

#### 1. Buffer Operations (RWMutex)
All buffer operations are thread-safe:
- `Add()`, `GetAll()`, `GetSince()`, `Clear()` protected by RWMutex
- Safe for concurrent reads and writes
- Returns copies, not references (prevents external modification)

#### 2. Catchup Coordination (Catchup Mutex)
Prevents races between catchup and buffer operations:
- `GetCatchupEvents()` is atomic with respect to buffer operations
- `PersistAndClear()` guarantees no race between persist and clear
- Ensures consistent view of database + buffer state

```go
// Internal implementation (automatic)
func (s *Stream) GetCatchupEvents(lastEventID string) []Event {
    s.catchupMu.Lock()
    defer s.catchupMu.Unlock()

    // Database query + buffer read now atomic
    catchupEvents := s.catchupFunc(streamID, lastEventID)
    currentBuffer := s.buffer.GetAll()
    return append(catchupEvents, currentBuffer...)
}
```

#### 3. Multi-Client Broadcasting
Safe for concurrent clients:
- Multiple clients can connect to same stream
- Events broadcast atomically to all clients
- Automatic cleanup of disconnected clients

### Race Condition Prevention

**Problem:** Buffer cleared before database commit completes

Without proper coordination, this race can occur:

```
T0: Block completes, DB write starts
T1: Buffer cleared ← TOO EARLY!
T2: Client reconnects
T3: Catchup queries DB (not committed yet)
T4: Client gets NOTHING (data loss!)
T5: DB commit completes (too late)
```

**Solution: Atomic PersistAndClear**

```go
// ❌ BAD: Race condition possible
events := stream.SnapshotBuffer()
db.Save(events)
stream.ClearBuffer()  // May clear before DB commit!

// ✅ GOOD: Atomic persist-and-clear
stream.PersistAndClear(func(events []mstream.Event) error {
    return db.Save(events)  // Buffer cleared ONLY if this succeeds
})
```

**How it works:**

1. **Catchup mutex locks** - Prevents concurrent catchup during persist
2. **Persist executes** - Your function commits to database
3. **On success** - Buffer cleared automatically
4. **On failure** - Buffer retained, error returned
5. **Mutex unlocks** - Catchup can now proceed

**Timeline with fix:**

```
T0: Block completes, PersistAndClear called
T1: Catchup mutex locked
T2: DB write executes
T3: Client reconnects (waits for mutex)
T4: DB commit succeeds
T5: Buffer cleared
T6: Mutex released
T7: Client catchup executes (sees committed data)
```

### Configuration Options

```go
stream := mstream.NewStream(id, workFunc,
    // Catchup function for database-backed replay
    mstream.WithCatchup(func(streamID, lastEventID string) ([]mstream.Event, error) {
        blocks := db.GetTurnBlocksAfter(streamID, lastEventID)
        return toEvents(blocks), nil
    }),

    // Client channel buffer size
    mstream.WithBufferSize(100),

    // Stream timeout (auto-cleanup)
    mstream.WithTimeout(5*time.Minute),

    // Completion callback
    mstream.WithOnComplete(func(id string) {
        log.Printf("Stream %s completed", id)
    }),

    // Enable event IDs (DEBUG mode)
    mstream.WithEventIDs(os.Getenv("DEBUG") == "true"),
)
```

## Use Cases

- **LLM streaming** - Stream AI responses to users
- **Real-time logs** - Tail logs over HTTP
- **Live dashboards** - Metrics and analytics streaming
- **Progress updates** - Long-running job status
- **Chat applications** - Real-time messaging

## Configuration

```go
stream := mstream.NewStream(id, workFunc,
    mstream.WithBufferSize(100),           // Client channel buffer
    mstream.WithTimeout(5*time.Minute),    // Stream timeout
    mstream.WithOnComplete(func(id string) {
        log.Printf("Stream %s completed", id)
    }),
)
```

## Status

⚠️ **Alpha** - Extracted from production code at [Meridian](https://meridian-flow.com). API may change.

## Contributing

PRs welcome! This is primarily maintained for Meridian's needs, but happy to accept improvements.

## License

MIT

