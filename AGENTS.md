# meridian-stream-go — Agent Instructions

## Public Library Notice

This is a **public Go module** (`github.com/haowjy/meridian-stream-go`). External consumers depend on it. Be mindful of breaking changes to exported types, interfaces, and function signatures.

## Architecture

```
stream.go          -> Stream struct (core: goroutine lifecycle, event dispatch)
event.go           -> StreamEvent type (ID, Type, Data)
buffer.go          -> EventBuffer (in-memory catchup for reconnecting clients)
handler.go         -> SSEHandler (HTTP handler, Last-Event-ID catchup)
registry.go        -> StreamRegistry (manage multiple concurrent streams)
options.go         -> StreamOption functional options
interjection.go    -> InterjectionBuffer (user input during streaming)
adapters/          -> Framework adapters (net/http, etc.)
examples/          -> Runnable usage examples
```

## Key Design Principles

- **One stream, many clients** — A single goroutine produces events; multiple SSE connections consume them via the handler.
- **Reconnection-safe** — Clients send `Last-Event-ID`; the buffer replays missed events. No data loss on disconnect.
- **Framework agnostic** — Core types have no HTTP dependency. `handler.go` and `adapters/` bridge to HTTP frameworks.
- **Goroutine lifecycle** — Streams clean up automatically when the producer finishes or all clients disconnect.

## Development Commands

```bash
make test       # Run all tests
make examples   # Build example binaries
make clean      # Remove built examples
```

## Conventions

- Exported types live at package root (`meridianstream`)
- Tests use `_test.go` suffix, same package
- `integration_test.go` tests multi-client reconnection scenarios end-to-end
