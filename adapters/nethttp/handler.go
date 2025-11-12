package nethttp

import (
	"bufio"
	"net/http"

	mstream "github.com/haowjy/meridian-stream"
)

// Handler creates an http.Handler for SSE streaming
func Handler(registry *mstream.Registry, opts ...mstream.SSEOption) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Extract stream ID from URL path (e.g., /streams/{id})
		// You can use chi, gorilla/mux, or parse manually
		streamID := r.PathValue("id") // Go 1.22+ routing

		if streamID == "" {
			http.Error(w, "stream ID required", http.StatusBadRequest)
			return
		}

		// Get stream from registry
		stream := registry.Get(streamID)
		if stream == nil {
			http.Error(w, "stream not found", http.StatusNotFound)
			return
		}

		// Set SSE headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		// Get flusher
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		// Create buffered writer that implements SSEWriter
		bw := &bufioFlusher{
			Writer:  bufio.NewWriter(w),
			flusher: flusher,
		}

		// Parse Last-Event-ID header for reconnection catchup
		lastEventID := r.Header.Get("Last-Event-ID")
		if lastEventID != "" {
			opts = append(opts, mstream.WithLastEventID(lastEventID))
		}

		// Stream via SSE
		mstream.StreamSSE(r.Context(), bw, stream, opts...)
	}
}

// bufioFlusher wraps bufio.Writer to implement SSEWriter
type bufioFlusher struct {
	*bufio.Writer
	flusher http.Flusher
}

func (b *bufioFlusher) Flush() error {
	if err := b.Writer.Flush(); err != nil {
		return err
	}
	b.flusher.Flush()
	return nil
}
