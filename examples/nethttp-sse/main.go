package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	mstream "github.com/haowjy/meridian-stream"
	nethttpadapter "github.com/haowjy/meridian-stream/adapters/nethttp"
)

func main() {
	registry := mstream.NewRegistry(
		mstream.WithCleanupInterval(1 * time.Minute),
		mstream.WithRetentionPeriod(10 * time.Minute),
	)

	// Start cleanup goroutine
	go registry.StartCleanup(context.Background())

	// Home page
	http.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `
<!DOCTYPE html>
<html>
<head>
    <title>Resilient Stream Demo</title>
</head>
<body>
    <h1>Resilient Stream Demo (net/http)</h1>
    <button onclick="startStream()">Start Stream</button>
    <div id="output"></div>

    <script>
        let eventSource;

        async function startStream() {
            // Create stream
            const res = await fetch('/streams', { method: 'POST' });
            const data = await res.json();

            console.log('Stream created:', data);
            document.getElementById('output').innerHTML = '<h2>Events:</h2>';

            // Connect to SSE
            eventSource = new EventSource(data.stream_url);

            eventSource.addEventListener('counter', (e) => {
                const div = document.createElement('div');
                div.textContent = 'Event ' + e.lastEventId + ': ' + e.data;
                document.getElementById('output').appendChild(div);
            });

            eventSource.addEventListener('complete', (e) => {
                const div = document.createElement('div');
                div.textContent = 'Stream complete!';
                div.style.fontWeight = 'bold';
                document.getElementById('output').appendChild(div);
                eventSource.close();
            });

            eventSource.onerror = (e) => {
                console.error('SSE error:', e);
                eventSource.close();
            };
        }
    </script>
</body>
</html>
        `)
	})

	// Create stream endpoint
	http.HandleFunc("POST /streams", func(w http.ResponseWriter, r *http.Request) {
		streamID := uuid.New().String()

		stream := mstream.NewStream(streamID, func(ctx context.Context, send func(mstream.Event)) error {
			// Send 20 events
			for i := 0; i < 20; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					data, _ := json.Marshal(map[string]interface{}{
						"count": i,
						"time":  time.Now().Format(time.RFC3339),
					})

					send(mstream.Event{
						Data: data,
						ID:   fmt.Sprintf("%d", i),
						Type: "counter",
					})

					time.Sleep(500 * time.Millisecond)
				}
			}

			// Send completion event
			send(mstream.Event{
				Data: []byte(`{"status":"complete"}`),
				Type: "complete",
			})

			return nil
		},
			mstream.WithBufferSize(100),
			mstream.WithTimeout(30*time.Second),
		)

		if err := registry.Register(stream); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		stream.Start()

		log.Printf("Stream created: %s", streamID)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"stream_id":  streamID,
			"stream_url": fmt.Sprintf("/streams/%s", streamID),
		})
	})

	// SSE streaming endpoint
	http.HandleFunc("GET /streams/{id}", nethttpadapter.Handler(registry,
		mstream.WithKeepalive(15*time.Second),
	))

	// Get stream status
	http.HandleFunc("GET /streams/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.PathValue("id")

		stream := registry.Get(streamID)
		if stream == nil {
			http.Error(w, "stream not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"stream_id":    streamID,
			"status":       stream.Status(),
			"client_count": stream.ClientCount(),
		})
	})

	// Cancel stream
	http.HandleFunc("POST /streams/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		streamID := r.PathValue("id")

		stream := registry.Get(streamID)
		if stream == nil {
			http.Error(w, "stream not found", http.StatusNotFound)
			return
		}

		stream.Cancel()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"status":  stream.Status(),
		})
	})

	log.Println("Server starting on http://localhost:3000")
	log.Println("Open http://localhost:3000 in your browser")

	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatal(err)
	}
}
