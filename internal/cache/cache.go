package cache

import (
	"time"
)

type Cache interface {
	Set(key string, value interface{}, duration time.Duration)
	Get(key string) (interface{}, bool)
	Delete(key string)
	GetStats() Stats
}

type Stats struct {
	Hits   int64
	Misses int64
	Count  int
}

// Helper function to start the server (implementation will be in concrete types)
// func StartServer(c Cache, addr string) error {
// 	mux := http.NewServeMux()

// 	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
// 		key := r.URL.Query().Get("key")
// 		value, found := c.Get(key)
// 		if !found {
// 			http.Error(w, "Key not found", http.StatusNotFound)
// 			return
// 		}
// 		// Implement response writing logic here
// 		w.Header().Set("Content-Type", "application/json")
// 		response := map[string]interface{}{
// 			"key":   key,
// 			"value": value,
// 		}
// 		if err := json.NewEncoder(w).Encode(response); err != nil {
// 			http.Error(w, "Error encoding response", http.StatusInternalServerError)
// 			return
// 		}

// 	})

// 	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
// 		key := r.URL.Query().Get("key")
// 		value := r.URL.Query().Get("value")
// 		duration, err := time.ParseDuration(r.URL.Query().Get("duration"))
// 		if err != nil {
// 			duration = 0 // Use default expiration
// 		}
// 		c.Set(key, value, duration)
// 		w.WriteHeader(http.StatusOK)
// 	})

// 	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
// 		key := r.URL.Query().Get("key")
// 		c.Delete(key)
// 		w.WriteHeader(http.StatusOK)
// 	})

// 	return http.ListenAndServe(addr, mux)
// }
