package distcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ashishsonamm/distcache/internal/cache"
	"github.com/ashishsonamm/distcache/internal/replication"
	"github.com/ashishsonamm/distcache/internal/sharding"
)

type EvictionPolicy string

const (
	LRU EvictionPolicy = "LRU"
	LFU EvictionPolicy = "LFU"
)

type DistCache struct {
	cache          cache.Cache
	consistentHash *sharding.ConsistentHash
	replicator     *replication.Replicator
	nodeID         string
	nodes          []string
	logger         *log.Logger
	stopCleanup    chan struct{} // Channel to signal stopping of cleanup routine
	nodesStatus    sync.Map      // for testing - to store the status of nodes
}

type Config struct {
	NodeID            string
	Nodes             []string
	DefaultExpiration time.Duration
	CleanupInterval   time.Duration
	VirtualNodes      int
	ReplicationFactor int
	Capacity          int
	EvictionPolicy    EvictionPolicy
}

func NewDistCache(config Config) *DistCache {
	dc := &DistCache{
		consistentHash: sharding.NewConsistentHash(config.VirtualNodes),
		nodeID:         config.NodeID,
		nodes:          config.Nodes,
		logger:         log.New(log.Writer(), fmt.Sprintf("[DistCache %s] ", config.NodeID), log.LstdFlags),
		stopCleanup:    make(chan struct{}), // Initialize stopCleanup channel
	}

	for _, node := range config.Nodes {
		dc.consistentHash.Add(node)
		dc.nodesStatus.Store(node, true)
	}

	switch config.EvictionPolicy {
	case LRU:
		dc.cache = cache.NewLRUCache(config.Capacity)
	case LFU:
		dc.cache = cache.NewLFUCache(config.Capacity)
	default:
		log.Fatalf("Unsupported eviction policy: %s", config.EvictionPolicy)
	}

	dc.replicator = replication.NewReplicator(dc.nodeID, dc.nodes, config.ReplicationFactor, &dc.cache, dc.consistentHash,
		func(nodeID string) bool {
			val, _ := dc.nodesStatus.Load(nodeID)
			return !val.(bool)
		},
	)

	go dc.startCleanupTimer(config.CleanupInterval)

	return dc
}

func (dc *DistCache) startCleanupTimer(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dc.cleanup()
		case <-dc.stopCleanup: // Exit when stopCleanup signal is received
			dc.logger.Println("Stopping cleanup process")
			return
		}
	}
}

func (dc *DistCache) cleanup() {
	dc.logger.Println("Starting cleanup")
	if cleanable, ok := dc.cache.(interface {
		Cleanup()
	}); ok {
		cleanable.Cleanup()
	}
	dc.logger.Println("Cleanup completed")
}

// Stop method to signal stopping of the cleanup routine
func (dc *DistCache) Stop() {
	close(dc.stopCleanup) // Signal to stop the cleanup process
	dc.logger.Println("Cache stopped and cleanup stopped")
}

func (dc *DistCache) SimulateDown(nodeID string) {
	dc.nodesStatus.Store(nodeID, false)
}

func (dc *DistCache) SimulateUp(nodeID string) {
	dc.nodesStatus.Store(nodeID, true)
}

func (dc *DistCache) IsDown(nodeID string) bool {
	val, _ := dc.nodesStatus.Load(nodeID)
	return !val.(bool)
}

func (dc *DistCache) getReplicaNodes(key string) []string {
	return dc.replicator.GetReplicaNodes(key)
}

func (dc *DistCache) Set(key string, value interface{}, duration time.Duration) error {
	if dc.IsDown(dc.nodeID) {
		dc.logger.Printf("%s - Node is down", dc.nodeID)
		return errors.New("node is down")
	}
	if key == "" {
		dc.logger.Printf("Key cannot be empty")
		return fmt.Errorf("key cannot be empty")
	}
	primaryNode := dc.consistentHash.Get(key)
	dc.logger.Printf("Setting key %s on primary node %s", key, primaryNode)

	// Try primary node first, then fall back to other nodes
	nodes := append([]string{primaryNode}, dc.getReplicaNodes(key)...)
	for _, node := range nodes {
		if !dc.IsDown(node) {
			if node == dc.nodeID {
				dc.cache.Set(key, value, duration)
				return dc.replicator.ReplicateSet(key, value, duration)
			}
			err := dc.replicator.ForwardSet(node, key, value, duration, true)
			if err == nil {
				return nil
			}
			dc.logger.Printf("Error forwarding set operation for key %s to node %s: %v", key, node, err)
		}
	}

	// If all nodes are down, set locally as a last resort
	dc.cache.Set(key, value, duration)
	return dc.replicator.ReplicateSet(key, value, duration)
}

func (dc *DistCache) Get(key string) (interface{}, bool, error) {
	if dc.IsDown(dc.nodeID) {
		dc.logger.Printf("%s - Node is down", dc.nodeID)
		return nil, false, errors.New("node is down")
	}
	primaryNode := dc.consistentHash.Get(key)
	dc.logger.Printf("Getting key %s from primary node %s", key, primaryNode)

	// Try primary node and replica nodes
	nodes := append([]string{primaryNode}, dc.getReplicaNodes(key)...)

	resultChan := make(chan struct {
		value interface{}
		found bool
		err   error
	}, len(nodes))

	for _, node := range nodes {
		go func(n string) {
			if !dc.IsDown(n) {
				value, found, err := dc.replicator.ForwardGet(n, key)
				resultChan <- struct {
					value interface{}
					found bool
					err   error
				}{value, found, err}
			} else {
				resultChan <- struct {
					value interface{}
					found bool
					err   error
				}{nil, false, errors.New("node is down")}
			}
		}(node)
	}

	for i := 0; i < len(nodes); i++ {
		result := <-resultChan
		if result.err == nil && result.found {
			return result.value, true, nil
		}
	}

	dc.logger.Printf("Key %s not found on any available node", key)
	return nil, false, errors.New("key not found on any available node")
}

func (dc *DistCache) Delete(key string) error {
	if dc.IsDown(dc.nodeID) {
		dc.logger.Printf("%s - Node is down", dc.nodeID)
		return errors.New("node is down")
	}
	primaryNode := dc.consistentHash.Get(key)
	dc.logger.Printf("Deleting key %s from primary node %s", key, primaryNode)
	if primaryNode == dc.nodeID {
		dc.cache.Delete(key)
		return dc.replicator.ReplicateDelete(key)
	}
	return dc.replicator.ForwardDelete(primaryNode, key)
}

func (dc *DistCache) GetStats() cache.Stats {
	return dc.cache.GetStats()
}

func (dc *DistCache) GetNodeForKey(key string) string {
	return dc.consistentHash.Get(key)
}

func (dc *DistCache) GetNodeID() string {
	return dc.nodeID
}

func (dc *DistCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/set":
		dc.handleSet(w, r)
	case "/get":
		dc.handleGet(w, r)
	default:
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func (dc *DistCache) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		dc.logger.Printf("handleSet, Method not allowed")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Key       string      `json:"key"`
		Value     interface{} `json:"value"`
		Duration  float64     `json:"duration"`
		Replicate bool        `json:"replicate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		dc.logger.Printf("handleSet, Error: %v", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dc.cache.Set(data.Key, data.Value, time.Duration(data.Duration)*time.Second)
	if data.Replicate {
		err := dc.replicator.ReplicateSet(data.Key, data.Value, time.Duration(data.Duration)*time.Second)
		if err != nil {
			dc.logger.Printf("handleSet, Error: %v", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (dc *DistCache) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		dc.logger.Printf("handleGet, Method not allowed")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		dc.logger.Printf("handleGet, Error: key is required")
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	value, found := dc.cache.Get(key)
	if !found {
		dc.logger.Printf("handleGet, Error: key not found")
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"value": value})
}
