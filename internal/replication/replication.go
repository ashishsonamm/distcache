package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ashishsonamm/distcache/internal/sharding"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ashishsonamm/distcache/internal/cache"
)

const defaultVirtualNodesCount int = 100

type Replicator struct {
	nodeID            string
	nodes             []string
	replicationFactor int
	cache             *cache.Cache
	client            *http.Client
	consistentHash    *sharding.ConsistentHash
	logger            *log.Logger
	isNodeDown        func(nodeID string) bool
}

func NewReplicator(nodeID string, nodes []string, replicationFactor int, cache *cache.Cache, consistentHash *sharding.ConsistentHash, isNodeDown func(string) bool) *Replicator {
	return &Replicator{
		nodeID:            nodeID,
		nodes:             nodes,
		replicationFactor: replicationFactor,
		cache:             cache,
		client:            &http.Client{Timeout: 5 * time.Second},
		consistentHash:    consistentHash,
		logger:            log.New(log.Writer(), fmt.Sprintf("[Replicator %s] ", nodeID), log.LstdFlags),
		isNodeDown:        isNodeDown,
	}
}

func (r *Replicator) ReplicateSet(key string, value interface{}, duration time.Duration) error {
	replicaNodes := r.GetReplicaNodes(key)
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicaNodes))

	for _, node := range replicaNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if err := r.sendSetRequest(node, key, value, duration, false); err != nil {
				errChan <- fmt.Errorf("failed to replicate to %s: %w", node, err)
			}
		}(node)
	}
	// wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// check if any error occurred
	for err := range errChan {
		if err != nil {
			r.logger.Printf("Error occured in replicating: %v", err)
			return err
		}
	}
	return nil
}

func (r *Replicator) ReplicateDelete(key string) error {
	replicaNodes := r.GetReplicaNodes(key)
	for _, node := range replicaNodes {
		if err := r.sendDeleteRequest(node, key); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) ForwardSet(node, key string, value interface{}, duration time.Duration, replicate bool) error {
	return r.sendSetRequest(node, key, value, duration, replicate)
}

func (r *Replicator) ForwardGet(node, key string) (interface{}, bool, error) {
	if r.isNodeDown(node) {
		r.logger.Printf("%s - Node is down", node)
		return nil, false, errors.New("node is down")
	}
	url := fmt.Sprintf("http://%s/get?key=%s", node, key)
	resp, err := r.client.Get(url)
	if err != nil {
		r.logger.Printf("Error in sending get request, err - %v", err)
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}

	var result struct {
		Value interface{} `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		r.logger.Printf("Error in decoding, err - %v", err)
		return nil, false, err
	}

	return result.Value, true, nil
}

func (r *Replicator) ForwardDelete(node, key string) error {
	return r.sendDeleteRequest(node, key)
}

func (r *Replicator) UpdateNodes(nodes []string) {
	r.nodes = nodes
}

func (r *Replicator) GetReplicaNodes(key string) []string {
	var replicaNodes []string
	nodeCount := len(r.nodes)

	// Get the primary node for this key
	primaryNode := r.consistentHash.Get(key)

	// Find the index of the primary node in the nodes slice
	primaryIndex := -1
	for i, node := range r.nodes {
		if node == primaryNode {
			primaryIndex = i
			break
		}
	}

	if primaryIndex == -1 {
		// This shouldn't happen, but let's handle it just in case
		r.logger.Printf("Error: Primary node %s not found in nodes list", primaryNode)
		return replicaNodes
	}

	// Select replica nodes by moving clockwise around the ring
	for i := 1; len(replicaNodes) < r.replicationFactor && len(replicaNodes) < nodeCount-1; i++ {
		nextIndex := (primaryIndex + i) % nodeCount
		nextNode := r.nodes[nextIndex]
		if nextNode != r.nodeID {
			if !r.isNodeDown(nextNode) {
				replicaNodes = append(replicaNodes, nextNode)
			}
		}
	}

	return replicaNodes
}

func (r *Replicator) sendSetRequest(node, key string, value interface{}, duration time.Duration, replicate bool) error {
	if r.isNodeDown(node) {
		r.logger.Printf("%s - Node is down", node)
		return fmt.Errorf("node %s is down", node)
	}
	url := fmt.Sprintf("http://%s/set", node)
	data := map[string]interface{}{
		"key":       key,
		"value":     value,
		"duration":  duration.Seconds(),
		"replicate": replicate,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		r.logger.Printf("Error in marshaling json data, err - %v", err)
		return err
	}

	resp, err := r.client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		r.logger.Printf("Error in sending post request, err - %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r.logger.Printf("Error in deleting value")
		return errors.New("failed to set value on replica")
	}

	return nil
}

func (r *Replicator) sendDeleteRequest(node, key string) error {
	if r.isNodeDown(node) {
		r.logger.Printf("%s - Node is down", node)
		return fmt.Errorf("node %s is down", node)
	}
	url := fmt.Sprintf("http://%s/delete?key=%s", node, key)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		r.logger.Printf("Error in creating new delete request, err - %v", err)
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		r.logger.Printf("Error in sending request, err - %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r.logger.Printf("Error in deleting value")
		return errors.New("failed to delete value on replica")
	}

	return nil
}
