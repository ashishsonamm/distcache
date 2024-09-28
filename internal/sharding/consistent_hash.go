package sharding

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type ConsistentHash struct {
	circle       map[uint32]string
	sortedHashes []uint32
	nodes        map[string]bool
	virtualNodes int
	mutex        sync.RWMutex
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
	return &ConsistentHash{
		circle:       make(map[uint32]string),
		nodes:        make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

func (c *ConsistentHash) Add(node string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.nodes[node]; exists {
		return
	}

	c.nodes[node] = true

	for i := 0; i < c.virtualNodes; i++ {
		hash := c.hashKey(node + strconv.Itoa(i))
		c.circle[hash] = node
		c.sortedHashes = append(c.sortedHashes, hash)
	}

	sort.Slice(c.sortedHashes, func(i, j int) bool {
		return c.sortedHashes[i] < c.sortedHashes[j]
	})
}

func (c *ConsistentHash) Remove(node string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, exists := c.nodes[node]; !exists {
		return
	}

	delete(c.nodes, node)

	for i := 0; i < c.virtualNodes; i++ {
		hash := c.hashKey(node + strconv.Itoa(i))
		delete(c.circle, hash)
		for i, h := range c.sortedHashes {
			if h == hash {
				c.sortedHashes = append(c.sortedHashes[:i], c.sortedHashes[i+1:]...)
				break
			}
		}
	}
}

func (c *ConsistentHash) Get(key string) string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.circle) == 0 {
		return ""
	}

	hash := c.hashKey(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= hash
	})

	if idx == len(c.sortedHashes) {
		idx = 0
	}

	return c.circle[c.sortedHashes[idx]]
}

func (c *ConsistentHash) hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
