package cache

import (
	"container/heap"
	"sync"
	"time"
)

type LFUCache struct {
	capacity int
	items    map[string]*lfuItem
	freq     *priorityQueue
	mu       sync.RWMutex
	stats    Stats
}

type lfuItem struct {
	key        string
	value      interface{}
	frequency  int
	expiration time.Time
	index      int
}

type priorityQueue []*lfuItem

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].frequency < pq[j].frequency || (pq[i].frequency == pq[j].frequency && pq[i].expiration.Before(pq[j].expiration))
}
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*lfuItem)
	item.index = n
	*pq = append(*pq, item)
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

func NewLFUCache(capacity int) Cache {
	return &LFUCache{
		capacity: capacity,
		items:    make(map[string]*lfuItem),
		freq:     &priorityQueue{},
	}
}

func (c *LFUCache) Set(key string, value interface{}, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration time.Time
	if duration > 0 {
		expiration = time.Now().Add(duration)
	}

	if item, ok := c.items[key]; ok {
		item.value = value
		item.expiration = expiration
		item.frequency++
		heap.Fix(c.freq, item.index)
		return
	}

	if len(c.items) >= c.capacity {
		c.evict()
	}

	item := &lfuItem{
		key:        key,
		value:      value,
		frequency:  1,
		expiration: expiration,
	}
	heap.Push(c.freq, item)
	c.items[key] = item
	c.stats.Count++
}

func (c *LFUCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if item, ok := c.items[key]; ok {
		if !item.expiration.IsZero() && time.Now().After(item.expiration) {
			c.stats.Misses++
			return nil, false
		}
		item.frequency++
		heap.Fix(c.freq, item.index)
		c.stats.Hits++
		return item.value, true
	}

	c.stats.Misses++
	return nil, false
}

func (c *LFUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if item, ok := c.items[key]; ok {
		heap.Remove(c.freq, item.index)
		delete(c.items, key)
		c.stats.Count--
	}
}

func (c *LFUCache) evict() {
	if c.freq.Len() > 0 {
		item := heap.Pop(c.freq).(*lfuItem)
		delete(c.items, item.key)
		c.stats.Count--
	}
}

func (c *LFUCache) GetStats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *LFUCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for c.freq.Len() > 0 {
		item := c.freq.Peek().(*lfuItem)
		if item.expiration.IsZero() || item.expiration.After(now) {
			break
		}
		heap.Pop(c.freq)
		delete(c.items, item.key)
		c.stats.Count--
	}
}

func (pq priorityQueue) Peek() interface{} {
	if len(pq) == 0 {
		return nil
	}
	return pq[0]
}
