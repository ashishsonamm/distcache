package cache

import (
	"container/list"
	"sync"
	"time"
)

type LRUCache struct {
	capacity     int
	items        map[string]*list.Element
	evictionList *list.List
	mu           sync.RWMutex
	stats        Stats
}

type lruItem struct {
	key        string
	value      interface{}
	expiration time.Time
}

func NewLRUCache(capacity int) Cache {
	return &LRUCache{
		capacity:     capacity,
		items:        make(map[string]*list.Element),
		evictionList: list.New(),
	}
}

func (c *LRUCache) Set(key string, value interface{}, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration time.Time
	if duration > 0 {
		expiration = time.Now().Add(duration)
	}

	if el, ok := c.items[key]; ok {
		c.evictionList.MoveToFront(el)
		item := el.Value.(*lruItem)
		item.value = value
		item.expiration = expiration
		return
	}

	if c.evictionList.Len() >= c.capacity {
		c.evict()
	}

	item := &lruItem{key: key, value: value, expiration: expiration}
	element := c.evictionList.PushFront(item)
	c.items[key] = element
	c.stats.Count++
}

func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if el, ok := c.items[key]; ok {
		item := el.Value.(*lruItem)
		if !item.expiration.IsZero() && time.Now().After(item.expiration) {
			c.stats.Misses++
			return nil, false
		}
		c.evictionList.MoveToFront(el)
		c.stats.Hits++
		return item.value, true
	}

	c.stats.Misses++
	return nil, false
}

func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.evictionList.Remove(el)
		delete(c.items, key)
		c.stats.Count--
	}
}

func (c *LRUCache) evict() {
	if el := c.evictionList.Back(); el != nil {
		item := el.Value.(*lruItem)
		delete(c.items, item.key)
		c.evictionList.Remove(el)
		c.stats.Count--
	}
}

func (c *LRUCache) GetStats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *LRUCache) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for e := c.evictionList.Front(); e != nil; {
		item := e.Value.(*lruItem)
		if item.expiration.IsZero() || item.expiration.After(now) {
			break
		}
		next := e.Next()
		c.evictionList.Remove(e)
		delete(c.items, item.key)
		c.stats.Count--
		e = next
	}
}
