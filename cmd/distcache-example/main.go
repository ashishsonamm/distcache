package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ashishsonamm/distcache/pkg/distcache"
)

func main() {
	nodes := []string{"localhost:8091", "localhost:8092", "localhost:8093", "localhost:8094", "localhost:8095", "localhost:8096"}
	// Create configurations for three nodes
	var configs []distcache.Config
	for _, node := range nodes {
		config := distcache.Config{
			NodeID:            node,
			Nodes:             nodes,
			DefaultExpiration: 5 * time.Minute,
			CleanupInterval:   10 * time.Minute,
			VirtualNodes:      100,
			ReplicationFactor: 2,
			Capacity:          1000,
			EvictionPolicy:    distcache.LRU,
		}
		configs = append(configs, config)
	}

	// Create cache instances and start HTTP servers
	caches := make([]*distcache.DistCache, len(configs))
	var wg sync.WaitGroup
	for i, config := range configs {
		wg.Add(1)
		go func(i int, cfg distcache.Config) {
			defer wg.Done()
			cache := distcache.NewDistCache(cfg)
			caches[i] = cache
			defer cache.Stop()

			server := &http.Server{
				Addr:    cfg.NodeID,
				Handler: cache,
			}

			log.Printf("Starting server on %s", cfg.NodeID)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("Could not listen on %s: %v", cfg.NodeID, err)
			}
		}(i, config)
	}

	// Wait for servers to start
	time.Sleep(2 * time.Second)

	//testPrimaryNodeDown(caches)

	// demonstrate race conditions
	runConcurrentUpdateTest(caches)
	// dc := caches[0]

	// go func() {
	// 	time.Sleep(10 * time.Millisecond)
	// 	err := dc.Set("user_123", "Alice", 1*time.Hour)
	// 	if err != nil {
	// 		log.Printf("Error setting Alice: %v", err)
	// 	}
	// 	fmt.Println("Set Alice")
	// }()

	// go func() {
	// 	time.Sleep(10 * time.Millisecond)
	// 	err := dc.Set("user_123", "Bob", 1*time.Hour)
	// 	if err != nil {
	// 		log.Printf("Error setting Bob: %v", err)
	// 	}
	// 	fmt.Println("Set Bob")
	// }()

	// // Wait a bit for the operations to complete
	// time.Sleep(100 * time.Millisecond)

	// // Read the value
	// value, found, err := dc.Get("user_123")
	// if err != nil {
	// 	log.Printf("Error getting user_123: %v", err)
	// } else if found {
	// 	fmt.Printf("Final value for user_123: %v\n", value)
	// } else {
	// 	fmt.Println("user_123 not found")
	// }

	// // Check values on all nodes
	// for i, cache := range caches {
	// 	value, found, err := cache.Get("user_123")
	// 	if err != nil {
	// 		log.Printf("Error getting user_123 from node %d: %v", i, err)
	// 	} else if found {
	// 		fmt.Printf("Value on node %d: %v\n", i, value)
	// 	} else {
	// 		fmt.Printf("user_123 not found on node %d\n", i)
	// 	}
	// }

	// // Demonstrate setting and getting values
	// keys := []string{"2456hw56sfdhwth", "argeqweryh2445rg", "dhf2456yh64nabdbs", "ryjs245ygwebmergwy54argg", "wh425yten356jh", "lkdsjfowieur285", "q1092785asdfklh9", "29384falsjlkfhoi2", "1jfalk89223j", "2kf8924rjio4fu89"}
	// for i, key := range keys {
	// 	value := fmt.Sprintf("value for %s", key)
	// 	// Use different nodes to set values
	// 	cache := caches[i%len(caches)]
	// 	node := cache.GetNodeForKey(key)
	// 	fmt.Printf("Key %s hashes to node %s\n", key, node)
	// 	err := cache.Set(key, value, 100*time.Minute)
	// 	if err != nil {
	// 		log.Printf("Error setting %s: %v", key, err)
	// 	} else {
	// 		fmt.Printf("Set %s to %s using node %s\n", key, value, cache.GetNodeID())
	// 	}
	// }

	// // Try getting values from different nodes
	// for i, key := range keys {
	// 	// Use a different node to get each value
	// 	cache := caches[(i+1)%len(caches)]
	// 	value, found, err := cache.Get(key)
	// 	if err != nil {
	// 		log.Printf("Error getting %s from node %s: %v", key, cache.GetNodeID(), err)
	// 	} else if found {
	// 		fmt.Printf("Got %s = %v from node %s\n", key, value, cache.GetNodeID())
	// 	} else {
	// 		fmt.Printf("%s not found when querying node %s\n", key, cache.GetNodeID())
	// 	}
	// }

	// Keep the main goroutine running
	wg.Wait()
}

func testPrimaryNodeDown(caches []*distcache.DistCache) {
	primaryNode := caches[1] // Assuming the first node is primary
	key := "test_key"
	value := "test_value"

	// Set a value before simulating node down
	err := primaryNode.Set(key, value, 1*time.Hour)
	if err != nil {
		log.Printf("Failed to set initial value: %v", err)
		return
	}

	// Simulate primary node down
	for _, cache := range caches {
		cache.SimulateDown(primaryNode.GetNodeID())
	}
	//primaryNode.SimulateDown()
	fmt.Println("Primary node is down")

	// Try to set a new value
	err = caches[1].Set(key, "new_value", 1*time.Hour)
	if err != nil {
		fmt.Printf("Set operation failed as expected: %v\n", err)
	} else {
		fmt.Println("Set operation unexpectedly succeeded")
	}

	go func() {
		for _, cache := range caches {
			cache.SimulateUp(primaryNode.GetNodeID())
		}
		fmt.Println("Primary node is back up")
	}()

	// Try to get the value
	for i, c := range caches {
		value, found, err := c.Get(key)
		if err != nil {
			fmt.Printf("Get from cache %d failed: %v\n", i, err)
		} else if found {
			fmt.Printf("Value found in cache %d: %v\n", i, value)
		} else {
			fmt.Printf("Value not found in cache %d\n", i)
		}
	}

	// Simulate primary node back up
	for _, cache := range caches {
		cache.SimulateUp(primaryNode.GetNodeID())
	}
	fmt.Println("Primary node is back up")
	//primaryNode.SimulateUp()

	// Try operations again
	err = primaryNode.Set(key, "after_recovery", 1*time.Hour)
	if err != nil {
		fmt.Printf("Set after recovery failed: %v\n", err)
	} else {
		fmt.Println("Set after recovery succeeded")
	}

	for i, c := range caches {
		value, found, err := c.Get(key)
		if err != nil {
			fmt.Printf("Get from cache %d after recovery failed: %v\n", i, err)
		} else if found {
			fmt.Printf("Value found in cache %d after recovery: %v\n", i, value)
		} else {
			fmt.Printf("Value not found in cache %d after recovery\n", i)
		}
	}
}

func runConcurrentUpdateTest(caches []*distcache.DistCache) {
	key := "user_123"
	iterations := 10
	aliceCount := 0
	bobCount := 0

	for i := 0; i < iterations; i++ {
		wg := sync.WaitGroup{}
		wg.Add(2)

		//go func() {
		//	defer wg.Done()
		//	err := caches[0].Set(key, "Alice", 1*time.Hour)
		//	if err != nil {
		//		return
		//	}
		//}()
		//
		//go func() {
		//	defer wg.Done()
		//	err := caches[1].Set(key, "Bob", 1*time.Hour)
		//	if err != nil {
		//		return
		//	}
		//}()

		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			err := caches[0].Set("user_123", "Alice", 1*time.Hour)
			if err != nil {
				log.Printf("Error setting Alice: %v", err)
			}
			fmt.Println("Set Alice")
		}()

		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			err := caches[1].Set("user_123", "Bob", 1*time.Hour)
			if err != nil {
				log.Printf("Error setting Bob: %v", err)
			}
			fmt.Println("Set Bob")
		}()

		wg.Wait()
		time.Sleep(100 * time.Millisecond)

		value, _, _ := caches[2].Get(key)
		if value == "Alice" {
			aliceCount++
		} else if value == "Bob" {
			bobCount++
		}
	}

	fmt.Printf("After %d iterations:\n", iterations)
	fmt.Printf("Alice: %d\n", aliceCount)
	fmt.Printf("Bob: %d\n", bobCount)
}
