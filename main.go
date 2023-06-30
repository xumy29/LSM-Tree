package main

import (
	log "LSM-Tree/log"
	"LSM-Tree/lsmt"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	elems := lsmt.GenerateData(1000000)

	lsmTree := lsmt.NewLSMTree(0)

	block_size := 10000
	index := 0
	fmt.Printf("Adding %d key-value pairs...\n", len(elems))
	for {
		var j int
		for j = index; j < lsmt.Min(index+block_size, len(elems)); j++ {
			lsmTree.Put(elems[j].Key, elems[j].Value)
		}
		index = j
		if index == len(elems) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("The lsmTree now has %d nodes in total\n", lsmTree.TotalSize)

	time.Sleep(5 * time.Second)
	lsmTree.Log_file_info()

	fmt.Printf("Get==1\n")
	log.Logger.Debug("Get==1")
	keys := make([]string, 0)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", rand.Intn(1000))
		keys = append(keys, key)
		val, err := lsmTree.Get(key)
		if err != nil {
			panic(err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("delete all keys with postfix '0'\n")

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%d", i*10)
		lsmTree.Delete(key)
	}
	lsmTree.Log_file_info()

	fmt.Printf("Get==2\n")
	log.Logger.Debug("Get==2")
	for i := 0; i < 10; i++ {
		key := keys[i]
		val, err := lsmTree.Get(key)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		// time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(7 * time.Second)
	lsmTree.Log_file_info()
	fmt.Printf("Get==3\n")
	log.Logger.Debug("Get==3")
	for i := 0; i < 10; i++ {
		key := keys[i]
		val, err := lsmTree.Get(key)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Printf("updates all keys with postfix '7'\n")

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%d", i*10+7)
		lsmTree.Put(key, "updated-"+fmt.Sprintf("value%d", i*10+7))
	}
	lsmTree.Log_file_info()

	fmt.Printf("Get==4\n")
	log.Logger.Debug("Get==4")
	for i := 0; i < 10; i++ {
		key := keys[i]
		val, err := lsmTree.Get(key)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		// time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(7 * time.Second)
	lsmTree.Log_file_info()
	fmt.Printf("Get==5\n")
	log.Logger.Debug("Get==5")
	for i := 0; i < 10; i++ {
		key := keys[i]
		val, err := lsmTree.Get(key)
		if err != nil {
			fmt.Printf("%v\n", err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		time.Sleep(500 * time.Millisecond)
	}

	val, err := lsmTree.Get("key1000001")
	if err != nil {
		fmt.Printf("%v\n", err)
	} else {
		fmt.Printf("search key %v, got value %v\n", "key1000001", val)
	}

}
