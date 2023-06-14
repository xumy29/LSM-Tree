package main

import (
	"LSM-Tree/avlTree"
	"LSM-Tree/lsmt"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	elems := lsmt.GenerateData(1000000)

	lsmTree := lsmt.NewLSMTree(500)
	for i := 0; i < len(elems); i++ {
		lsmTree.Put(elems[i].Key, elems[i].Value)
	}
	fmt.Printf("The lsmTree has %d nodes in total\n", lsmTree.TotalSize)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", rand.Intn(1000))
		val, err := lsmTree.Get(key)
		if err != nil {
			panic(err)
		} else {
			fmt.Printf("search key %v, got value %v\n", key, val)
		}
		time.Sleep(500 * time.Millisecond)
	}

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key%d", rand.Intn(100000))
		lsmTree.Delete(key)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", rand.Intn(1000))
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

	// for i := 0; i < len(elems); i++ {
	// 	lsmTree.Get(elems[i].Key)
	// }
	// time.Sleep(5 * time.Second)

	// for k := 0; k < 10; k++ {
	// 	fmt.Printf("GetData try %d\n", k+1)
	// 	lsmTree.Print()
	// 	for i := 0; i < len(elems); i++ {
	// 		_, err := lsmTree.Get(elems[i].Key)
	// 		if err != nil {
	// 			fmt.Printf("%v\n", err)
	// 			return
	// 		}
	// 	}
	// }
	// testTrees()

}

func testTrees() {
	// 由此可以看出，当key按顺序插入时，avl比binary Tree性能好得多
	for k := 0; k < 5; k++ {
		startTime := time.Now()
		avl := &avlTree.AVLTree{}
		tmp := ""
		for i := 0; i < 2048; i++ {
			tmp += "y"
			avl.Add(tmp, fmt.Sprintf("%d", i*i))
		}
		fmt.Printf("avl tree height: %d tree size: %d\n", avl.Height(), avl.Size())
		endTime := time.Now()
		elapsed := endTime.Sub(startTime).Milliseconds()
		fmt.Printf("avlTree use time: %v ms.\n", elapsed)

		// startTime1 := time.Now()
		// biTree := lsmt.NewTree(nil)
		// tmp = ""
		// for i := 0; i < 2048; i++ {
		// 	tmp += "y"
		// 	lsmt.Upsert(&biTree, lsmt.Element{
		// 		Key:   tmp,
		// 		Value: fmt.Sprintf("%d", i*i),
		// 	})
		// }
		// fmt.Printf("binary tree height: %d\n", lsmt.GetHeight(biTree))
		// endTime1 := time.Now()
		// elapsed1 := endTime1.Sub(startTime1).Milliseconds()
		// fmt.Printf("binaryTree use time: %v ms.\n", elapsed1)
	}

}
