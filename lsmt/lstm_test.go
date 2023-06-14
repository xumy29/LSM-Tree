package lsmt

import (
	"LSM-Tree/core"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

////////////////////////////////////////////////////
//// logic test ////
///////////////////////////////////////////////////

func TestInMemoryOnly(t *testing.T) {
	var wg sync.WaitGroup
	var expected []core.Element
	total := 10
	tree := NewLSMTree(total + 1 /* flush threshold larger than total */)
	for i := 0; i < total; i++ {
		e := core.Element{Key: fmt.Sprintf("%d", i), Value: fmt.Sprintf("%d", i)}
		expected = append(expected, e)
		wg.Add(1)
		// 测试多线程写
		go func() {
			tree.Put(e.Key, e.Value)
			wg.Done()
		}()
	}
	wg.Wait()
	if tree.tree.Size() != total {
		t.Errorf("got tree size %d; want %d", tree.tree.Size(), total)
	}
	for i := 0; i < total; i++ {
		wg.Add(1)
		e := fmt.Sprintf("%d", i)
		// 测试多线程读
		go func() {
			v, err := tree.Get(e)
			if err != nil {
				t.Errorf("key %s not found", e)
			}
			if v != e {
				t.Errorf("got %s for key %s; want %s", v, e, e)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	got := tree.tree.Inorder()
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("got result %v; want %v", got, expected)
	}
}

func TestFlushedToDisk(t *testing.T) {
	t.Parallel()
	tree := NewLSMTree(2)
	tree.Put("1", "One")
	tree.Put("2", "Two")
	// 等待写入到磁盘
	time.Sleep(1 * time.Second)
	if tree.tree.Size() != 0 {
		t.Errorf("got tree size %d; want 0", tree.tree.Size())
	}
	if len(tree.diskFiles) != 1 {
		t.Errorf("got disk file size %d; want 1", len(tree.diskFiles))
	}
	if _, err := tree.Get("1"); err != nil {
		t.Error("key 1 not found")
	}
	if _, err := tree.Get("2"); err != nil {
		t.Error("key 2 not found")
	}
	tree.Put("3", "Three")
	if _, err := tree.Get("3"); err != nil {
		t.Error("key 3 not found")
	}
	tree.Put("4", "Four")
	// 等待写入到磁盘和compaction
	time.Sleep(3 * time.Second)
	if len(tree.diskFiles) != 1 {
		t.Errorf("got disk file size %d; want 1", len(tree.diskFiles))
	}
	if len(tree.diskFiles) == 1 {
		got := tree.diskFiles[0].AllElements()
		want := []core.Element{{Key: "1", Value: "One"}, {Key: "2", Value: "Two"}, {Key: "3", Value: "Three"}, {Key: "4", Value: "Four"}}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("got result %v; want %v", got, want)
		}
	}
}

func TestCompactionCollapse(t *testing.T) {
	t.Parallel()
	tree := NewLSMTree(1)
	tree.Put("1", "One")
	time.Sleep(time.Second)
	tree.Put("1", "ONE")
	// 等待写入到磁盘和compaction.
	time.Sleep(3 * time.Second)
	if len(tree.diskFiles) != 1 {
		t.Errorf("got disk file size %d; want 1", len(tree.diskFiles))
	}
	if len(tree.diskFiles) == 1 {
		got := tree.diskFiles[0].AllElements()
		want := []core.Element{{Key: "1", Value: "ONE"}}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("got result %v; want %v", got, want)
		}
	}
}

////////////////////////////////////////////////////
//// benchmark test ////
///////////////////////////////////////////////////

func PutData(elems []core.Element, lsmTree *LSMTree) {
	for i := 0; i < len(elems); i++ {
		lsmTree.Put(elems[i].Key, elems[i].Value)
	}
}

func GetData(elems []core.Element, lsmTree *LSMTree) {
	for i := 0; i < len(elems); i++ {
		_, err := lsmTree.Get(elems[i].Key)
		if err != nil {
			fmt.Printf("getData wrong! %v\n", err)
		}
	}
}

func benchmarkPut(b *testing.B) {
	elemCnt := 10000
	len2Flush := elemCnt / 10

	elems := GenerateData(elemCnt)
	for k := 0; k < b.N; k++ {
		// startTime := time.Now()

		lsmTree := NewLSMTree(len2Flush)
		PutData(elems, lsmTree)
		lsmTree.Destroy()

		// endTime := time.Now()
		// elapsed := endTime.Sub(startTime).Milliseconds()

		// fmt.Printf("第 %d 次迭代的执行时间：%d 毫秒\n", k+1, elapsed)
	}
}

// func BenchmarkGet(b *testing.B) {
// 	elemCnt := 10000
// 	len2Flush := elemCnt / 10

// 	elems := GenerateData(elemCnt)

// 	lsmTree := NewLSMTree(len2Flush)

// 	PutData(elems, lsmTree)

// 	for k := 0; k < b.N; k++ {
// 		// startTime := time.Now()
// 		fmt.Printf("iter %d\n", k+1)
// 		lsmTree.Print()
// 		GetData(elems, lsmTree)

// 		// endTime := time.Now()
// 		// elapsed := endTime.Sub(startTime).Milliseconds()

// 		// fmt.Printf("第 %d 次迭代的执行时间：%d 毫秒\n", k+1, elapsed)
// 	}
// }
