package lsmt

import (
	"LSM-Tree/config"
	"LSM-Tree/core"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////
//// logic test ////
///////////////////////////////////////////////////

/* 测试“内存”中的树，以及多线程并发读写 */
func TestInMemoryOnly(t *testing.T) {
	var wg sync.WaitGroup
	var expected []*core.Element
	total := 10
	tree := NewLSMTree(total + 1)
	for i := 0; i < total; i++ {
		e := &core.Element{Key: fmt.Sprintf("%d", i), Value: fmt.Sprintf("%d", i)}
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

/* 测试“内存”中的树能否正确地被flush到“磁盘” */
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
	if tree.diskFiles[0].Len() != 1 {
		t.Errorf("got disk level-0 files num %d; want 1", tree.diskFiles[0].Len())
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
	tree.Put("5", "Five")
	tree.Put("6", "Six")
	tree.Put("7", "Seven")
	tree.Put("8", "Eight")
	// 等待写入到磁盘和compaction
	time.Sleep(3 * time.Second)
	if tree.diskFiles[0].Len() != 0 {
		t.Errorf("got disk level-0 files num %d; want 0", tree.diskFiles[0].Len())
	}
	if tree.diskFiles[1].Len() != 1 {
		t.Errorf("got disk level-1 files num %d; want 1", tree.diskFiles[1].Len())
	}
	if tree.diskFiles[1].Len() == 1 {
		got := tree.diskFiles[1].Front().Value.(*DiskFile).AllElements()
		want := []*core.Element{{Key: "1", Value: "One"}, {Key: "2", Value: "Two"}, {Key: "3", Value: "Three"}, {Key: "4", Value: "Four"},
			{Key: "5", Value: "Five"}, {Key: "6", Value: "Six"}, {Key: "7", Value: "Seven"}, {Key: "8", Value: "Eight"}}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("got result %v; want %v", got, want)
		}
	}
}

/* 测试删除操作 */
func TestDelete(t *testing.T) {
	tree := NewLSMTree(2)
	tree.Put("1", "One")
	tree.Put("2", "Two")

	// 写入到磁盘且能正确读取
	time.Sleep(1 * time.Second)
	assert.Equal(t, 1, tree.diskFiles[0].Len())
	val, err := tree.Get("1")
	assert.Equal(t, true, err == nil)
	assert.Equal(t, "One", val)

	// 删除，未写入到磁盘，从内存中读取不到
	tree.Delete("1")
	assert.Equal(t, 1, tree.tree.Size())
	_, err = tree.Get("1")
	assert.Equal(t, true, err != nil)

	// 随便插入一个新键值对
	tree.Put("3", "Three")

	// 删除操作被写入到磁盘，且进行了compact，仍然读取不到被删除的键
	time.Sleep(1 * time.Second)
	_, err = tree.Get("1")
	assert.Equal(t, true, err != nil)
	// 但可以正常读取没被删除的键
	val, err = tree.Get("2")
	assert.Equal(t, true, err == nil)
	assert.Equal(t, "Two", val)

	// 尝试插入特殊删除标记值，会失败
	tree.Put("4", config.DefaultConfig().DeleteValue) // logfile.log中会打印错误

}

////////////////////////////////////////////////////
//// benchmark test ////
///////////////////////////////////////////////////

func BenchmarkPut(b *testing.B) {
	elems := GenerateData(1000000)

	for i := 0; i < b.N; i++ {
		lsmTree := NewLSMTree(0)
		for i := 0; i < len(elems); i++ {
			lsmTree.Put(elems[i].Key, elems[i].Value)
		}
		// fmt.Printf("The lsmTree has %d nodes in total\n", lsmTree.TotalSize)
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
