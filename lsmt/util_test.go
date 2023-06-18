package lsmt

import (
	"LSM-Tree/core"
	"container/list"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListRemove(t *testing.T) {
	l := list.New()
	l.PushFront(1)
	l.PushFront(2)
	assert.Equal(t, 2, l.Len())
	ListRemove(l, 1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, 2, l.Front().Value)
}

func TestMergeUpdate(t *testing.T) {
	elems := make([][]*core.Element, 4)
	all_keys := make([]string, 0)
	for j := 0; j < 4; j++ {
		disk_elems := make([]*core.Element, 5)
		keys := make([]string, 5)
		for i := 0; i < len(disk_elems); i++ {
			// keys[i] = fmt.Sprintf("key%d", rand.Intn(100))
		}
		sort.Strings(keys)
		for i := 0; i < len(disk_elems); i++ {
			disk_elems[i] = &core.Element{
				Key: keys[i],
			}
		}
		// fmt.Printf("disk %d, keys = %v\n", j, keys)
		elems[j] = disk_elems
		all_keys = append(all_keys, keys...)
	}
	// fmt.Printf("all keys = %v\n", all_keys)

	mergeElems := MergeUpdate(elems)
	keys := make([]string, len(mergeElems))
	for i, e := range mergeElems {
		keys[i] = e.Key
	}
	sort.Strings(all_keys)
	assert.Equal(t, all_keys, keys)
	// fmt.Printf("after merge elems, keys = %v\n", keys)
	// assert.Equal(t, true, false)
}

func TestListInsert(t *testing.T) {
	elems := []*core.Element{
		{Key: "1", Value: "One"},
		{Key: "2", Value: "Two"},
		{Key: "3", Value: "Three"},
		{Key: "4", Value: "Four"},
		{Key: "5", Value: "Five"},
		{Key: "6", Value: "Six"},
		{Key: "7", Value: "Seven"},
		// {Key: "8", Value: "Eight"},
		// {Key: "9", Value: "Nine"},
	}

	d1 := NewDiskFile(elems[0:2], 1)
	d2 := NewDiskFile(elems[2:4], 1)
	d3 := NewDiskFile(elems[4:6], 1)
	d4 := NewDiskFile(elems[6:], 1)

	// 在链表中插入一些初始值
	myList := list.New()
	myList.PushBack(d1)
	myList.PushBack(d4)

	files1 := []*DiskFile{d2, d3}

	for e := myList.Front(); e != nil; e = e.Next() {
		fmt.Printf("before listInsert, start_key: %v end_key: %v\n", e.Value.(*DiskFile).start_key, e.Value.(*DiskFile).end_key)
	}

	ListInsert(myList, files1)

	// 打印链表中的所有值
	for e := myList.Front(); e != nil; e = e.Next() {
		fmt.Printf("after listInsert, start_key: %v end_key: %v\n", e.Value.(*DiskFile).start_key, e.Value.(*DiskFile).end_key)
	}
	// assert.Equal(t, false, true)
}
