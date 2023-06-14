package lsmt

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"LSM-Tree/avlTree"
	"LSM-Tree/config"
	"LSM-Tree/core"
	log "LSM-Tree/log"
)

type LSMTree struct {
	/* 控制内存中两棵树的并发读写 */
	rwm  sync.RWMutex
	tree *avlTree.AVLTree
	/* 从tree写入到硬盘的中间缓冲区列表，每个元素的类型是 *avlTree.AVLTree，指向一个缓冲区 */
	treesInFlush   *list.List
	flushThreshold int
	/* 控制对磁盘文件的并发读写 */
	drwm sync.RWMutex
	/** 磁盘文件列表
	 * flush时新文件插入到最前面 */
	diskFiles *list.List
	/* 与子协程沟通的管道 */
	stop chan struct{}
	/* 包括内存中的元素、正在flush到磁盘和已经在磁盘中的元素个数 */
	TotalSize int
	config    *config.Config
}

// debug
// todo: treesInFlush 由*avlTree.AVLTree变成了list，相关操作需调整
// func (t *LSMTree) Print() {
// 	fmt.Printf("LSMTree: %p\n", t)
// 	cnt := 0
// 	fmt.Printf("tree root: %p size: %d\n", t.tree, t.tree.Size())
// 	cnt += t.tree.Size()
// 	fmt.Printf("treeInFlush root: %p ", t.treesInFlush)
// 	if t.treesInFlush == nil {
// 		fmt.Printf("\n")
// 	} else {
// 		fmt.Printf("size: %d\n", t.treesInFlush.Size())
// 		cnt += t.treesInFlush.Size()
// 	}
// 	fmt.Printf("diskFiles: %p\n", t.diskFiles)
// 	for i := 0; i < len(t.diskFiles); i++ {
// 		fmt.Printf("diskFile %d, indexTree root: %p, size: %d\n", i, t.diskFiles[i].index, t.diskFiles[i].size)
// 		cnt += t.diskFiles[i].size
// 	}
// 	fmt.Printf("total size: %d\n", cnt)
// }

func NewLSMTree(flushThreshold int) *LSMTree {
	t := &LSMTree{
		flushThreshold: flushThreshold,
		stop:           make(chan struct{}, 1),
		tree:           &avlTree.AVLTree{},
		treesInFlush:   list.New(),
		diskFiles:      list.New(),
		config:         config.DefaultConfig(),
	}
	return t
}

func (t *LSMTree) Put(key, value string) {
	if value == config.DefaultConfig().DeleteValue {
		log.Logger.Error(fmt.Sprintf("Error occurs during Put(key:'%v',value:'%v'). This value is reserved as special delete value, try another value or use escape characters", key, value))
		return
	}
	t.rwm.Lock()
	defer t.rwm.Unlock()
	log.Trace(fmt.Sprintf("Put(key: %v, value: %v)", key, value))
	t.TotalSize += t.tree.Add(key, value)
	// log.Logger.Debug("LSMTree Put or Update", "key", key, "value", value)
	if t.tree.Size() >= t.flushThreshold {
		// Trigger flush.
		log.Logger.Debug("LSMTree triggers flush", "Treesize", t.tree.Size())
		t.toFlush()
	}
}

func (t *LSMTree) Delete(key string) {
	t.rwm.Lock()
	defer t.rwm.Unlock()
	log.Trace(fmt.Sprintf("Delete(key: %v)", key))
	t.TotalSize += t.tree.Add(key, config.DefaultConfig().DeleteValue)
}

func (t *LSMTree) Get(key string) (string, error) {
	deleteVal := config.DefaultConfig().DeleteValue
	t.rwm.RLock()
	if node := t.tree.Search(key); node != nil {
		if node.Value == deleteVal {
			// 该key已被删除
			t.rwm.RUnlock()
			return "", fmt.Errorf("key %s was deleted", key)
		}
		t.rwm.RUnlock()
		return node.Value, nil
	}
	for e := t.treesInFlush.Front(); e != nil; e = e.Next() {
		treeInFlush := e.Value.(*avlTree.AVLTree)
		if node := treeInFlush.Search(key); node != nil {
			if node.Value == deleteVal {
				// 该key已被删除
				t.rwm.RUnlock()
				return "", fmt.Errorf("key %s was deleted", key)
			}
			t.rwm.RUnlock()
			return node.Value, nil
		}
	}
	t.rwm.RUnlock()
	// The key is not in memory. Search in disk files.
	t.drwm.RLock()
	defer t.drwm.RUnlock()
	// 从最前面的最新磁盘文件开始往后搜，搜到的第一个即返回
	for e := t.diskFiles.Front(); e != nil; e = e.Next() {
		d := e.Value.(*DiskFile)
		elem, err := d.Search(key)
		if err == nil {
			// found in disk
			if elem.Value == deleteVal {
				return "", fmt.Errorf("key %s was deleted", key)
			}
			return elem.Value, nil
		}
	}
	return "", fmt.Errorf("key %s not found", key)
}

func (t *LSMTree) toFlush() {
	// 此函数包含对树的操作，需加锁或在调用本函数的其他函数上下文中加锁
	e := t.treesInFlush.PushFront(t.tree) // 最新的树加在链表最前面
	log.Logger.Debug(fmt.Sprintf("now we have %d treeInFlush.", t.treesInFlush.Len()))
	t.tree = &avlTree.AVLTree{}
	go t.flush(e.Value.(*avlTree.AVLTree))
}

/** 创建一个新的磁盘文件，将一个缓冲区的内容写入到磁盘文件
 * 写入完成后，将该缓冲区指针从链表中移除
 */
func (t *LSMTree) flush(treeInFlush *avlTree.AVLTree) {
	// Create a new disk file.
	d := NewDiskFile(treeInFlush.Inorder())
	// Put the disk file in the list.
	t.drwm.Lock()
	// 最新的文件放在最前面
	t.diskFiles.PushFront(d)
	log.Logger.Debug(fmt.Sprintf("now we have %d diskFiles.", t.diskFiles.Len()))
	t.drwm.Unlock()
	// Remove the tree in flush.
	t.rwm.Lock()
	ListRemove(t.treesInFlush, treeInFlush)
	t.rwm.Unlock()
}

func (t *LSMTree) compactService(interval int) {
	for {
		select {
		case <-t.stop:
			t.stop <- struct{}{}
			fmt.Print("compact 线程关闭\n")
			return
		default:
			time.Sleep(time.Duration(interval) * time.Millisecond)
			var d1, d2 *DiskFile
			t.drwm.Lock()
			fileCnt := t.diskFiles.Len()
			if fileCnt >= 2 {
				d1 = t.diskFiles.Remove(t.diskFiles.Back()).(*DiskFile)
				d2 = t.diskFiles.Back().Value.(*DiskFile)
				t.diskFiles.PushBack(d1)
			}
			t.drwm.Unlock()
			if d1 == nil || d2 == nil {
				continue
			}
			// Create a new compacted disk file.
			d := compact(d1, d2)
			// Replace the two old files.
			t.drwm.Lock()
			// 先删除最后两个文件，即被合并的文件
			t.diskFiles.Remove(t.diskFiles.Back())
			t.diskFiles.Remove(t.diskFiles.Back())
			// compact后的文件放在最后面，代表数据最旧
			t.diskFiles.PushBack(d)
			log.Logger.Debug(fmt.Sprintf("now we have %d diskFiles.", t.diskFiles.Len()))

			t.drwm.Unlock()
		}
	}
}

func compact(d1, d2 *DiskFile) *DiskFile {
	log.Logger.Debug("start compacting two diskFiles.", "disk1'id", d1.id, "disk2'id", d2.id)
	elems1 := d1.AllElements()
	elems2 := d2.AllElements()

	var newElems []core.Element
	var i1, i2 int
	for i1 < len(elems1) && i2 < len(elems2) {
		e1 := elems1[i1]
		e2 := elems2[i2]
		if e1.Key < e2.Key {
			newElems = append(newElems, e1)
			i1++
		} else if e1.Key > e2.Key {
			newElems = append(newElems, e2)
			i2++
		} else {
			// d1 is assumed to be older than d2.
			newElems = append(newElems, e2)
			i1++
			i2++
		}
	}
	newElems = append(newElems, elems1[i1:]...)
	newElems = append(newElems, elems2[i2:]...)
	newDiskFile := NewDiskFile(newElems)
	log.Logger.Debug("successfully compact two diskFiles.", "disk1'id", d1.id, "disk1'size", d1.size,
		"disk2'id", d2.id, "disk2'size", d2.size,
		"newDisk'id", newDiskFile.id, "newDisk'size", newDiskFile.size)
	return newDiskFile
}

func (t *LSMTree) Destroy() {
	// 结束子协程
	t.stop <- struct{}{}
	<-t.stop
}
