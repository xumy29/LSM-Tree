package lsmt

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"LSM-Tree/avlTree"
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
	 * 新文件插入到最前面，搜索时从最新文件开始搜索，合并时从最旧文件开始 */
	diskFiles []DiskFile
	/* 与子协程沟通的管道 */
	stop chan struct{}
	/* 包括内存中的元素、正在flush到磁盘和已经在磁盘中的元素个数 */
	TotalSize int
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
	}
	go t.compactService()
	return t
}

func (t *LSMTree) Put(key, value string) {
	t.rwm.Lock()
	defer t.rwm.Unlock()
	t.TotalSize += t.tree.Add(key, value)
	if t.tree.Size() >= t.flushThreshold {
		// Trigger flush.
		log.Logger.Debug("LSMTree triggers flush", "Treesize", t.tree.Size())
		t.toFlush()
	}
}

func (t *LSMTree) toFlush() {
	// 此函数包含对树的操作，需加锁或在调用本函数的其他函数上下文中加锁
	e := t.treesInFlush.PushFront(t.tree) // 最新的树加在链表最前面
	log.Logger.Debug(fmt.Sprintf("now we have %d treeInFlush.", t.treesInFlush.Len()))
	t.tree = &avlTree.AVLTree{}
	go t.flush(e.Value.(*avlTree.AVLTree))
}

func (t *LSMTree) Get(key string) (string, error) {
	t.rwm.RLock()
	if node := t.tree.Search(key); node != nil {
		t.rwm.RUnlock()
		return node.Value, nil
	}
	for e := t.treesInFlush.Front(); e != nil; e = e.Next() {
		treeInFlush := e.Value.(*avlTree.AVLTree)
		if node := treeInFlush.Search(key); node != nil {
			t.rwm.RUnlock()
			return node.Value, nil
		}
	}
	t.rwm.RUnlock()
	// The key is not in memory. Search in disk files.
	t.drwm.RLock()
	defer t.drwm.RUnlock()
	for _, d := range t.diskFiles {
		e, err := d.Search(key)
		if err == nil {
			// Found in disk
			return e.Value, nil
		}
	}
	return "", fmt.Errorf("key %s not found", key)
}

/** 创建一个新的磁盘文件，将一个缓冲区的内容写入到磁盘文件
 * 写入完成后，将该缓冲区指针从链表中移除
 */
func (t *LSMTree) flush(treeInFlush *avlTree.AVLTree) {
	// Create a new disk file.
	d := []DiskFile{NewDiskFile(treeInFlush.Inorder())}
	// Put the disk file in the list.
	t.drwm.Lock()
	// 最新的文件放在最前面
	t.diskFiles = append(d, t.diskFiles...)
	t.drwm.Unlock()
	// Remove the tree in flush.
	t.rwm.Lock()
	ListRemove(t.treesInFlush, treeInFlush)
	t.rwm.Unlock()
}

func (t *LSMTree) compactService() {
	for {
		select {
		case <-t.stop:
			t.stop <- struct{}{}
			fmt.Print("compact 线程关闭\n")
			return
		default:
			time.Sleep(time.Second)
			var d1, d2 DiskFile
			t.drwm.RLock()
			fileCnt := len(t.diskFiles)
			if fileCnt >= 2 {
				d1 = t.diskFiles[fileCnt-1]
				d2 = t.diskFiles[fileCnt-2]
			}
			t.drwm.RUnlock()
			if d1.Empty() || d2.Empty() {
				continue
			}
			// Create a new compacted disk file.
			d := compact(d1, d2)
			// Replace the two old files.
			t.drwm.Lock()
			// 原先这里是 t.diskFiles = t.diskFiles[0 : len(diskFiles)-2],  t.diskFiles = append(t.diskFiles, d) 。似乎不太合理，因为compact过程中可能有新文件被写入diskFiles
			tmp := t.diskFiles[fileCnt:]
			t.diskFiles = t.diskFiles[0 : fileCnt-2]
			t.diskFiles = append(t.diskFiles, d)
			t.diskFiles = append(t.diskFiles, tmp...)
			t.drwm.Unlock()
		}
	}
}

func compact(d1, d2 DiskFile) DiskFile {
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
