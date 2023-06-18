package lsmt

import (
	"container/list"
	"fmt"
	"sync"

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
	/** 多级磁盘文件
	 * key即磁盘level，最低层是0，value即该level的磁盘文件列表
	 * flush时新文件插入到最前面 */
	diskFiles map[int]*list.List
	/* 与子协程沟通的管道 */
	stop chan struct{}
	/* 包括内存中的元素、正在flush到磁盘和已经在磁盘中的元素个数 */
	TotalSize    int
	config       *config.Config
	isCompacting bool
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
		diskFiles:      make(map[int]*list.List),
		config:         config.DefaultConfig(),
		isCompacting:   false,
	}
	// 先初始化最低几层，已足够用
	for i := 0; i < 5; i++ {
		t.diskFiles[i] = list.New()
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
	for e := t.diskFiles[0].Front(); e != nil; e = e.Next() {
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
	d := NewDiskFile(treeInFlush.Inorder(), 0)
	// Put the disk file in the list.
	t.drwm.Lock()
	// 最新的文件放在最前面
	t.diskFiles[0].PushFront(d)
	log.Logger.Debug(fmt.Sprintf("now we have %d diskFiles in level-0.", t.diskFiles[0].Len()))
	if t.diskFiles[0].Len() >= t.config.MaxLevel0FileCnt {
		go t.compact(0)
	}
	t.drwm.Unlock()
	// Remove the tree in flush.
	t.rwm.Lock()
	ListRemove(t.treesInFlush, treeInFlush)
	t.rwm.Unlock()
}

func (t *LSMTree) compact(level int) {
	t.drwm.RLock()
	// 减少复杂性，最多有一个后台线程进行compact
	if t.isCompacting == true {
		t.drwm.RUnlock()
		return
	}
	t.isCompacting = true
	defer func() {
		t.isCompacting = false
	}()

	if level == 0 {
		// 将level0的所有文件合并成一个，并与level-1的key有重叠的文件合并成新文件
		files_0 := DiskList2Slice(t.diskFiles[0])
		min_key := MinKeyOfDiskSlice(files_0)
		max_key := MaxKeyOfDiskSlice(files_0)
		files_1 := make([]*DiskFile, 0)
		for e := t.diskFiles[1].Front(); e != nil; e = e.Next() {
			d := e.Value.(*DiskFile)
			if d.start_key > max_key {
				// level1及以上的文件是有序的，所以某个文件最小的key超过当前key的范围时可以结束遍历
				break
			}
			if d.end_key < min_key {
				// 往后遍历直到找到有与level0的key范围重叠的文件
				continue
			}
			files_1 = append(files_1, d)
		}
		t.drwm.RUnlock()
		// 根据得到的level0文件指针和level1文件指针进行合并
		new_files1 := t.compact_0(files_0, files_1)
		t.drwm.Lock()
		// 删除合并前的文件，插入合并后产生的新文件
		for _, file0 := range files_0 {
			ListRemove(t.diskFiles[0], file0)
		}
		for _, file1 := range files_1 {
			ListRemove(t.diskFiles[1], file1)
		}
		// 根据前后文件的key，插入到合适的地方
		// 具体地，插入的地方应该满足：前一个文件的end_key比new_files1最小的start_key小，后一个文件的start_key比new_files1最大的end_key大
		// 由于new_files1本身是有序的，找到一个位置即可连续插入
		// todo : 完成 ListInsert(t.diskFiles[1], new_files1)
		for _, file1 := range new_files1 {
			t.diskFiles[1].PushBack(file1)
		}

		log.Logger.Debug(fmt.Sprintf("Successfully compact. Now we have %d files in level0, %d files in level1\n", t.diskFiles[0].Len(), t.diskFiles[1].Len()))
		t.drwm.Unlock()
	}
}

/** 接收level0的所有文件，以及level1的所有key与level0有重叠的文件，合并成新的level1文件并返回 */
func (t *LSMTree) compact_0(files_0 []*DiskFile, files_1 []*DiskFile) []*DiskFile {
	// 先对files0进行排序
	elems := make([][]*core.Element, len(files_0))
	for i := 0; i < len(files_0); i++ {
		elems[i] = files_0[i].AllElements()
	}
	sorted_files0_elems := MergeUpdate(elems)
	new_files1 := make([]*DiskFile, 0)

	index0 := 0
	new_file_elems := make([]*core.Element, 0)

	if len(files_1) == 0 { // level-1没有key与level-0重叠的文件,直接写入新level-1文件
		for index0 < len(sorted_files0_elems) {
			upperbound := Min(index0+t.config.LevelLFileSize, len(sorted_files0_elems))
			new_disk_file := NewDiskFile(sorted_files0_elems[index0:upperbound], 1)
			new_files1 = append(new_files1, new_disk_file)
			index0 = upperbound
		}
		return new_files1
	}

	for i := 0; i < len(files_1); i++ {
		old_file_elems := files_1[i].AllElements()
		index1 := 0
		for {
			if index1 == len(old_file_elems) {
				break
			}
			if index0 == len(sorted_files0_elems) {
				break
			}
			if old_file_elems[index1].Key < sorted_files0_elems[index0].Key {
				new_file_elems = append(new_file_elems, old_file_elems[index1])
				index1 += 1
			} else {
				new_file_elems = append(new_file_elems, sorted_files0_elems[index0])
				index0 += 1
			}
			// 文件满，写下一个新文件
			if len(new_file_elems) >= t.config.LevelLFileSize {
				new_disk_file := NewDiskFile(new_file_elems, 1)
				new_files1 = append(new_files1, new_disk_file)
				new_file_elems = make([]*core.Element, 0)
			}
		}

	}
	// new_file_elems 可能还有元素，写入到新文件中
	if len(new_file_elems) > 0 {
		new_disk_file := NewDiskFile(new_file_elems, 1)
		new_files1 = append(new_files1, new_disk_file)
	}

	return new_files1
}

// func (t *LSMTree) compactService(interval int) {
// 	for {
// 		select {
// 		case <-t.stop:
// 			t.stop <- struct{}{}
// 			fmt.Print("compact 线程关闭\n")
// 			return
// 		default:
// 			time.Sleep(time.Duration(interval) * time.Millisecond)
// 			var d1, d2 *DiskFile
// 			t.drwm.Lock()
// 			fileCnt := t.diskFiles.Len()
// 			if fileCnt >= 2 {
// 				d1 = t.diskFiles.Remove(t.diskFiles.Back()).(*DiskFile)
// 				d2 = t.diskFiles.Back().Value.(*DiskFile)
// 				t.diskFiles.PushBack(d1)
// 			}
// 			t.drwm.Unlock()
// 			if d1 == nil || d2 == nil {
// 				continue
// 			}
// 			// Create a new compacted disk file.
// 			d := compact(d1, d2)
// 			// Replace the two old files.
// 			t.drwm.Lock()
// 			// 先删除最后两个文件，即被合并的文件
// 			t.diskFiles.Remove(t.diskFiles.Back())
// 			t.diskFiles.Remove(t.diskFiles.Back())
// 			// compact后的文件放在最后面，代表数据最旧
// 			t.diskFiles.PushBack(d)
// 			log.Logger.Debug(fmt.Sprintf("now we have %d diskFiles.", t.diskFiles.Len()))

// 			t.drwm.Unlock()
// 		}
// 	}
// }

// func compact(d1, d2 *DiskFile) *DiskFile {
// 	log.Logger.Debug("start compacting two diskFiles.", "disk1'id", d1.id, "disk2'id", d2.id)
// 	elems1 := d1.AllElements()
// 	elems2 := d2.AllElements()

// 	var newElems []core.Element
// 	var i1, i2 int
// 	for i1 < len(elems1) && i2 < len(elems2) {
// 		e1 := elems1[i1]
// 		e2 := elems2[i2]
// 		if e1.Key < e2.Key {
// 			newElems = append(newElems, e1)
// 			i1++
// 		} else if e1.Key > e2.Key {
// 			newElems = append(newElems, e2)
// 			i2++
// 		} else {
// 			// d1 is assumed to be older than d2.
// 			newElems = append(newElems, e2)
// 			i1++
// 			i2++
// 		}
// 	}
// 	newElems = append(newElems, elems1[i1:]...)
// 	newElems = append(newElems, elems2[i2:]...)
// 	newDiskFile := NewDiskFile(newElems)
// 	log.Logger.Debug("successfully compact two diskFiles.", "disk1'id", d1.id, "disk1'size", d1.size,
// 		"disk2'id", d2.id, "disk2'size", d2.size,
// 		"newDisk'id", newDiskFile.id, "newDisk'size", newDiskFile.size)
// 	return newDiskFile
// }

func (t *LSMTree) Destroy() {
	// 结束子协程
	t.stop <- struct{}{}
	<-t.stop
}
