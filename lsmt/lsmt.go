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
	/* 控制内存中tree和treesInFlush的并发读写 */
	rwm  sync.RWMutex
	tree *avlTree.AVLTree
	/* 从tree写入到硬盘的中间缓冲区列表，每个元素的类型是 *avlTree.AVLTree，指向一个缓冲区 */
	treesInFlush   *list.List
	flushThreshold int

	/* 控制对磁盘文件的并发读写 */
	drwm sync.RWMutex
	/** 多级磁盘文件
	 * key即磁盘level，最低层是0，value即该level的磁盘文件列表 */
	diskFiles map[int]*list.List
	/* 包括内存中的元素、正在flush到磁盘和已经在磁盘中的元素个数 */
	TotalSize int

	config *config.Config
	/* 是否正在进行磁盘文件归并 */
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
		tree:           &avlTree.AVLTree{},
		treesInFlush:   list.New(),
		diskFiles:      make(map[int]*list.List),
		config:         config.DefaultConfig(),
		isCompacting:   false,
	}
	if t.flushThreshold == 0 {
		t.flushThreshold = t.config.ElemCnt2Flush
	}

	for i := 0; i < t.config.FileLevelCnt; i++ {
		t.diskFiles[i] = list.New()
	}
	return t
}

func (t *LSMTree) Put(key, value string) {
	if value == t.config.DeleteValue {
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
		// log.Logger.Debug("LSMTree triggers flush", "Treesize", t.tree.Size())
		t.toFlush()
	}
}

func (t *LSMTree) Delete(key string) {
	t.rwm.Lock()
	defer t.rwm.Unlock()
	log.Trace(fmt.Sprintf("Delete(key: %v)", key))
	t.TotalSize += t.tree.Add(key, t.config.DeleteValue)
	if t.tree.Size() >= t.flushThreshold {
		t.toFlush()
	}
}

func (t *LSMTree) Get(key string) (string, error) {
	deleteVal := t.config.DeleteValue
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
	log.Logger.Debug(fmt.Sprintf("get key %v, current file level: %d\n", key, 0))
	for e := t.diskFiles[0].Front(); e != nil; e = e.Next() {
		d := e.Value.(*DiskFile)
		elem, err := d.Search(key)
		if err == nil {
			// found in disk
			// found in disk
			log.Logger.Debug("found key in level-0 file", "file start key", d.start_key, "file end key", d.end_key)
			if elem.Value == deleteVal {
				log.Logger.Debug("this key was deleted")
				return "", fmt.Errorf("key %s was deleted", key)
			}
			return elem.Value, nil
		}
	}

	// 从level1开始，每层文件都是有序的，只需找到该key所在的文件，在该文件内搜索即可
	for i := 1; i < t.config.FileLevelCnt; i++ {
		log.Logger.Debug(fmt.Sprintf("get key %v, current file level: %d\n", key, i))
		files := t.diskFiles[i]
		if files.Len() == 0 {
			continue
		}
		for e := files.Front(); e != nil; e = e.Next() {
			d := e.Value.(*DiskFile)
			// log.Logger.Debug("file key range", "start", d.start_key, "end", d.end_key)
			if d.start_key <= key && d.end_key >= key {
				log.Trace("found file")
				elem, err := d.Search(key)
				if err == nil {
					// found in disk
					log.Logger.Debug("found key in level-1 file", "file start key", d.start_key, "file end key", d.end_key)
					if elem.Value == deleteVal {
						log.Logger.Debug("this key was deleted")
						return "", fmt.Errorf("key %s was deleted", key)
					}
					return elem.Value, nil
				}
				// 不在此层级中，往下一层找
				break
			}
		}
	}

	return "", fmt.Errorf("key %s not found", key)
}

func (t *LSMTree) toFlush() {
	// 此函数包含对树的操作，需加锁或在调用本函数的其他函数上下文中加锁
	e := t.treesInFlush.PushFront(t.tree) // 最新的树加在链表最前面
	// log.Logger.Debug(fmt.Sprintf("now we have %d treeInFlush.", t.treesInFlush.Len()))
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
	// log.Logger.Debug(fmt.Sprintf("now we have %d diskFiles in level-0.", t.diskFiles[0].Len()))
	if t.diskFiles[0].Len() >= t.config.MaxLevel0FileCnt {
		go t.compact(0)
	}
	t.drwm.Unlock()
	// Remove the tree in flush.
	t.rwm.Lock()
	ListRemove(t.treesInFlush, treeInFlush)
	t.rwm.Unlock()
}

func (t *LSMTree) compact0isDone() bool {
	t.drwm.RLock()
	defer t.drwm.RUnlock()
	return t.diskFiles[0].Len() < t.config.MaxLevel0FileCnt
}

func (t *LSMTree) compact(level int) {
	t.drwm.Lock()
	// 减少复杂性，最多有一个后台线程进行compact
	if t.isCompacting {
		t.drwm.Unlock()
		return
	}
	t.isCompacting = true
	t.drwm.Unlock()

	if level == 0 {
		t.drwm.RLock()
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
		log.Logger.Debug(fmt.Sprintf("Start compacting. Now we have %d files in level0, %d files in level1\n", t.diskFiles[0].Len(), t.diskFiles[1].Len()))
		// t.Print_Files_1_Ranges()
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
		ListInsert(t.diskFiles[1], new_files1)

		log.Logger.Debug(fmt.Sprintf("Successfully compact. Now we have %d files in level0, %d files in level1\n", t.diskFiles[0].Len(), t.diskFiles[1].Len()))
		// t.Print_Files_1_Ranges()
		t.isCompacting = false
		t.drwm.Unlock()

		if !t.compact0isDone() {
			t.compact(0)
		}
	}

}

/** 接收level0的所有文件，以及level1的所有key与level0有重叠的文件，合并成新的level1文件并返回
 * 整体的合并的过程是：先将level0的所有文件合并成一个，再将合并后的文件与level1的文件逐个合并
 */
func (t *LSMTree) compact_0(files_0 []*DiskFile, files_1 []*DiskFile) []*DiskFile {
	log.Logger.Debug(fmt.Sprintf("compacting... files0_cnt_to_merge: %d, files1_cnt_to_merge: %d", len(files_0), len(files_1)))
	// 先对files0进行排序
	elems := make([][]*core.Element, len(files_0))
	// file0_elem_cnt := 0
	for i := 0; i < len(files_0); i++ {
		elems[i] = files_0[i].AllElements()
		// log.Trace(fmt.Sprintf("file0 size : %d, key range[%v,%v]", len(elems[i]), files_0[i].start_key, files_0[i].end_key))
	}
	// for i := 0; i < len(files_1); i++ {
	// 	tmp := files_1[i].AllElements()
	// 	log.Trace(fmt.Sprintf("file1 size : %d, key range[%v,%v]", len(tmp), files_1[i].start_key, files_1[i].end_key))
	// }
	sorted_files0_elems := MergeUpdate(elems)
	log.Trace(fmt.Sprintf("sorted_files0_elems size : %d, key range[%v,%v]", len(sorted_files0_elems),
		sorted_files0_elems[0].Key, sorted_files0_elems[len(sorted_files0_elems)-1].Key))
	new_files1 := make([]*DiskFile, 0)

	index0 := 0
	new_file_elems := make([]*core.Element, 0)

	if len(files_1) == 0 { // level-1没有key与level-0重叠的文件,直接写入新level-1文件
		for index0 < len(sorted_files0_elems) {
			upperbound := Min(index0+t.config.LevelLFileSize, len(sorted_files0_elems))
			new_disk_file := NewDiskFile(sorted_files0_elems[index0:upperbound], 1)
			log.Trace(fmt.Sprintf("new file1 size : %d, key range[%v,%v]", upperbound-index0, new_disk_file.start_key, new_disk_file.end_key))
			new_files1 = append(new_files1, new_disk_file)
			index0 = upperbound
		}
		return new_files1
	}

	var file1_idx int
	var old_file_elems []*core.Element
	var index1 int
	file1_elem_cnt := 0
	merge_elem_cnt := 0
	for file1_idx = 0; file1_idx < len(files_1); file1_idx++ {
		old_file_elems = files_1[file1_idx].AllElements()
		file1_elem_cnt += len(old_file_elems)
		index1 = 0
		for {
			if index1 >= len(old_file_elems) {
				// log.Logger.Debug(fmt.Sprintf("compact_0. break..index1..new_files_elems: %d", len(new_file_elems)))
				break
			}
			if index0 >= len(sorted_files0_elems) {
				// log.Logger.Debug(fmt.Sprintf("compact_0. break..index0..new_files_elems: %d", len(new_file_elems)))
				// 将file1（可能大于1个文件）剩下的元素也加到new_file_elems中
				new_file_elems = append(new_file_elems, old_file_elems[index1:]...)
				// index1 = len(old_file_elems)
				break
			}

			if old_file_elems[index1].Key < sorted_files0_elems[index0].Key {
				new_file_elems = append(new_file_elems, old_file_elems[index1])
				index1 += 1
			} else {
				key := sorted_files0_elems[index0].Key
				new_file_elems = append(new_file_elems, sorted_files0_elems[index0])
				index0 += 1
				// 若level1文件有相同key，要丢弃该key对应的较旧的记录
				if old_file_elems[index1].Key == key {
					index1 += 1
				}
			}
			// 文件满，写下一个新文件
			if len(new_file_elems) >= t.config.LevelLFileSize {
				new_disk_file := NewDiskFile(new_file_elems, 1)
				new_files1 = append(new_files1, new_disk_file)
				log.Trace(fmt.Sprintf("new file1 size : %d, key range[%v,%v]", len(new_file_elems), new_disk_file.start_key, new_disk_file.end_key))
				// log.Logger.Debug(fmt.Sprintf("compact_0. write new file, new filw size: %d", len(new_file_elems)))
				new_file_elems = make([]*core.Element, 0)
				merge_elem_cnt += t.config.LevelLFileSize

			}
		}

	}

	// files_1 的最后一个文件可能有key大于maxkey的元素，需将这部分也写入文件
	// new_file_elems = append(new_file_elems, old_file_elems[index1:]...)
	// log.Logger.Debug(fmt.Sprintf("compact_0. new_files_elems: %d", len(new_file_elems)))

	// sorted_files0_elems 也可能有剩余，这种情况发生在files0中出现比所有file1的key都大的key的情况下
	new_file_elems = append(new_file_elems, sorted_files0_elems[index0:]...)
	// log.Logger.Debug(fmt.Sprintf("compact_0. new_files_elems: %d", len(new_file_elems)))

	// new_file_elems 可能还有元素，写入到新文件中
	for len(new_file_elems) > 0 {
		upperbound := Min(t.config.LevelLFileSize, len(new_file_elems))
		new_disk_file := NewDiskFile(new_file_elems[:upperbound], 1)
		log.Trace(fmt.Sprintf("new file1 size : %d, key range[%v,%v]", upperbound, new_disk_file.start_key, new_disk_file.end_key))
		new_files1 = append(new_files1, new_disk_file)
		new_file_elems = new_file_elems[upperbound:]
		merge_elem_cnt += upperbound
	}
	// log.Logger.Debug(fmt.Sprintf("compact_0. files0 elems cnt: %d, sorted_files0_elems cnt: %d", file0_elem_cnt, len(sorted_files0_elems)))
	// log.Logger.Debug(fmt.Sprintf("compact_0. files1 elems cnt: %d, merge_elems cnt: %d", file1_elem_cnt_truly, merge_elem_cnt))

	return new_files1
}

/** 接收level0的所有文件，以及level1的所有key与level0有重叠的文件，合并成新的level1文件并返回
 * 整体的合并的过程是：将level0的所有文件和level1的所有文件分别合并成一个，再将合并后的两个文件进行合并并写入新文件
 */
// func (t *LSMTree) compact_0_V2(files_0 []*DiskFile, files_1 []*DiskFile) []*DiskFile {
// 	elems_0 := make([][]*core.Element, len(files_0))
// 	elems_1 := make([][]*core.Element, len(files_1))
// 	for i := 0; i < len(files_0); i++ {
// 		elems_0[i] = files_0[i].AllElements()
// 	}
// 	for i := 0; i < len(files_1); i++ {
// 		elems_1[i] = files_1[i].AllElements()
// 	}
// 	sorted_files0_elems := MergeUpdate(elems_0)
// 	log.Logger.Debug(fmt.Sprintf("sorted_files0_elems size : %d, key range[%v,%v]", len(sorted_files0_elems),
// 		sorted_files0_elems[0].Key, sorted_files0_elems[len(sorted_files0_elems)-1].Key))

// 	sorted_files1_elems := MergeUpdate(elems_1)
// 	if len(sorted_files1_elems) > 0 {
// 		log.Logger.Debug(fmt.Sprintf("sorted_files1_elems size : %d, key range[%v,%v]", len(sorted_files1_elems),
// 			sorted_files1_elems[0].Key, sorted_files1_elems[len(sorted_files1_elems)-1].Key))
// 	}

// 	index0 := 0
// 	index1 := 0
// 	merge_elems := make([]*core.Element, 0)
// 	for index0 < len(sorted_files0_elems) && index1 < len(sorted_files1_elems) {
// 		if sorted_files0_elems[index0].Key < sorted_files1_elems[index1].Key {
// 			merge_elems = append(merge_elems, sorted_files0_elems[index0])
// 			index0 += 1
// 		} else {
// 			merge_elems = append(merge_elems, sorted_files1_elems[index1])
// 			index1 += 1
// 		}
// 	}
// 	merge_elems = append(merge_elems, sorted_files0_elems[index0:]...)
// 	merge_elems = append(merge_elems, sorted_files1_elems[index1:]...)
// 	log.Logger.Debug(fmt.Sprintf("merged_files1_elems size : %d, key range[%v,%v]", len(merge_elems),
// 		merge_elems[0].Key, merge_elems[len(merge_elems)-1].Key))
// 	i := 0
// 	new_files1 := make([]*DiskFile, 0)
// 	for i < len(merge_elems) {
// 		upperbound := Min(i+t.config.LevelLFileSize, len(merge_elems))
// 		new_disk_file := NewDiskFile(merge_elems[i:upperbound], 1)
// 		log.Logger.Debug(fmt.Sprintf("new file1 size : %d, key range[%v,%v]", upperbound-i, new_disk_file.start_key, new_disk_file.end_key))
// 		new_files1 = append(new_files1, new_disk_file)
// 		i = upperbound
// 	}
// 	return new_files1
// }
