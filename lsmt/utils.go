package lsmt

import (
	"LSM-Tree/core"
	"container/list"
	"fmt"
	"reflect"
)

func GenerateData(elemCnt int) []*core.Element {
	elems := make([]*core.Element, elemCnt)
	for i := 0; i < elemCnt; i++ {
		elem := &core.Element{
			Key:   fmt.Sprintf("key%d", i),
			Value: fmt.Sprintf("val%d", i),
		}
		elems[i] = elem
	}

	return elems
}

func Max(i, j int) int {
	if i > j {
		return i
	} else {
		return j
	}
}

func Min(i, j int) int {
	if i < j {
		return i
	}
	return j
}

func ListRemove(list *list.List, val interface{}) {
	for e := list.Front(); e != nil; e = e.Next() {
		// fmt.Printf("%v\n%v\n", e.Value, reflect.ValueOf(val).Interface())
		if e.Value == reflect.ValueOf(val).Interface() {
			list.Remove(e)
			break
		}
	}
}

/** 将compact得到的level1文件插入到level1文件列表中
* 由于compact线程只有一个，且level1中的文件一直保持有序，可以保证遍历到
的第一个start_key大于maxkey的文件之前的位置即为插入点
*/
func ListInsert(l *list.List, files1 []*DiskFile) {
	// 获取files1的key范围
	max_key := MaxKeyOfDiskSlice(files1)

	// 寻找插入的位置
	if l.Len() == 0 || l.Front().Value.(*DiskFile).start_key > max_key {
		for i := len(files1) - 1; i >= 0; i-- {
			l.PushFront(files1[i])
		}
		return
	}

	var pre *list.Element
	for e := l.Front(); ; {
		next := e.Next()
		pre = e
		if next == nil {
			break
		}
		d_next := next.Value.(*DiskFile)
		if d_next.start_key > max_key {
			break
		}
		e = next
	}

	for _, d := range files1 {
		pre = l.InsertAfter(d, pre)
	}
}

func MinKeyOfDiskSlice(files []*DiskFile) string {
	if len(files) == 0 {
		return ""
	}
	min_key := files[0].start_key
	for i := 0; i < len(files); i++ {
		if files[i].start_key < min_key {
			min_key = files[i].start_key
		}
	}
	return min_key
}

func MaxKeyOfDiskSlice(files []*DiskFile) string {
	if len(files) == 0 {
		return ""
	}
	max_key := files[0].end_key
	for i := 0; i < len(files); i++ {
		if files[i].end_key > max_key {
			max_key = files[i].end_key
		}
	}
	return max_key
}

func DiskList2Slice(l *list.List) []*DiskFile {
	diskFiles := make([]*DiskFile, 0)
	for d := l.Front(); d != nil; d = d.Next() {
		diskFiles = append(diskFiles, d.Value.(*DiskFile))
	}
	return diskFiles
}

/** 对几个level0文件的元素进行合并和更新
 * 当出现相同key时，要注意新旧关系
 * 参数elems默认从level0的链表转过来，index越小的文件越新
 */
func MergeUpdate(elems [][]*core.Element) []*core.Element {
	total_num := 0
	n := len(elems)
	for _, disk_elems := range elems {
		total_num += len(disk_elems)
	}
	res := make([]*core.Element, 0, total_num/2) // 考虑到不同文件可能存在重复的key
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = 0
	}
	for {
		min_key_disk_index := -1
		min_key := ""
		remain := 0
		for i := 0; i < n; i++ {
			if indices[i] < len(elems[i]) {
				remain += 1
				if min_key == "" || elems[i][indices[i]].Key < min_key {
					min_key_disk_index = i
					min_key = elems[i][indices[i]].Key
				}
			}
		}
		if remain <= 1 { // 只剩最多一个数组
			break
		}
		res = append(res, elems[min_key_disk_index][indices[min_key_disk_index]])
		indices[min_key_disk_index] += 1
	}
	for i := 0; i < n; i++ {
		res = append(res, elems[i][indices[i]:]...)
	}
	return res
}

func (t *LSMTree) GetDiskFiles() map[int]*list.List {
	return t.diskFiles
}
