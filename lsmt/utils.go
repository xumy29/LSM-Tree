package lsmt

import (
	"LSM-Tree/core"
	log "LSM-Tree/log"
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
 * 参数elems默认从level0的链表按顺序转换过来，index越小的文件越新
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
		// 更新min_key_disk_index对应的数组的下标，同时也去除其他文件中的重复key的较旧记录
		for i := 0; i < n; i++ {
			if indices[i] < len(elems[i]) && elems[i][indices[i]].Key == min_key {
				indices[i] += 1
			}
		}
	}
	for i := 0; i < n; i++ {
		res = append(res, elems[i][indices[i]:]...)
	}
	return res
}

func (t *LSMTree) GetDiskFiles() map[int]*list.List {
	return t.diskFiles
}

func (t *LSMTree) Print_Files_0_1_Ranges() {
	for i := 0; i < 2; i++ {
		files := t.diskFiles[i]
		ids := make([]int, 0)
		ranges := make([][2]string, 0)
		for e := files.Front(); e != nil; e = e.Next() {
			d := e.Value.(*DiskFile)
			ids = append(ids, int(d.id))
			ranges = append(ranges, d.GetKeyRange())
		}
		log.Logger.Debug("files info", "level", i, "file_cnt", files.Len(), "ids", ids, "ranges", ranges)
	}

}

func (t *LSMTree) Log_file_info() {
	t.Print_Files_0_1_Ranges()
	files := t.GetDiskFiles()
	cnt := 0
	log.Logger.Debug("final file1 info:")
	for e := files[1].Front(); e != nil; e = e.Next() {
		d := e.Value.(*DiskFile)
		key_range := d.GetKeyRange()
		log.Logger.Debug(fmt.Sprintf("file %d, size = %d, key_range=[%v,%v]", d.GetID(), d.GetSize(), key_range[0], key_range[1]))
		cnt += d.GetSize()
	}
	log.Logger.Debug(fmt.Sprintf("total sizes: %d", cnt))

}
