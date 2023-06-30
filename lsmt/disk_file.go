package lsmt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"sync/atomic"

	"LSM-Tree/avlTree"
	"LSM-Tree/config"
	"LSM-Tree/core"
	log "LSM-Tree/log"
)

var (
	/* 所有DiskFile对象共用的全局ID */
	globalID int32 = -1
)

/** 模拟一个磁盘文件
 */
type DiskFile struct {
	level     int              // 所属磁盘层级
	id        int32            // 每个文件独有的ID
	start_key string           // 文件中的最小键
	end_key   string           // 文件中的最大键
	index     *avlTree.AVLTree // 索引树
	size      int              // 文件中的键值对个数
	buf       bytes.Buffer     // 字节数组，键值对以字节数组形式顺序存储在buf中
}

func (d DiskFile) Empty() bool {
	return d.size == 0
}

/** 创建一个新的磁盘文件
* 注意，这里是在内存中用字节数组来模拟磁盘空间
* 对于一个elem：key，value，在磁盘文件中按写入顺序写入elem，再另外保存一棵索引树，
树的key即elem的key，value是elem的value在磁盘文件中的位置（第几个字节）
* 为了减少索引树的体积，每隔几个elem存储一个索引
*/
func NewDiskFile(elems []*core.Element, level int) *DiskFile {
	d := &DiskFile{
		size:  len(elems),
		id:    atomic.AddInt32(&globalID, 1),
		index: &avlTree.AVLTree{},
		level: level,
	}
	log.Logger.Info("Create new diskFile", "diskID", d.id)
	var indexElems []core.Element
	var enc *gob.Encoder
	indexDistance := config.DefaultConfig().IndexDistance
	for i, e := range elems {
		// log.Logger.Debug(fmt.Sprintf("writing to new diskfile %d, current elem.key: %v", d.id, e.Key))
		if i%indexDistance == 0 {
			// Create sparse index.
			idx := core.Element{Key: e.Key, Value: fmt.Sprintf("%d", d.buf.Len())}
			log.Trace("diskFile created sparse index element", "diskID", d.id, "key", idx.Key, "index", idx.Value)
			indexElems = append(indexElems, idx)
			enc = gob.NewEncoder(&d.buf)
		}
		enc.Encode(*e)
	}
	d.index.BatchAdd(indexElems)
	// 默认至少有一个元素
	d.start_key = elems[0].Key
	d.end_key = elems[len(elems)-1].Key
	return d
}

/** 在一个磁盘文件中搜索key，若搜到则返回该key对应的elem
 * 由于磁盘文件的索引树只索引了一部分elem，所以需要先从索引树中通过key的比较得到对应elem的存储区间，
再遍历该区间查找elem
*/
func (d *DiskFile) Search(key string) (core.Element, error) {
	canErr := fmt.Errorf("key %s not found in disk file", key)
	if d.Empty() {
		return core.Element{}, canErr
	}
	var si, ei int
	startNode := d.index.LowerBound(key)
	if startNode == nil {
		// Key smaller than all.
		log.Trace(fmt.Sprintf("Searching key: %v in diskFile %d, not found", key, d.id))
		return core.Element{}, canErr
	}
	si, _ = strconv.Atoi(startNode.Value)
	endNode := d.index.UpperBound(key)
	if endNode == nil {
		// Key larger than all or equal to the last one.
		ei = d.buf.Len()
	} else {
		ei, _ = strconv.Atoi(endNode.Value)
		// log.Logger.Debug(fmt.Sprintf("Searching key: %v in diskFile %d, endNode.key: %v, endNode.Val: %v", key, d.id, endNode.Key, endNode.Value))
	}
	// log.Logger.Debug(fmt.Sprintf("Searching key: %v in diskFile %d, searching in index range [%d,%d)]", key, d.id, si, ei))
	buf := bytes.NewBuffer(d.buf.Bytes()[si:ei])
	dec := gob.NewDecoder(buf)
	for {
		var e core.Element
		if err := dec.Decode(&e); err != nil {
			if err.Error() != "EOF" {
				log.Logger.Error("got err", "err", err)
			}
			break
		}
		if e.Key == key {
			log.Trace(fmt.Sprintf("Searching key: %v in diskFile %d, searching in index range [%d,%d)], and find it!", key, d.id, si, ei))
			return e, nil
		}
	}
	return core.Element{}, canErr
}

/** 返回一个磁盘文件中的所有elem
 */
func (d *DiskFile) AllElements() []*core.Element {
	indexElems := d.index.Inorder()
	var elems []*core.Element = make([]*core.Element, d.size+1)
	var dec *gob.Decoder
	cnt := 0
	for i, idx := range indexElems {
		start, _ := strconv.Atoi(idx.Value)
		end := d.buf.Len()
		if i < len(indexElems)-1 {
			end, _ = strconv.Atoi(indexElems[i+1].Value)
		}
		dec = gob.NewDecoder(bytes.NewBuffer(d.buf.Bytes()[start:end]))
		// var e core.Element
		for {
			elems[cnt] = &core.Element{}
			if dec.Decode(elems[cnt]) != nil {
				break
			}
			cnt += 1
		}

	}
	return elems[:len(elems)-1]
}
