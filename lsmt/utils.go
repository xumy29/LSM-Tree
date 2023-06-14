package lsmt

import (
	"LSM-Tree/core"
	"container/list"
	"fmt"
	"reflect"
)

func GenerateData(elemCnt int) []core.Element {
	elems := make([]core.Element, elemCnt)
	for i := 0; i < elemCnt; i++ {
		elem := core.Element{
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

func ListRemove(list *list.List, val interface{}) {
	for e := list.Front(); e != nil; e = e.Next() {
		// fmt.Printf("%v\n%v\n", e.Value, reflect.ValueOf(val).Interface())
		if e.Value == reflect.ValueOf(val).Interface() {
			list.Remove(e)
			break
		}
	}
}
