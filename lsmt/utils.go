package lsmt

import (
	"LSM-Tree/core"
	"fmt"
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
