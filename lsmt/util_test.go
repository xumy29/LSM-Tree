package lsmt

import (
	"container/list"
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
