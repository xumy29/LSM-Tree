package avlTree

import (
	"LSM-Tree/core"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	opAdd = iota
	opRemove
	opSearch
)

func TestTreePerformance(t *testing.T) {
	tree := &AVLTree{}
	m := make(map[string]string)

	const maxKey = 20000
	const nops = 1000000
	for i := 0; i < nops; i++ {
		op := rand.Intn(3)
		k := fmt.Sprintf("%d", rand.Intn(maxKey))

		switch op {
		case opAdd:
			v := fmt.Sprintf("%d", rand.Int())
			tree.Add(k, v)
			m[k] = v
		case opRemove:
			tree.Remove(k)
			delete(m, k)
		case opSearch:
			node := tree.Search(k)
			tok := node != nil
			mv, mok := m[k]
			if tok != mok {
				t.Errorf("Incorrect key searching. key: %v, want {ok} : {%v}, got: {%v}", k, mok, tok)
				continue
			}
			if tok && node.Value != mv {
				t.Errorf("Incorrect key searching. key: %v, want {val} : {%v}, got: {%v}", k, mv, node.Value)
			}
		}
	}
}

func TestLowerUpperBound(t *testing.T) {
	tree := AVLTree{}
	key := ""
	val := "b"
	for i := 0; i < 5; i++ {
		key += "x"
		tree.Add(key, val)
	}
	assert.Equal(t, true, tree.LowerBound("a") == nil)
	assert.Equal(t, "x", tree.LowerBound("x").Key)
	assert.Equal(t, "xxxxx", tree.LowerBound("xxxxx").Key)
	assert.Equal(t, "xxxxx", tree.LowerBound("xxxxxxxx").Key)
	assert.Equal(t, "xxxxx", tree.LowerBound("y").Key)

	assert.Equal(t, "x", tree.UpperBound("a").Key)
	assert.Equal(t, "xx", tree.UpperBound("x").Key)
	assert.Equal(t, "xxxxx", tree.UpperBound("xxxx").Key)
	assert.Equal(t, true, tree.UpperBound("xxxxx") == nil)
	assert.Equal(t, true, tree.UpperBound("y") == nil)
}

func TestInorder(t *testing.T) {
	tree := AVLTree{}
	key := ""
	val := "b"

	expected := make([]*core.Element, 0)
	for i := 0; i < 5; i++ {
		key += "x"
		expected = append(expected, &core.Element{Key: key, Value: val})
		tree.Add(key, val)
	}

	got := tree.Inorder()
	assert.Equal(t, expected, got)

	e := &core.Element{Key: "xxa", Value: val}
	expected = append(expected[:2], append([]*core.Element{e}, expected[2:]...)...)
	tree.Add("xxa", val)
	got = tree.Inorder()
	assert.Equal(t, expected, got)
}
