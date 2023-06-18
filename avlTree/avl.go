package avlTree

import (
	"LSM-Tree/core"
	"fmt"
)

type AVLTree struct {
	root *AVLNode
	size int
}

/** 若key存在则更新value，若key不存在则插入新节点
 * 插入成功返回1，更新成功返回0
 */
func (t *AVLTree) Add(key string, value string) int {
	var isAdd bool
	t.root, isAdd = t.root.add(key, value)
	if isAdd {
		t.size += 1
		return 1
	}
	return 0
}

func (t *AVLTree) BatchAdd(elems []core.Element) {
	for _, e := range elems {
		t.Add(e.Key, e.Value)
	}
}

func (t *AVLTree) Remove(key string) {
	if t.root == nil {
		return
	}
	t.root = t.root.remove(key)
}

func (t *AVLTree) Search(key string) *AVLNode {
	if t.root == nil {
		return nil
	}
	return t.root.search(key)
}

/** 查找小于等于参数key的最大key对应的节点
 * 若查找不到满足的节点，则返回nil
 */
func (t *AVLTree) LowerBound(key string) *AVLNode {
	if t.root == nil {
		return nil
	}
	return t.root.lowerBound(key)
}

/** 查找大于参数key的最小key对应的节点
 * 若查找不到满足的节点，则返回nil
 */
func (t *AVLTree) UpperBound(key string) *AVLNode {
	if t.root == nil {
		return nil
	}
	return t.root.upperBound(key)
}

/* 按中序遍历整棵树并打印节点数组 */
func (t *AVLTree) DisplayInOrder() {
	elems := t.Inorder()
	fmt.Printf("%v\n", elems)
}

/* 按中序遍历整棵树并返回节点数组 */
func (t *AVLTree) Inorder() (nodes []*core.Element) {
	if t.root == nil {
		return make([]*core.Element, 0)
	}
	nodes = make([]*core.Element, 0, t.size)
	t.root.inorder(&nodes)

	return
}

func (t *AVLTree) Height() int {
	if t.root == nil {
		return 0
	}
	return t.root.height
}

func (t *AVLTree) Size() int {
	return t.size
}

type AVLNode struct {
	Key    string
	Value  string
	height int
	left   *AVLNode
	right  *AVLNode
}

func (n *AVLNode) add(key string, value string) (node *AVLNode, isAdd bool) {
	if key == "" {
		fmt.Printf("empty key not supported!\n")
		return n, false
	}
	if n == nil {
		return &AVLNode{key, value, 1, nil, nil}, true
	}

	if key < n.Key {
		n.left, isAdd = n.left.add(key, value)
	} else if key > n.Key {
		n.right, isAdd = n.right.add(key, value)
	} else {
		n.Value = value
		isAdd = false
	}
	// 只有isAdd==true即有新节点插入时，才进行rebalance
	if isAdd {
		return n.rebalanceTree(), true
	}
	return n, false

}

func (n *AVLNode) remove(key string) *AVLNode {
	if n == nil {
		return nil
	}
	if key < n.Key {
		n.left = n.left.remove(key)
	} else if key > n.Key {
		n.right = n.right.remove(key)
	} else {
		if n.left != nil && n.right != nil {
			// node to delete found with both children;
			// replace values with smallest node of the right sub-tree
			rightMinNode := n.right.findSmallest()
			n.Key = rightMinNode.Key
			n.Value = rightMinNode.Value
			// delete smallest node that we replaced
			n.right = n.right.remove(rightMinNode.Key)
		} else if n.left != nil {
			// node only has left child
			n = n.left
		} else if n.right != nil {
			// node only has right child
			n = n.right
		} else {
			// node has no children
			n = nil
			return n
		}

	}
	return n.rebalanceTree()
}

// Searches for a node
func (n *AVLNode) search(key string) *AVLNode {
	if n == nil {
		return nil
	}
	if key < n.Key {
		return n.left.search(key)
	} else if key > n.Key {
		return n.right.search(key)
	} else {
		return n
	}
}

/* 查找小于等于参数key的最大key对应的节点 */
func (n *AVLNode) lowerBound(key string) *AVLNode {
	if n == nil {
		return nil
	}
	if n.Key <= key {
		rightTreeResult := n.right.lowerBound(key)
		if rightTreeResult == nil {
			return n
		}
		return rightTreeResult
	} else {
		return n.left.lowerBound(key)
	}
}

func (n *AVLNode) upperBound(key string) *AVLNode {
	if n == nil {
		return nil
	}
	if n.Key > key {
		leftTreeResult := n.left.upperBound(key)
		if leftTreeResult == nil {
			return n
		}
		return leftTreeResult
	} else {
		return n.right.upperBound(key)
	}
}

func (n *AVLNode) inorder(nodes *[]*core.Element) {
	if n == nil {
		return
	}
	if n.left != nil {
		n.left.inorder(nodes)
	}
	*nodes = append(*nodes, &core.Element{
		Key:   n.Key,
		Value: n.Value,
	})
	if n.right != nil {
		n.right.inorder(nodes)
	}
}

func (n *AVLNode) getHeight() int {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *AVLNode) recalculateHeight() {
	n.height = 1 + max(n.left.getHeight(), n.right.getHeight())
}

// Checks if node is balanced and rebalance
func (n *AVLNode) rebalanceTree() *AVLNode {
	if n == nil {
		return n
	}
	n.recalculateHeight()

	// check balance factor and rotateLeft if right-heavy and rotateRight if left-heavy
	balanceFactor := n.left.getHeight() - n.right.getHeight()
	if balanceFactor == -2 {
		// check if child is left-heavy and rotateRight first
		if n.right.left.getHeight() > n.right.right.getHeight() {
			n.right = n.right.rotateRight()
		}
		return n.rotateLeft()
	} else if balanceFactor == 2 {
		// check if child is right-heavy and rotateLeft first
		if n.left.right.getHeight() > n.left.left.getHeight() {
			n.left = n.left.rotateLeft()
		}
		return n.rotateRight()
	}
	return n
}

// Rotate nodes left to balance node
func (n *AVLNode) rotateLeft() *AVLNode {
	newRoot := n.right
	n.right = newRoot.left
	newRoot.left = n

	n.recalculateHeight()
	newRoot.recalculateHeight()
	return newRoot
}

// Rotate nodes right to balance node
func (n *AVLNode) rotateRight() *AVLNode {
	newRoot := n.left
	n.left = newRoot.right
	newRoot.right = n

	n.recalculateHeight()
	newRoot.recalculateHeight()
	return newRoot
}

// Finds the smallest child (based on the key) for the current node
func (n *AVLNode) findSmallest() *AVLNode {
	if n.left != nil {
		return n.left.findSmallest()
	} else {
		return n
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
