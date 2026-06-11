package engine

import "github.com/shopspring/decimal"

// color represents a red-black tree node color.
type color bool

const (
	red   color = true
	black color = false
)

// rbNode is a node in the red-black tree. The tree is keyed by price.
type rbNode struct {
	price               decimal.Decimal
	color               color
	left, right, parent *rbNode
}

// rbTree is an ordered set of prices. It supports O(log n) insert, delete,
// and min/max, replacing the O(n) sorted slice in halfBook.
//
// Invariants (standard red-black):
//  1. Every node is red or black.
//  2. The root is black.
//  3. No two consecutive red nodes on any path.
//  4. Every path from root to nil has the same number of black nodes.
type rbTree struct {
	root *rbNode
	size int
}

// min returns the node with the smallest price, or nil if the tree is empty.
func (t *rbTree) min() *rbNode {
	return subtreeMin(t.root)
}

// max returns the node with the largest price, or nil if the tree is empty.
func (t *rbTree) max() *rbNode {
	return subtreeMax(t.root)
}

func subtreeMin(n *rbNode) *rbNode {
	if n == nil {
		return nil
	}
	for n.left != nil {
		n = n.left
	}
	return n
}

func subtreeMax(n *rbNode) *rbNode {
	if n == nil {
		return nil
	}
	for n.right != nil {
		n = n.right
	}
	return n
}

// successor returns the in-order successor of n (next larger price), or nil.
func successor(n *rbNode) *rbNode {
	if n.right != nil {
		return subtreeMin(n.right)
	}
	p := n.parent
	for p != nil && n == p.right {
		n = p
		p = p.parent
	}
	return p
}

// find returns the node with the given price, or nil.
func (t *rbTree) find(price decimal.Decimal) *rbNode {
	cur := t.root
	for cur != nil {
		cmp := price.Cmp(cur.price)
		switch {
		case cmp < 0:
			cur = cur.left
		case cmp > 0:
			cur = cur.right
		default:
			return cur
		}
	}
	return nil
}

// insert adds price to the tree (no-op if already present).
func (t *rbTree) insert(price decimal.Decimal) {
	var parent *rbNode
	cur := &t.root
	for *cur != nil {
		parent = *cur
		cmp := price.Cmp(parent.price)
		if cmp < 0 {
			cur = &parent.left
		} else if cmp > 0 {
			cur = &parent.right
		} else {
			return // already present
		}
	}
	n := &rbNode{price: price, color: red, parent: parent}
	*cur = n
	t.size++
	t.insertFixup(n)
}

func (t *rbTree) insertFixup(n *rbNode) {
	for n.parent != nil && n.parent.color == red {
		p := n.parent
		g := p.parent
		if g == nil {
			break
		}
		if p == g.left {
			uncle := g.right
			if uncle != nil && uncle.color == red {
				// Case 1: uncle is red — recolor and move up.
				p.color = black
				uncle.color = black
				g.color = red
				n = g
			} else {
				if n == p.right {
					// Case 2: n is a right child — left-rotate parent.
					t.rotateLeft(p)
					n, p = p, n
				}
				// Case 3: n is a left child — right-rotate grandparent.
				p.color = black
				g.color = red
				t.rotateRight(g)
			}
		} else {
			uncle := g.left
			if uncle != nil && uncle.color == red {
				p.color = black
				uncle.color = black
				g.color = red
				n = g
			} else {
				if n == p.left {
					t.rotateRight(p)
					n, p = p, n
				}
				p.color = black
				g.color = red
				t.rotateLeft(g)
			}
		}
	}
	t.root.color = black
}

// delete removes the node with the given price (no-op if not found).
func (t *rbTree) delete(price decimal.Decimal) {
	n := t.find(price)
	if n == nil {
		return
	}
	t.deleteNode(n)
	t.size--
}

func (t *rbTree) deleteNode(n *rbNode) {
	// y is the node actually spliced out; x replaces it.
	var x, xParent *rbNode
	y := n

	if n.left == nil {
		x = n.right
		xParent = n.parent
		t.transplant(n, x)
	} else if n.right == nil {
		x = n.left
		xParent = n.parent
		t.transplant(n, x)
	} else {
		// Replace n with its in-order successor (minimum of right subtree).
		y = subtreeMin(n.right)
		yOrigColor := y.color
		x = y.right
		if y.parent == n {
			xParent = y
		} else {
			xParent = y.parent
			t.transplant(y, x)
			y.right = n.right
			y.right.parent = y
		}
		t.transplant(n, y)
		y.left = n.left
		y.left.parent = y
		y.color = n.color
		if yOrigColor == black {
			t.deleteFixup(x, xParent)
		}
		return
	}

	if y.color == black {
		t.deleteFixup(x, xParent)
	}
}

func (t *rbTree) deleteFixup(x *rbNode, xParent *rbNode) {
	for x != t.root && (x == nil || x.color == black) {
		if x == xParent.left {
			w := xParent.right
			if w != nil && w.color == red {
				// Case 1: sibling is red.
				w.color = black
				xParent.color = red
				t.rotateLeft(xParent)
				w = xParent.right
			}
			if w == nil {
				x = xParent
				xParent = x.parent
			} else if (w.left == nil || w.left.color == black) &&
				(w.right == nil || w.right.color == black) {
				// Case 2: sibling's children are both black.
				w.color = red
				x = xParent
				xParent = x.parent
			} else {
				if w.right == nil || w.right.color == black {
					// Case 3: sibling's right child is black.
					if w.left != nil {
						w.left.color = black
					}
					w.color = red
					t.rotateRight(w)
					w = xParent.right
				}
				// Case 4: sibling's right child is red.
				w.color = xParent.color
				xParent.color = black
				if w.right != nil {
					w.right.color = black
				}
				t.rotateLeft(xParent)
				x = t.root
				xParent = nil
			}
		} else {
			w := xParent.left
			if w != nil && w.color == red {
				w.color = black
				xParent.color = red
				t.rotateRight(xParent)
				w = xParent.left
			}
			if w == nil {
				x = xParent
				xParent = x.parent
			} else if (w.right == nil || w.right.color == black) &&
				(w.left == nil || w.left.color == black) {
				w.color = red
				x = xParent
				xParent = x.parent
			} else {
				if w.left == nil || w.left.color == black {
					if w.right != nil {
						w.right.color = black
					}
					w.color = red
					t.rotateLeft(w)
					w = xParent.left
				}
				w.color = xParent.color
				xParent.color = black
				if w.left != nil {
					w.left.color = black
				}
				t.rotateRight(xParent)
				x = t.root
				xParent = nil
			}
		}
	}
	if x != nil {
		x.color = black
	}
}

// transplant replaces subtree rooted at u with subtree rooted at v.
func (t *rbTree) transplant(u, v *rbNode) {
	if u.parent == nil {
		t.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	if v != nil {
		v.parent = u.parent
	}
}

// rotateLeft performs a left rotation around n.
//
//	n                y
//	 \      →       /
//	  y            n
func (t *rbTree) rotateLeft(n *rbNode) {
	y := n.right
	n.right = y.left
	if y.left != nil {
		y.left.parent = n
	}
	y.parent = n.parent
	if n.parent == nil {
		t.root = y
	} else if n == n.parent.left {
		n.parent.left = y
	} else {
		n.parent.right = y
	}
	y.left = n
	n.parent = y
}

// rotateRight performs a right rotation around n.
//
//	  n            y
//	 /      →       \
//	y                n
func (t *rbTree) rotateRight(n *rbNode) {
	y := n.left
	n.left = y.right
	if y.right != nil {
		y.right.parent = n
	}
	y.parent = n.parent
	if n.parent == nil {
		t.root = y
	} else if n == n.parent.right {
		n.parent.right = y
	} else {
		n.parent.left = y
	}
	y.right = n
	n.parent = y
}

// ascend calls fn for each price in ascending order, stopping if fn returns false.
func (t *rbTree) ascend(fn func(decimal.Decimal) bool) {
	var walk func(*rbNode) bool
	walk = func(n *rbNode) bool {
		if n == nil {
			return true
		}
		if !walk(n.left) {
			return false
		}
		if !fn(n.price) {
			return false
		}
		return walk(n.right)
	}
	walk(t.root)
}

// descend calls fn for each price in descending order, stopping if fn returns false.
func (t *rbTree) descend(fn func(decimal.Decimal) bool) {
	var walk func(*rbNode) bool
	walk = func(n *rbNode) bool {
		if n == nil {
			return true
		}
		if !walk(n.right) {
			return false
		}
		if !fn(n.price) {
			return false
		}
		return walk(n.left)
	}
	walk(t.root)
}
