package engine

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dp(s string) decimal.Decimal { v, _ := decimal.NewFromString(s); return v }

// collect drains the tree in ascending order into a slice of strings.
func collectAsc(t *rbTree) []string {
	var out []string
	t.ascend(func(p decimal.Decimal) bool {
		out = append(out, p.String())
		return true
	})
	return out
}

func collectDesc(t *rbTree) []string {
	var out []string
	t.descend(func(p decimal.Decimal) bool {
		out = append(out, p.String())
		return true
	})
	return out
}

// ── invariant checker ─────────────────────────────────────────────────────────

// checkInvariants verifies all four red-black invariants and returns the
// black-height of the tree (number of black nodes on any root-to-nil path).
func checkInvariants(t *testing.T, tree *rbTree) {
	t.Helper()
	if tree.root == nil {
		return
	}
	require.Equal(t, black, tree.root.color, "root must be black")
	blackHeight(t, tree.root)
}

// blackHeight recursively checks invariants 3 & 4 and returns the black-height.
func blackHeight(t *testing.T, n *rbNode) int {
	t.Helper()
	if n == nil {
		return 1 // nil counts as black
	}
	if n.color == red {
		if (n.left != nil && n.left.color == red) || (n.right != nil && n.right.color == red) {
			t.Errorf("red node %s has a red child (consecutive reds)", n.price)
		}
	}
	lh := blackHeight(t, n.left)
	rh := blackHeight(t, n.right)
	if lh != rh {
		t.Errorf("black-height mismatch at %s: left=%d right=%d", n.price, lh, rh)
	}
	bh := lh
	if n.color == black {
		bh++
	}
	return bh
}

// ── unit tests ────────────────────────────────────────────────────────────────

func TestRBTree_InsertAscendingOrder(t *testing.T) {
	var tree rbTree
	prices := []string{"100", "200", "300", "400", "500"}
	for _, p := range prices {
		tree.insert(dp(p))
	}
	checkInvariants(t, &tree)
	assert.Equal(t, prices, collectAsc(&tree))
	assert.Equal(t, 5, tree.size)
}

func TestRBTree_InsertDescendingOrder(t *testing.T) {
	var tree rbTree
	prices := []string{"500", "400", "300", "200", "100"}
	for _, p := range prices {
		tree.insert(dp(p))
	}
	checkInvariants(t, &tree)
	assert.Equal(t, []string{"100", "200", "300", "400", "500"}, collectAsc(&tree))
}

func TestRBTree_InsertDuplicateIsNoop(t *testing.T) {
	var tree rbTree
	tree.insert(dp("100"))
	tree.insert(dp("100"))
	assert.Equal(t, 1, tree.size)
	checkInvariants(t, &tree)
}

func TestRBTree_MinMax(t *testing.T) {
	var tree rbTree
	for _, p := range []string{"300", "100", "500", "200", "400"} {
		tree.insert(dp(p))
	}
	require.NotNil(t, tree.min())
	require.NotNil(t, tree.max())
	assert.Equal(t, "100", tree.min().price.String())
	assert.Equal(t, "500", tree.max().price.String())
}

func TestRBTree_MinMaxEmpty(t *testing.T) {
	var tree rbTree
	assert.Nil(t, tree.min())
	assert.Nil(t, tree.max())
}

func TestRBTree_DeleteLeaf(t *testing.T) {
	var tree rbTree
	for _, p := range []string{"200", "100", "300"} {
		tree.insert(dp(p))
	}
	tree.delete(dp("100"))
	checkInvariants(t, &tree)
	assert.Equal(t, []string{"200", "300"}, collectAsc(&tree))
	assert.Equal(t, 2, tree.size)
}

func TestRBTree_DeleteRoot(t *testing.T) {
	var tree rbTree
	tree.insert(dp("200"))
	tree.insert(dp("100"))
	tree.insert(dp("300"))
	tree.delete(dp("200"))
	checkInvariants(t, &tree)
	assert.Equal(t, []string{"100", "300"}, collectAsc(&tree))
}

func TestRBTree_DeleteAllNodes(t *testing.T) {
	var tree rbTree
	prices := []string{"100", "200", "300", "400", "500"}
	for _, p := range prices {
		tree.insert(dp(p))
	}
	for _, p := range prices {
		tree.delete(dp(p))
		checkInvariants(t, &tree)
	}
	assert.Equal(t, 0, tree.size)
	assert.Nil(t, tree.root)
}

func TestRBTree_DeleteNonExistentIsNoop(t *testing.T) {
	var tree rbTree
	tree.insert(dp("100"))
	tree.delete(dp("999"))
	assert.Equal(t, 1, tree.size)
	checkInvariants(t, &tree)
}

func TestRBTree_DescendOrder(t *testing.T) {
	var tree rbTree
	for _, p := range []string{"300", "100", "500", "200", "400"} {
		tree.insert(dp(p))
	}
	assert.Equal(t, []string{"500", "400", "300", "200", "100"}, collectDesc(&tree))
}

func TestRBTree_AscendEarlyStop(t *testing.T) {
	var tree rbTree
	for _, p := range []string{"100", "200", "300", "400", "500"} {
		tree.insert(dp(p))
	}
	var seen []string
	tree.ascend(func(p decimal.Decimal) bool {
		seen = append(seen, p.String())
		return p.String() != "300" // stop after 300
	})
	assert.Equal(t, []string{"100", "200", "300"}, seen)
}

// ── stress / property test ────────────────────────────────────────────────────

// TestRBTree_RandomInsertDelete inserts and deletes random prices, verifying
// invariants after every operation and that the in-order output matches a
// sorted reference set.
func TestRBTree_RandomInsertDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const N = 2000
	rng := rand.New(rand.NewSource(42))
	var tree rbTree
	reference := make(map[string]bool)

	for i := 0; i < N; i++ {
		p := decimal.NewFromInt(rng.Int63n(500) + 1)
		key := p.String()
		if reference[key] {
			// delete an existing price
			tree.delete(p)
			delete(reference, key)
		} else {
			tree.insert(p)
			reference[key] = true
		}
		checkInvariants(t, &tree)
		assert.Equal(t, len(reference), tree.size)
	}

	// Final in-order check: tree output must match sorted reference.
	want := make([]string, 0, len(reference))
	for k := range reference {
		want = append(want, k)
	}
	sort.Slice(want, func(i, j int) bool {
		return dp(want[i]).LessThan(dp(want[j]))
	})
	assert.Equal(t, want, collectAsc(&tree))
}

// ── integration: halfBook uses the tree correctly ─────────────────────────────

func TestHalfBook_BidsBestIsHighest(t *testing.T) {
	b := newHalfBook(Buy)
	b.addOrder(limit(Buy, "100", "1"))
	b.addOrder(limit(Buy, "103", "1"))
	b.addOrder(limit(Buy, "101", "1"))

	best := b.bestLevel()
	require.NotNil(t, best)
	assert.True(t, best.Price.Equal(dp("103")), "best bid should be highest price")
}

func TestHalfBook_AsksBestIsLowest(t *testing.T) {
	b := newHalfBook(Sell)
	b.addOrder(limit(Sell, "103", "1"))
	b.addOrder(limit(Sell, "100", "1"))
	b.addOrder(limit(Sell, "101", "1"))

	best := b.bestLevel()
	require.NotNil(t, best)
	assert.True(t, best.Price.Equal(dp("100")), "best ask should be lowest price")
}

func TestHalfBook_PruneCleansTree(t *testing.T) {
	b := newHalfBook(Sell)
	o := limit(Sell, "100", "1")
	b.addOrder(o)
	assert.Equal(t, 1, b.tree.size)

	b.removeOrderByID(o.ID, o.Price)
	assert.Equal(t, 0, b.tree.size)
	assert.Nil(t, b.bestLevel())
}

func TestHalfBook_SnapshotBidsDescending(t *testing.T) {
	b := newHalfBook(Buy)
	b.addOrder(limit(Buy, "101", "1"))
	b.addOrder(limit(Buy, "103", "1"))
	b.addOrder(limit(Buy, "102", "1"))

	snap := b.snapshot()
	require.Len(t, snap, 3)
	assert.Equal(t, "103", snap[0].Price)
	assert.Equal(t, "102", snap[1].Price)
	assert.Equal(t, "101", snap[2].Price)
}

func TestHalfBook_SnapshotAsksAscending(t *testing.T) {
	b := newHalfBook(Sell)
	b.addOrder(limit(Sell, "103", "1"))
	b.addOrder(limit(Sell, "101", "1"))
	b.addOrder(limit(Sell, "102", "1"))

	snap := b.snapshot()
	require.Len(t, snap, 3)
	assert.Equal(t, "101", snap[0].Price)
	assert.Equal(t, "102", snap[1].Price)
	assert.Equal(t, "103", snap[2].Price)
}
