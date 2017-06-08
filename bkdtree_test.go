package bkdtree

import (
	"testing"
)

func TestBkdInsert(t *testing.T) {
	t0mCap := 1000
	treesCap := 5
	numDims := 2
	bytesPerDim := 4
	leafCap := 50
	intraCap := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd := NewBkdTree(t0mCap, treesCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if bkd == nil {
		t.Fatalf("bkd is nil")
	}

	bkdCap := t0mCap<<uint(treesCap) - 1
	maxVal := uint64(1000)
	size := bkdCap + 1
	points := NewRandPoints(numDims, maxVal, size)

	//insert points
	for i := 0; i < bkdCap; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %v", i, err)
		}
		if bkd.numPoints != i+1 {
			t.Fatalf("incorrect numPoints. numPoints=%v, i=%v", bkd.numPoints, i)
		}
		remained := bkd.numPoints % bkd.t0mCap
		quotient := bkd.numPoints / bkd.t0mCap
		if len(bkd.t0m) != remained {
			t.Fatalf("bkd.numPoints %d, len(bkd.t0m) %d is incorect, want %d", bkd.numPoints, len(bkd.t0m), remained)
		}
		for i := 0; i < len(bkd.trees); i++ {
			tiCap := bkd.t0mCap << uint(i)
			want := tiCap * (quotient % 2)
			if bkd.trees[i].numPoints != uint64(want) {
				t.Fatalf("bkd.numPoints %d, bkd.tree[%d].numPoints %d is incorrect, want %d", bkd.numPoints, i, bkd.trees[i].numPoints, want)
			}
			quotient >>= 1
		}
	}
	err := bkd.Insert(points[bkdCap])
	if err == nil {
		t.Fatalf("bkd.Insert shall fail if tree is full")
	}
}
