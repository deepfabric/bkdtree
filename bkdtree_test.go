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

func prepareBkdTree(t *testing.T, maxVal uint64) (bkd *BkdTree, points []Point) {
	t0mCap := 1000
	treesCap := 5
	numDims := 2
	bytesPerDim := 4
	leafCap := 50
	intraCap := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd = NewBkdTree(t0mCap, treesCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if bkd == nil {
		t.Fatalf("bkd is nil")
	}

	bkdCap := t0mCap<<uint(treesCap) - 1
	size := bkdCap
	points = NewRandPoints(numDims, maxVal, size)
	for i := 0; i < bkdCap; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %v", i, err)
		}
	}
	return
}

func TestBkdIntersect(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points := prepareBkdTree(t, maxVal)
	var lowPoint, highPoint Point
	var visitor *IntersectCollector
	var err error

	//some intersect
	lowPoint = points[7]
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	for _, point := range visitor.points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}

	//all intersect
	lowPoint = Point{[]uint64{0, 0, 0}, 0}
	highPoint = Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(visitor.points) != len(points) {
		t.Errorf("found %d matchs, want %d", len(visitor.points), len(points))
	}
}

func TestBkdErase(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points := prepareBkdTree(t, maxVal)
	var target, lowPoint, highPoint Point
	var visitor *IntersectCollector
	var found bool
	var err error

	//erase an non-existing point
	target = points[17]
	target.UserData = uint64(len(points))
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%v", err)
	} else if found {
		t.Fatalf("point %v found, want non-existing", target)
	}

	//erase an existing point
	target = points[13]
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%v", err)
	} else if !found {
		t.Fatalf("point %v not found", target)
	}

	// verify the point really erased.
	lowPoint = target
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%v", err)
	}
	for _, point := range visitor.points {
		if point.Equal(target) {
			t.Errorf("point %v still exists", target)
		}
	}
}
