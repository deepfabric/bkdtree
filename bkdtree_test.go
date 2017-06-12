package bkdtree

import (
	"fmt"
	"path/filepath"
	"testing"

	"bytes"

	"encoding/binary"
	"os"

	"github.com/pkg/errors"
)

func (n *KdTreeExtIntraNode) equal(n2 *KdTreeExtIntraNode) (res bool) {
	if n.SplitDim != n2.SplitDim || n.NumStrips != n2.NumStrips ||
		len(n.SplitValues) != len(n2.SplitValues) ||
		len(n.Children) != len(n2.Children) {
		res = false
		return
	}
	for i := 0; i < len(n.SplitValues); i++ {
		if n.SplitValues[i] != n2.SplitValues[i] {
			res = false
			return
		}
	}
	for i := 0; i < len(n.Children); i++ {
		if n.Children[i] != n2.Children[i] {
			res = false
			return
		}
	}
	res = true
	return
}

func TestIntraNodeReadWrite(t *testing.T) {
	n := KdTreeExtIntraNode{
		SplitDim:    1,
		NumStrips:   4,
		SplitValues: []uint64{3, 5, 7},
		Children: []KdTreeExtNodeInfo{
			{Offset: 0, NumPoints: 7},
			{Offset: 10, NumPoints: 9},
			{Offset: 20, NumPoints: 137},
			{Offset: 180, NumPoints: 999},
		},
	}
	bf := new(bytes.Buffer)
	if err := n.Write(bf); err != nil {
		t.Fatalf("%+v", err)
	}

	var n2 KdTreeExtIntraNode
	if err := n2.Read(bf); err != nil {
		t.Fatalf("%+v", err)
	}

	if !n.equal(&n2) {
		t.Fatalf("KdTreeExtIntraNode changes after encode and decode: %v, %v", n, n2)
	}
}
func TestBkdInsert(t *testing.T) {
	t0mCap := 1000
	treesCap := 5
	bkdCap := t0mCap<<uint(treesCap) - 1
	numDims := 2
	bytesPerDim := 4
	leafCap := 50
	intraCap := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd := NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if bkd == nil {
		t.Fatalf("bkd is nil")
	}

	maxVal := uint64(1000)
	size := bkdCap + 1
	points := NewRandPoints(numDims, maxVal, size)

	//insert points
	for i := 0; i < bkdCap; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
		if bkd.NumPoints != i+1 {
			t.Fatalf("incorrect numPoints. numPoints=%v, i=%v", bkd.NumPoints, i)
		}
		remained := bkd.NumPoints % bkd.t0mCap
		quotient := bkd.NumPoints / bkd.t0mCap
		if len(bkd.t0m) != remained {
			t.Fatalf("bkd.numPoints %d, len(bkd.t0m) %d is incorect, want %d", bkd.NumPoints, len(bkd.t0m), remained)
		}
		for i := 0; i < len(bkd.trees); i++ {
			tiCap := bkd.t0mCap << uint(i)
			want := tiCap * (quotient % 2)
			if bkd.trees[i].meta.NumPoints != uint64(want) {
				t.Fatalf("bkd.numPoints %d, bkd.tree[%d].numPoints %d is incorrect, want %d", bkd.NumPoints, i, bkd.trees[i].meta.NumPoints, want)
			}
			quotient >>= 1
		}
	}
	err := bkd.Insert(points[bkdCap])
	if err == nil {
		t.Fatalf("bkd.Insert shall fail if tree is full")
	}
}

func prepareBkdTree(maxVal uint64) (bkd *BkdTree, points []Point, err error) {
	t0mCap := 1000
	treesCap := 5
	bkdCap := t0mCap<<uint(treesCap) - 1
	numDims := 2
	bytesPerDim := 4
	leafCap := 50
	intraCap := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd = NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if bkd == nil {
		err = errors.Errorf("bkd is nil")
		return
	}
	//fmt.Printf("created BkdTree %v\n", bkd)

	size := bkdCap
	points = NewRandPoints(numDims, maxVal, size)
	for i := 0; i < bkdCap; i++ {
		err = bkd.Insert(points[i])
		if err != nil {
			err = errors.Errorf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
	}
	return
}

func TestBkdIntersect(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var lowPoint, highPoint Point
	var visitor *IntersectCollector

	//some intersect
	lowPoint = points[7]
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	var matched int
	for _, point := range visitor.points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
		if point.Equal(lowPoint) {
			matched++
		}
	}
	if matched != 1 {
		t.Errorf("found %d points equal to %v", matched, lowPoint)
	}

	//all intersect
	lowPoint = Point{[]uint64{0, 0, 0}, 0}
	highPoint = Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if len(visitor.points) != len(points) {
		t.Errorf("found %d matchs, want %d", len(visitor.points), len(points))
	}
}

func verifyBkdMeta(bkd *BkdTree) (err error) {
	cnt := len(bkd.t0m)
	var f *os.File
	for i := 0; i < len(bkd.trees); i++ {
		if bkd.trees[i].meta.NumPoints <= 0 {
			continue
		}
		fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, i))
		f, err = os.OpenFile(fp, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		_, err = f.Seek(-int64(KdTreeExtMetaSize), 2)
		if err != nil {
			return
		}
		var meta KdTreeExtMeta
		err = binary.Read(f, binary.BigEndian, &meta)
		if err != nil {
			return
		}
		if meta != bkd.trees[i].meta {
			err = errors.Errorf("bkd.trees[%d].meta does not match file content, has %v, want %v", i, bkd.trees[i].meta, meta)
			return
		}
		cnt += int(meta.NumPoints)
	}
	if cnt != bkd.NumPoints {
		err = errors.Errorf("bkd.numPoints does not match file content, has %v, want %v", bkd.NumPoints, cnt)
		return
	}
	return
}

func countPoint(bkd *BkdTree, point Point) (cnt int, err error) {
	lowPoint, highPoint := point, point
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	err = bkd.Intersect(visitor)
	if err != nil {
		return
	}
	for _, p := range visitor.points {
		if p.Equal(point) {
			cnt++
		}
	}
	return
}

func TestBkdErase(t *testing.T) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	var target Point
	var cnt int
	var found bool

	//erase an non-existing point
	target = points[17]
	target.UserData = uint64(len(points))
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%v", err)
	} else if found {
		t.Fatalf("point %v found, want non-existing", target)
	}

	//erase an existing point, verify the point really erased.
	target = points[13]
	found, err = bkd.Erase(target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if !found {
		t.Fatalf("point %v not found", target)
	} else if bkd.NumPoints != len(points)-1 {
		t.Fatalf("incorrect bkd.numPoints %d, want %d", bkd.NumPoints, len(points)-1)
	} else if err = verifyBkdMeta(bkd); err != nil {
		t.Fatalf("%+v", err)
	}

	cnt, err = countPoint(bkd, target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if cnt != 0 {
		t.Errorf("point %v still exists", target)
	}
	//there's room for insertion
	err = bkd.Insert(target)
	if err != nil {
		t.Fatalf("bkd.Insert failed, err: %+v", err)
	} else if bkd.NumPoints != len(points) {
		t.Fatalf("incorrect bkd.numPoints %d, want %d", bkd.NumPoints, len(points))
	} else if err = verifyBkdMeta(bkd); err != nil {
		t.Fatalf("%+v", err)
	}

	cnt, err = countPoint(bkd, target)
	if err != nil {
		t.Fatalf("%+v", err)
	} else if cnt != 1 {
		t.Errorf("point %v still exists", target)
	}
}

func BenchmarkBkdInsert(b *testing.B) {
	t0mCap := 1000
	treesCap := 20
	bkdCap := t0mCap<<uint(treesCap) - 1
	numDims := 2
	bytesPerDim := 4
	leafCap := 50
	intraCap := 4
	dir := "/tmp"
	prefix := "bkd"
	bkd := NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if bkd == nil {
		b.Fatalf("bkd is nil")
	}
	//fmt.Printf("created BkdTree %v\n", bkd)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := bkd.Insert(Point{[]uint64{uint64(i), uint64(i)}, uint64(i)})
		if err != nil {
			b.Fatalf("bkd.Insert failed, i=%v, err: %+v", i, err)
		}
	}
	return
}

func BenchmarkBkdErase(b *testing.B) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = bkd.Erase(points[i])
		if err != nil {
			b.Fatalf("%+v", err)
		}
	}
}

func BenchmarkBkdIntersect(b *testing.B) {
	var maxVal uint64 = 1000
	bkd, points, err := prepareBkdTree(maxVal)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	var lowPoint, highPoint Point
	var visitor *IntersectCollector
	lowPoint = points[7]
	highPoint = lowPoint
	visitor = &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = bkd.Intersect(visitor)
		if err != nil {
			b.Fatalf("%+v", err)
		} else if len(visitor.points) <= 0 {
			b.Errorf("found 0 matchs, however some expected")
		}
	}
}
