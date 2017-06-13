package bkdtree

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"bytes"

	"encoding/binary"
	"os"

	"time"

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
	bkd, err := NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if err != nil {
		t.Fatalf("%+v", err)
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
		if int(bkd.t0m.meta.NumPoints) != remained {
			t.Fatalf("bkd.numPoints %d, bkd.t0m %d is incorect, want %d", bkd.NumPoints, int(bkd.t0m.meta.NumPoints), remained)
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
	if err := bkd.Insert(points[bkdCap]); err == nil {
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
	bkd, err = NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap, dir, prefix)
	if err != nil {
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
	cnt := int(bkd.t0m.meta.NumPoints)
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

func (bkd *BkdTree) equal(bkd2 *BkdTree) (res bool) {
	if bkd.bkdCap != bkd2.bkdCap {
		fmt.Printf("bkd.bkdCap differ, %d %d\n", bkd.bkdCap, bkd2.bkdCap)
		return
	}
	if bkd.t0mCap != bkd2.t0mCap {
		fmt.Printf("bkd.t0mCap differ, %d %d\n", bkd.t0mCap, bkd2.t0mCap)
		return
	}
	if bkd.numDims != bkd2.numDims {
		fmt.Printf("bkd.numDims differ, %d %d\n", bkd.numDims, bkd2.numDims)
		return
	}
	if bkd.bytesPerDim != bkd2.bytesPerDim {
		fmt.Printf("bkd.bytesPerDim differ, %d %d\n", bkd.bytesPerDim, bkd2.bytesPerDim)
		return
	}
	if bkd.pointSize != bkd2.pointSize {
		fmt.Printf("bkd.pointSize differ, %d %d\n", bkd.pointSize, bkd2.pointSize)
		return
	}
	if bkd.leafCap != bkd2.leafCap {
		fmt.Printf("bkd.leafCap differ, %d %d\n", bkd.leafCap, bkd2.leafCap)
		return
	}
	if bkd.intraCap != bkd2.intraCap {
		fmt.Printf("bkd.intraCap differ, %d %d\n", bkd.intraCap, bkd2.intraCap)
		return
	}
	if bkd.dir != bkd2.dir {
		fmt.Printf("bkd.dir differ, %s %s\n", bkd.dir, bkd2.dir)
		return
	}
	if bkd.prefix != bkd2.prefix {
		fmt.Printf("bkd.prefix differ, %s %s\n", bkd.prefix, bkd2.prefix)
		return
	}
	if bkd.t0m.meta != bkd2.t0m.meta {
		fmt.Printf("bkd.t0m meta differ, %v %v\n", bkd.t0m.meta, bkd2.t0m.meta)
		return
	}

	if len(bkd.trees) != len(bkd2.trees) {
		fmt.Printf("bkd.trees length differ, %d %d\n", len(bkd.trees), len(bkd2.trees))
		return
	}
	for i := 0; i < len(bkd.trees); i++ {
		if bkd.trees[i].meta != bkd2.trees[i].meta {
			fmt.Printf("bkd.trees[%d] meta differ, %v %v\n", i, bkd.trees[i].meta, bkd2.trees[i].meta)
			return
		}
	}
	res = true
	return
}

func TestBkdOpenClose(t *testing.T) {
	var bkd, bkd2 *BkdTree
	var err error
	var maxVal uint64 = 1000
	bkd, _, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err = bkd.Close(); err != nil {
		t.Fatalf("%+v", err)
	}

	bkd2 = &BkdTree{}
	if err = bkd2.Open(bkd.bkdCap, bkd.dir, bkd.prefix); err != nil {
		t.Fatalf("%+v", err)
	}
	if !bkd.equal(bkd2) {
		t.Fatalf("bkd meta changed with close and open.")
	}
}

func bkdWriter(abort chan interface{}, bkd *BkdTree, points []Point) {
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP //break for
		default:
		}
		idx := rand.Intn(len(points))
		_, err := bkd.Erase(points[idx])
		if err != nil {
			panic(err)
		}
		err = bkd.Insert(points[idx])
		if err != nil {
			panic(err)
		}
	}
}

func bkdCloser(abort chan interface{}, bkd *BkdTree) {
	var interval time.Duration = 2 * time.Second
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP //break for
		default:
		}

		if err := bkd.Close(); err != nil {
			panic(err)
		}
		if err := bkd.Open(bkd.bkdCap, bkd.dir, bkd.prefix); err != nil {
			panic(err)
		}
		//sleep interval
		for {
			select {
			case <-time.After(interval):
			}
		}
	}
}

func bkdReader(abort chan interface{}, bkd *BkdTree, points []Point) {
FOR_LOOP:
	for {
		select {
		case <-abort:
			break FOR_LOOP //break for
		default:
		}

		idx1 := rand.Intn(len(points))
		idx2 := rand.Intn(len(points))
		visitor := &IntersectCollector{points[idx1], points[idx2], make([]Point, 0)}
		if err := bkd.Intersect(visitor); err != nil {
			panic(err)
		}
	}
}

func TestBkdConcurrentOps(t *testing.T) {
	var bkd *BkdTree
	var points []Point
	var err error
	var maxVal uint64 = 1000
	bkd, points, err = prepareBkdTree(maxVal)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	chs := make([]chan interface{}, 0)
	for i := 0; i < 1; i++ {
		ch := make(chan interface{}, 1)
		chs = append(chs, ch)
		go bkdCloser(ch, bkd)
	}
	for i := 0; i < 3; i++ {
		ch := make(chan interface{}, 1)
		chs = append(chs, ch)
		go bkdWriter(ch, bkd, points)
	}
	for i := 0; i < 5; i++ {
		ch := make(chan interface{}, 1)
		chs = append(chs, ch)
		go bkdReader(ch, bkd, points)
	}
	//sleep a while, send message to abort readers and writers
	for {
		select {
		case <-time.After(60 * time.Second):
			for _, ch := range chs {
				ch <- "abort"
			}
		case <-time.After(70 * time.Second):
			fmt.Println("children shall all have quited")
		}
	}
}
