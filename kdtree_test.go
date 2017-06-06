package bkdtree

import (
	"testing"
)

func TestKdIntersectSome(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, 1)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	for _, point := range visitor.points {
		isInside := IsInside(point, lowPoint, highPoint, numDims)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}
func TestKdIntersectAll(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := NewPointBase([]uint64{0, 0, 0}, 0)
	highPoint := NewPointBase([]uint64{maxVal, maxVal, maxVal}, 0)
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, size)}
	kdt.Intersect(visitor)
	if len(visitor.points) != size {
		t.Errorf("found %v matchs, however %v expected", len(visitor.points), size)
	}
}

func TestKdIntersect(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 100000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := NewPointBase([]uint64{20, 30, 40}, 0)
	highPoint := NewPointBase([]uint64{maxVal, maxVal, maxVal}, 0)
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	for _, point := range visitor.points {
		isInside := IsInside(point, lowPoint, highPoint, numDims)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}

func TestKdInsert(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	newPoint := NewPointBase([]uint64{40, 30, 20}, maxVal) //use unique userData
	kdt.Insert(newPoint)

	lowPoint := newPoint
	highPoint := newPoint
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	numMatchs := 0
	for _, point := range visitor.points {
		isInside := IsInside(point, lowPoint, highPoint, numDims)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
		if point.GetUserData() == newPoint.GetUserData() {
			numMatchs++
		}
	}
	if numMatchs != 1 {
		t.Errorf("found %v matchs, however 1 expected", numMatchs)
	}
}

func TestKdErase(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	kdt.Erase(points[0])

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) != 0 {
		t.Errorf("found %v matchs, however 0 expected", len(visitor.points))
	}
}
