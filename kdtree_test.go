package bkdtree

import (
	"testing"
)

func TestIntersectSome(t *testing.T) {
	numDims := 3
	maxVal := 1000
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
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
func TestIntersectAll(t *testing.T) {
	numDims := 3
	maxVal := 1000
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := NewPointBase([]int{0, 0, 0}, 0)
	highPoint := NewPointBase([]int{maxVal, maxVal, maxVal}, 0)
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)
	if len(visitor.points) != size {
		t.Errorf("found %v matchs, however %v expected", len(visitor.points), size)
	}
}

func TestIntersect(t *testing.T) {
	numDims := 3
	maxVal := 1000
	size := 100000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKDTree(points, numDims)

	lowPoint := NewPointBase([]int{20, 30, 40}, 0)
	highPoint := NewPointBase([]int{maxVal, maxVal, maxVal}, 0)
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
