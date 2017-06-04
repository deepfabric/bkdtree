package bkdtree

import (
	"math/rand"
	"testing"
)

type PointBase struct {
	Point
	Vec   []int
	DocId uint64
}

func (b PointBase) GetValue(dim int) (val uint64) {
	val = uint64(b.Vec[dim])
	return
}

func (b PointBase) GetUserData() (userData uint64) {
	userData = b.DocId
	return
}

func NewPointBase(vals []int, docId uint64) PointBase {
	ret := PointBase{}
	for _, val := range vals {
		ret.Vec = append(ret.Vec, val)
	}
	ret.DocId = docId
	return ret
}

func NewRandPoints(numDims, maxVal, size int) (points []Point) {
	for i := 0; i < size; i++ {
		vals := make([]int, 0)
		for j := 0; j < numDims; j++ {
			vals = append(vals, rand.Intn(maxVal))
		}
		point := NewPointBase(vals, uint64(i))
		points = append(points, point)
	}
	return
}

func TestSplitPoints(t *testing.T) {
	numDims := 3
	maxVal := 100
	size := 1000
	numStrips := 4
	points := NewRandPoints(numDims, maxVal, size)
	for dim := 0; dim < numDims; dim++ {
		splitValues, splitPoses := SplitPoints(points, dim, numStrips)
		//fmt.Printf("points: %v\nsplitValues: %v\nsplitPoses:%v\n", points, splitValues, splitPoses)
		if len(splitValues) != numStrips-1 || len(splitValues) != len(splitPoses) {
			t.Errorf("incorrect size of splitValues or splitPoses\n")
		}
		numSplits := len(splitValues)
		for strip := 0; strip < numStrips; strip++ {
			posBegin := 0
			minValue := uint64(0)
			if strip != 0 {
				posBegin = splitPoses[strip-1]
				minValue = splitValues[strip-1]
			}
			posEnd := size
			maxValue := uint64(maxVal)
			if strip != numSplits {
				posEnd = splitPoses[strip]
				maxValue = splitValues[strip]
			}

			for pos := posBegin; pos < posEnd; pos++ {
				val := points[pos].GetValue(dim)
				if val < minValue {
					t.Errorf("points[%v][%v] %v is less than minValue %v", pos, dim, val, minValue)
				}
				if val > maxValue {
					t.Errorf("points[%v][%v] %v is larger than maxValue %v", pos, dim, val, maxValue)
				}
			}
		}
	}
}

type CaseInside struct {
	point, lowPoint, highPoint Point
	numDims                    int
	isInside                   bool
}

func TestIsInside(t *testing.T) {
	cases := []CaseInside{
		{
			NewPointBase([]int{30, 80, 40}, 0),
			NewPointBase([]int{30, 80, 40}, 0),
			NewPointBase([]int{50, 90, 50}, 0),
			3,
			true,
		},
		{
			NewPointBase([]int{30, 79, 40}, 0),
			NewPointBase([]int{30, 80, 40}, 0),
			NewPointBase([]int{50, 90, 50}, 0),
			3,
			false,
		},
		{ //invalid range
			NewPointBase([]int{30, 80, 40}, 0),
			NewPointBase([]int{30, 80, 40}, 0),
			NewPointBase([]int{50, 90, 39}, 0),
			3,
			false,
		},
	}

	for i, tc := range cases {
		res := IsInside(tc.point, tc.lowPoint, tc.highPoint, tc.numDims)
		if res != tc.isInside {
			t.Errorf("case %v failed\n", i)
		}
	}
}
