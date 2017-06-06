package bkdtree

import (
	"math/rand"
	"testing"
)

func NewRandPoints(numDims int, maxVal uint64, size int) (points []Point) {
	for i := 0; i < size; i++ {
		vals := make([]uint64, 0, numDims)
		for j := 0; j < numDims; j++ {
			vals = append(vals, rand.Uint64()%maxVal)
		}
		point := NewPointBase(vals, uint64(i))
		points = append(points, point)
	}
	return
}

func TestSplitPoints(t *testing.T) {
	numDims := 3
	maxVal := uint64(100)
	size := 1000
	numStrips := 4
	points := NewRandPoints(numDims, maxVal, size)
	for dim := 0; dim < numDims; dim++ {
		pam := PointArrayMem{
			points:  points,
			byDim:   dim,
			numDims: numDims,
		}
		splitValues, splitPoses := SplitPoints(&pam, numStrips)
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
			NewPointBase([]uint64{30, 80, 40}, 0),
			NewPointBase([]uint64{30, 80, 40}, 0),
			NewPointBase([]uint64{50, 90, 50}, 0),
			3,
			true,
		},
		{
			NewPointBase([]uint64{30, 79, 40}, 0),
			NewPointBase([]uint64{30, 80, 40}, 0),
			NewPointBase([]uint64{50, 90, 50}, 0),
			3,
			false,
		},
		{ //invalid range
			NewPointBase([]uint64{30, 80, 40}, 0),
			NewPointBase([]uint64{30, 80, 40}, 0),
			NewPointBase([]uint64{50, 90, 39}, 0),
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
