package bkdtree

import (
	"github.com/keegancsmith/nth"
)

type Point interface {
	// Return the value X_{dim}, dim is started from 0
	GetValue(dim int) uint64
	GetUserData() uint64
}

type PointSorter struct {
	points []Point
	byDim  int
}

// Len is part of sort.Interface.
func (s *PointSorter) Len() int {
	return len(s.points)
}

// Swap is part of sort.Interface.
func (s *PointSorter) Swap(i, j int) {
	s.points[i], s.points[j] = s.points[j], s.points[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *PointSorter) Less(i, j int) bool {
	return s.points[i].GetValue(s.byDim) < s.points[j].GetValue(s.byDim)
}

// SplitPoints splits points per byDim
func SplitPoints(points []Point, byDim int, numStrips int) (splitValues []uint64, splitPoses []int) {
	if numStrips <= 1 {
		return
	}
	s := PointSorter{
		points: points,
		byDim:  byDim,
	}
	splitPos := len(points) / 2
	nth.Element(&s, splitPos)
	splitValue := points[splitPos].GetValue(byDim)

	numStrips1 := (numStrips + 1) / 2
	numStrips2 := numStrips - numStrips1
	splitValues1, splitPoses1 := SplitPoints(points[:splitPos], byDim, numStrips1)
	splitValues2, splitPoses2 := SplitPoints(points[splitPos:], byDim, numStrips2)
	for _, val := range splitValues1 {
		splitValues = append(splitValues, val)
	}
	for _, pos := range splitPoses1 {
		splitPoses = append(splitPoses, pos)
	}
	splitValues = append(splitValues, splitValue)
	splitPoses = append(splitPoses, splitPos)
	for _, val := range splitValues2 {
		splitValues = append(splitValues, val)
	}
	for _, pos := range splitPoses2 {
		splitPoses = append(splitPoses, pos+splitPos)
	}
	return
}

func IsInside(point, lowPoint, highPoint Point, numDims int) (isInside bool) {
	isInside = true
	for dim := 0; dim < numDims; dim++ {
		if point.GetValue(dim) < lowPoint.GetValue(dim) || point.GetValue(dim) > highPoint.GetValue(dim) {
			isInside = false
			break
		}
	}
	return
}
