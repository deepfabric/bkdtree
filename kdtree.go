package bkdtree

import (
	"sort"
)

const (
	MaxDims       int = 8
	PagenumSplits int = 4096
)

type U64Slice []uint64

func (a U64Slice) Len() int           { return len(a) }
func (a U64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a U64Slice) Less(i, j int) bool { return a[i] < a[j] }

type KdTreeNode interface {
	visit(visitor IntersectVisitor, numDims int)
}

type KdTreeIntraNode struct {
	KdTreeNode
	splitDim    int
	splitValues []uint64
	children    []KdTreeNode
}

type KdTreeLeafNode struct {
	KdTreeNode
	points []Point
}

type IntersectVisitor interface {
	GetLowPoint() Point
	GetHighPoint() Point
	VisitPoint(point Point)
}

type IntersectCollector struct {
	lowPoint  Point
	highPoint Point
	points    []Point
}

func (d IntersectCollector) GetLowPoint() Point     { return d.lowPoint }
func (d IntersectCollector) GetHighPoint() Point    { return d.highPoint }
func (d IntersectCollector) VisitPoint(point Point) { d.points = append(d.points, point) }

type KDTree struct {
	root    KdTreeNode
	NumDims int
}

func NewKDTree(points []Point, numDims int) *KDTree {
	if len(points) == 0 || numDims <= 0 || numDims > MaxDims {
		return nil
	}
	pointnumSplits := numDims * 8
	leafCap := PagenumSplits / pointnumSplits //how many points can be stored in one leaf node
	intraCap := (PagenumSplits - 8) / 16      //how many children can be stored in one intra node

	ret := &KDTree{
		NumDims: numDims,
		root:    createKDTree(points, 0, numDims, leafCap, intraCap),
	}
	return ret
}

func createKDTree(points []Point, depth int, numDims int, leafCap int, intraCap int) KdTreeNode {
	if len(points) == 0 {
		return nil
	}
	if len(points) <= leafCap {
		pointsCopy := make([]Point, len(points))
		copy(pointsCopy, points)
		ret := KdTreeLeafNode{
			points: pointsCopy,
		}
		return ret
	}

	splitDim := depth % numDims
	numStrips := (len(points) + leafCap - 1) / leafCap
	if numStrips > intraCap {
		numStrips = intraCap
	}

	splitValues, splitPoses := SplitPoints(points, splitDim, numStrips)

	children := make([]KdTreeNode, 0)
	for strip := 0; strip < numStrips; strip++ {
		posBegin := 0
		if strip != 0 {
			posBegin = splitPoses[strip-1]
		}
		posEnd := len(points)
		if strip != numStrips-1 {
			posEnd = splitPoses[strip]
		}
		child := createKDTree(points[posBegin:posEnd], depth+1, numDims, leafCap, intraCap)
		children = append(children, child)
	}
	ret := KdTreeIntraNode{
		splitDim:    splitDim,
		splitValues: splitValues,
		children:    children,
	}
	return ret
}

func (n KdTreeIntraNode) visit(visitor IntersectVisitor, numDims int) {
	lowVal := visitor.GetLowPoint().GetValue(n.splitDim)
	highVal := visitor.GetHighPoint().GetValue(n.splitDim)
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	for strip := begin; strip < end; strip++ {
		n.children[strip].visit(visitor, numDims)
	}
}

func (n KdTreeLeafNode) visit(visitor IntersectVisitor, numDims int) {
	lowPoint := visitor.GetLowPoint()
	highPoint := visitor.GetHighPoint()
	for _, point := range n.points {
		isMatch := true
		for dim := 0; dim < numDims; dim++ {
			if point.GetValue(dim) < lowPoint.GetValue(dim) || point.GetValue(dim) > highPoint.GetValue(dim) {
				isMatch = false
				break
			}
		}
		if isMatch {
			visitor.VisitPoint(point)
		}
	}
}

func (t *KDTree) Intersect(visitor IntersectVisitor) {
	t.root.visit(visitor, t.NumDims)
}
