package bkdtree

import (
	"math/rand"
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
	insert(point Point, numDims int)
	erase(point Point, numDims int) bool
}

type KdTreeIntraNode struct {
	splitDim    int
	splitValues []uint64
	children    []KdTreeNode
}

type KdTreeLeafNode struct {
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

func (d *IntersectCollector) GetLowPoint() Point     { return d.lowPoint }
func (d *IntersectCollector) GetHighPoint() Point    { return d.highPoint }
func (d *IntersectCollector) VisitPoint(point Point) { d.points = append(d.points, point) }

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
		ret := &KdTreeLeafNode{
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

	children := make([]KdTreeNode, 0, numStrips)
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
	ret := &KdTreeIntraNode{
		splitDim:    splitDim,
		splitValues: splitValues,
		children:    children,
	}
	return ret
}

func (n *KdTreeIntraNode) visit(visitor IntersectVisitor, numDims int) {
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

func (n *KdTreeLeafNode) visit(visitor IntersectVisitor, numDims int) {
	lowPoint := visitor.GetLowPoint()
	highPoint := visitor.GetHighPoint()
	for _, point := range n.points {
		isInside := IsInside(point, lowPoint, highPoint, numDims)
		if isInside {
			visitor.VisitPoint(point)
		}
	}
}

func (t *KDTree) Intersect(visitor IntersectVisitor) {
	t.root.visit(visitor, t.NumDims)
}

func (n *KdTreeIntraNode) insert(point Point, numDims int) {
	lowVal := point.GetValue(n.splitDim)
	highVal := lowVal
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	//if multiple strips could cover the point, select one randomly.
	strip := begin + rand.Intn(end-begin)
	n.children[strip].insert(point, numDims)
}

func (n *KdTreeLeafNode) insert(point Point, numDims int) {
	//append blindly, no rebalance
	n.points = append(n.points, point)
}

func (t *KDTree) Insert(point Point) {
	t.root.insert(point, t.NumDims)
}

func (n *KdTreeIntraNode) erase(point Point, numDims int) (found bool) {
	lowVal := point.GetValue(n.splitDim)
	highVal := lowVal
	numSplits := len(n.splitValues)
	//calculate children[begin:end) need to visit
	end := sort.Search(numSplits, func(i int) bool { return n.splitValues[i] > highVal })
	begin := sort.Search(end, func(i int) bool { return n.splitValues[i] >= lowVal })
	end++
	//if multiple strips could cover the point, iterate them. And stop iteration if found at middle way.
	for strip := begin; strip < end; strip++ {
		found = n.children[strip].erase(point, numDims)
		if found {
			break
		}
	}
	return
}

func (n *KdTreeLeafNode) erase(point Point, numDims int) (found bool) {
	found = false
	idx := len(n.points)
	for i, point2 := range n.points {
		//assumes each point's userData is unique
		if Equals(point, point2, numDims) && point.GetUserData() == point2.GetUserData() {
			idx = i
			break
		}
	}
	if idx < len(n.points) {
		n.points = append(n.points[:idx], n.points[idx+1:]...)
		found = true
	}
	return
}

func (t *KDTree) Erase(point Point) {
	t.root.erase(point, t.NumDims)
}
