package bkdtree

import (
	"encoding/binary"
	"io"
	"math"
)

const (
	KdMetaSize int64 = 8 * 4
)

type KDTreeExtNodeInfo struct {
	offset    uint64 //offset in file. It's a leaf if less than idxBegin, otherwise an intra node.
	numPoints uint64 //number of points of subtree rooted at this node
}

type KdTreeExtIntraNode struct {
	splitDim    uint32
	numStrips   uint32
	splitValues []uint64
	children    []KdTreeExtNodeInfo
}

func (n *KdTreeExtIntraNode) Read(r io.Reader, order ByteOrder) (err error) {
	//According to https://golang.org/pkg/encoding/binary/#Read,
	//"Data must be a pointer to a fixed-size value or a slice of fixed-size values."
	//Slice shall be adjusted to the expected length before calling binary.Read().
	//However this is not needed for binary.Write().
	err = binary.Read(r, order, &n.splitDim)
	if err != nil {
		return
	}
	err = binary.Read(r, order, &n.numStrips)
	if err != nil {
		return
	}
	n.splitValues = make([]uint64, n.numStrips-1)
	err = binary.Read(r, order, &n.numStrips)
	if err != nil {
		return
	}
	n.children = make([]KdTreeExtNodeInfo, n.numStrips)
	err = binary.Read(r, order, &n.chldren)
	return
}

type KdTreeExtMeta struct {
	idxBegin  uint64
	numDims   uint32
	numPoints uint32 //the current number of points. Deleting points could trigger rebuilding the tree.
}

type KdTreeExt struct {
	number int
	KdTreeExtMeta
}

type BKDTree struct {
	bkdCap    int // N in the paper. len(trees) shall be no larger than math.log2(bkdCap/t0mCap)
	t0mCap    int // M in the paper, the capacity of in-memory buffer
	numDims   int
	dir       string //directory of files which hold the persisted kdtrees
	prefix    string //prefix of file names
	numPoints int64
	t0m       []Point     // T0M in the paper, in-memory buffer.
	trees     []KDTreeExt //persisted kdtrees
}

//NewBKDTree creates a BKDTree
func NewBKDTree(bkdCap, t0mCap, numDims int, dir, prefix string) (bkd *BKDTree) {
	treesCap := int(math.Log2(float64(bkdCap / t0mCap)))
	bkd = &BKDTree{
		bkdCap:  bkdCap,
		t0mCap:  t0mCap,
		numDims: numDims,
		dir:     dir,
		prefix:  prefix,
		t0m:     make([]Point, 0, t0mCap),
		trees:   make([]KDTreeExt, 0, treesCap),
	}
	for i := 0; i < treesCap; i++ {
		kd := KDTreeExt{
			number:    i,
			numPoints: 0,
		}
		bkd.trees = append(bkd.trees, kd)
	}
	return
}
