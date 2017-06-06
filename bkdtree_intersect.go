package bkdtree

import (
	"fmt"
	"os"
	"path/filepath"
)

//Intersect does window query
func (bkd *BkdTree) Intersect(visitor IntersectVisitor) (err error) {
	bkd.intersectT0M(visitor)
	for i := 0; i <= len(bkd.trees); i++ {
		err = bkd.intersectTi(visitor, i)
		if err != nil {
			return
		}
	}
	return
}

func (bkd *BkdTree) intersectT0M(visitor IntersectVisitor) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	for _, point := range bkd.t0m {
		if IsInside(point, lowP, highP, bkd.numDims) {
			visitor.VisitPoint(point)
		}
	}
	return
}

func (bkd *BkdTree) intersectTi(visitor IntersectVisitor, idx int) (err error) {
	if bkd.trees[idx].numPoints <= 0 {
		return
	}
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, idx))
	f, err := os.Open(fp)
	if err != nil {
		return
	}
	defer f.Close()

	//depth-first visiting from the root node
	err = bkd.intersectNode(visitor, f, &bkd.trees[idx], -KdTreeExtMetaSize-int64(BlockSize))
	return
}

func (bkd *BkdTree) intersectNode(visitor IntersectVisitor, f *os.File,
	meta *KdTreeExtMeta, nodeOffset int64) (err error) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	if nodeOffset < 0 {
		f.Seek(nodeOffset, 2)
	} else {
		f.Seek(nodeOffset, 0)
	}
	var node KdTreeExtIntraNode
	err = node.Read(f)
	if err != nil {
		return
	}
	for _, child := range node.children {
		if child.numPoints <= 0 {
			continue
		}
		if child.offset < meta.idxBegin {
			//leaf node
			pae := PointArrayExt{
				f:           f,
				offBegin:    int64(child.offset),
				numPoints:   int(child.numPoints),
				byDim:       0, //not used
				bytesPerDim: bkd.bytesPerDim,
				numDims:     bkd.numDims,
				pointSize:   bkd.pointSize,
			}
			//TODO: Convert pae to PointArrayMem?
			for i := 0; i < pae.numPoints; i++ {
				point := pae.GetPoint(i)
				if IsInside(point, lowP, highP, bkd.numDims) {
					visitor.VisitPoint(point)
				}
			}
		} else {
			//intra node
			err = bkd.intersectNode(visitor, f, meta, int64(child.offset))
		}
		if err != nil {
			return
		}
	}
	return
}
