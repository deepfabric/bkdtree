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
		if point.Inside(lowP, highP) {
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
	meta := &bkd.trees[idx]
	err = bkd.intersectNode(visitor, f, meta, int64(meta.rootOff))
	return
}

func (bkd *BkdTree) intersectNode(visitor IntersectVisitor, f *os.File,
	meta *KdTreeExtMeta, nodeOffset int64) (err error) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	if nodeOffset < 0 {
		_, err = f.Seek(nodeOffset, 2)
	} else {
		_, err = f.Seek(nodeOffset, 0)
	}
	if err != nil {
		return
	}
	var node KdTreeExtIntraNode
	err = node.Read(f)
	if err != nil {
		return
	}
	for _, child := range node.Children {
		if child.NumPoints <= 0 {
			continue
		}
		if child.Offset < meta.pointsOffEnd {
			//leaf node
			pae := PointArrayExt{
				f:           f,
				offBegin:    int64(child.Offset),
				numPoints:   int(child.NumPoints),
				byDim:       0, //not used
				bytesPerDim: bkd.bytesPerDim,
				numDims:     bkd.numDims,
				pointSize:   bkd.pointSize,
			}
			//TODO: Convert pae to PointArrayMem?
			for i := 0; i < pae.numPoints; i++ {
				point, err1 := pae.GetPoint(i)
				if err1 != nil {
					err = err1
					return
				}
				if point.Inside(lowP, highP) {
					visitor.VisitPoint(point)
				}
			}
		} else {
			//intra node
			err = bkd.intersectNode(visitor, f, meta, int64(child.Offset))
		}
		if err != nil {
			return
		}
	}
	return
}
