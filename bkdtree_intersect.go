package bkdtree

import (
	"bytes"
)

//Intersect does window query
func (bkd *BkdTree) Intersect(visitor IntersectVisitor) (err error) {
	bkd.intersectT0M(visitor)
	for i := 0; i < len(bkd.trees); i++ {
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
	if bkd.trees[idx].meta.numPoints <= 0 {
		return
	}
	//depth-first visiting from the root node
	meta := &bkd.trees[idx].meta
	err = bkd.intersectNode(visitor, bkd.trees[idx].data, meta, int(meta.rootOff))
	return
}

func (bkd *BkdTree) intersectNode(visitor IntersectVisitor, data []byte,
	meta *KdTreeExtMeta, nodeOffset int) (err error) {
	lowP := visitor.GetLowPoint()
	highP := visitor.GetHighPoint()
	var node KdTreeExtIntraNode
	bf := bytes.NewReader(data[nodeOffset:])
	err = node.Read(bf)
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
				data:        data[int(child.Offset):],
				numPoints:   int(child.NumPoints),
				byDim:       0, //not used
				bytesPerDim: bkd.bytesPerDim,
				numDims:     bkd.numDims,
				pointSize:   bkd.pointSize,
			}
			for i := 0; i < pae.numPoints; i++ {
				point := pae.GetPoint(i)
				if point.Inside(lowP, highP) {
					visitor.VisitPoint(point)
				}
			}
		} else {
			//intra node
			err = bkd.intersectNode(visitor, data, meta, int(child.Offset))
		}
		if err != nil {
			return
		}
	}
	return
}
