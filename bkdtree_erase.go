package bkdtree

import (
	"bytes"
	"encoding/binary"
)

//Erase erases given point.
func (bkd *BkdTree) Erase(point Point) (found bool, err error) {
	//Query T0M with p; if found, delete it and return.
	pam := PointArrayMem{
		points: bkd.t0m,
		byDim:  0,
	}
	found = pam.Erase(point)
	if found {
		bkd.NumPoints--
		return
	}

	//Query each non-empty tree in the forest with p; if found, delete it and return
	for i := 0; i < len(bkd.trees); i++ {
		found, err = bkd.eraseTi(point, i)
		if err != nil {
			return
		} else if found {
			bkd.NumPoints--
			return
		}
	}
	return
}

func (bkd *BkdTree) eraseTi(point Point, idx int) (found bool, err error) {
	if bkd.trees[idx].meta.numPoints <= 0 {
		return
	}

	//depth-first erasing from the root node
	meta := &bkd.trees[idx].meta
	found, err = bkd.eraseNode(point, bkd.trees[idx].data, meta, int(meta.rootOff))
	if err != nil {
		return
	}
	if found {
		bkd.trees[idx].meta.numPoints--
		bf := bytes.NewBuffer(bkd.trees[idx].data[len(bkd.trees[idx].data)-KdTreeExtMetaSize:])
		err = binary.Write(bf, binary.BigEndian, meta)
		return
	}
	return
}

func (bkd *BkdTree) eraseNode(point Point, data []byte, meta *KdTreeExtMeta, nodeOffset int) (found bool, err error) {
	var node KdTreeExtIntraNode
	bf := bytes.NewBuffer(data[nodeOffset:])
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
			found = pae.Erase(point)
		} else {
			//intra node
			found, err = bkd.eraseNode(point, data, meta, int(child.Offset))
		}
		if err != nil {
			return
		}
		if found {
			child.NumPoints--
			break
		}
	}
	if found {
		err = node.Write(bf)
	}
	return
}
