package bkdtree

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

//Erase erases given point.
func (bkd *BkdTree) Erase(point Point) (found bool, err error) {
	//Query T0M with p; if found, delete it and return.
	pam := PointArrayMem{
		points:  bkd.t0m,
		byDim:   0,
		numDims: bkd.numDims,
	}
	found, err = pam.Erase(point)
	if err != nil {
		return
	}
	if found {
		bkd.numPoints--
		return
	}

	//Query each non-empty tree in the forest with p; if found, delete it and return
	for i := 0; i < len(bkd.trees); i++ {
		found, err = bkd.eraseTi(point, i)
		if err != nil {
			return
		}
		if found {
			bkd.numPoints--
			return
		}
	}
	return
}

func (bkd *BkdTree) eraseTi(point Point, idx int) (found bool, err error) {
	if bkd.trees[idx].numPoints <= 0 {
		return
	}
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, idx))
	f, err := os.Open(fp)
	if err != nil {
		return
	}
	defer f.Close()

	f.Seek(-KdTreeExtMetaSize, 2)
	var meta KdTreeExtMeta
	binary.Read(f, binary.BigEndian, &meta)
	if err != nil {
		return
	}
	//TODO: check if meta equals to bkd.trees[idx]

	//depth-first erasing from the root node
	found, err = bkd.eraseNode(point, f, &meta, -KdTreeExtMetaSize-int64(BlockSize))
	if err != nil {
		return
	}
	if found {
		meta.numPoints--
		f.Seek(-KdTreeExtMetaSize, 2)
		binary.Write(f, binary.BigEndian, &meta)
		bkd.trees[idx].numPoints--
		return
	}
	return
}

func (bkd *BkdTree) eraseNode(point Point, f *os.File, meta *KdTreeExtMeta, nodeOffset int64) (found bool, err error) {
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
	idx := 0
	for i, child := range node.children {
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
			found, err = pae.Erase(point)
		} else {
			//intra node
			found, err = bkd.eraseNode(point, f, meta, int64(child.offset))
		}
		if err != nil {
			return
		}
		if found {
			idx = i
			child.numPoints--
			break
		}
	}
	if found {
		_, err = f.Seek(-int64(int(node.numStrips)-idx)*KdTreeExtNodeInfoSize, 1)
		if err != nil {
			return
		}
		err = binary.Write(f, binary.BigEndian, node.children[idx])
	}
	return
}
