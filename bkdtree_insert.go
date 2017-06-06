package bkdtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

//Insert inserts given point. Fail if the tree is full.
func (bkd *BkdTree) Insert(point Point) (err error) {
	if bkd.numPoints >= bkd.bkdCap {
		return errors.New("BKDTree is full")
	}
	//insert into in-memory buffer t0m. If t0m is not full, return.
	bkd.t0m = append(bkd.t0m, point)
	if len(bkd.t0m) < bkd.t0mCap {
		bkd.numPoints++
		return
	}
	//find the smallest index k in [0, len(trees)) at which trees[k] is empty, or its capacity is no less than the sum of size of t0m + trees[0:k+1]
	sum := len(bkd.t0m)
	capK := bkd.t0mCap / 2
	k := 0
	for k = 0; k < len(bkd.trees); k++ {
		if bkd.trees[k].numPoints == 0 {
			break
		}
		sum += int(bkd.trees[k].numPoints)
		capK *= 2
		if capK >= sum {
			break
		}
	}
	if k >= cap(bkd.trees) {
		//impossible since bkd.numPoints has been checked
		return errors.New("BKDTree is full")
	}
	//extract all points from t0m and trees[0:k+1] into a file F
	tmpFp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d.tmp", bkd.prefix, k))
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, k))
	tmpF, err := os.OpenFile(tmpFp, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return
	}

	err = bkd.extractT0M(tmpF)
	if err != nil {
		return
	}
	for i := 0; i <= k; i++ {
		err = bkd.extractTi(tmpF, i)
		if err != nil {
			return
		}
	}
	meta, err := bkd.bulkLoad(tmpF)
	if err != nil {
		return
	}
	err = os.Rename(tmpFp, fp) //TODO: what happen if tmpF is open?
	if err != nil {
		return
	}
	bkd.trees[k] = *meta

	//empty T0M and Ti, 0<=i<k
	//TODO: remove these files?
	bkd.t0m = make([]Point, 0, bkd.t0mCap)
	for i := 0; i <= k; i++ {
		bkd.trees[i].numPoints = 0
		fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, i))
		err = os.Remove(fp)
		if err != nil {
			return
		}
	}
	bkd.numPoints++
	return
}

func (bkd *BkdTree) extractT0M(tmpF *os.File) (err error) {
	for _, point := range bkd.t0m {
		bytesP := Encode(point, bkd.numDims, bkd.bytesPerDim)
		_, err = tmpF.Write(bytesP)
		if err != nil {
			return
		}
	}
	return
}

func (bkd *BkdTree) extractTi(dstF *os.File, idx int) (err error) {
	if bkd.trees[idx].numPoints <= 0 {
		return
	}
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, idx))
	srcF, err := os.Open(fp)
	if err != nil {
		return
	}
	defer srcF.Close()

	srcF.Seek(-KdMetaSize, 2)
	var meta KdTreeExtMeta
	binary.Read(srcF, binary.BigEndian, &meta)
	if err != nil {
		return
	}
	//TODO: check if meta equals to bkd.trees[idx].Meta

	//depth-first extracting from the root node
	err = bkd.extractNode(dstF, srcF, &meta, -KdMetaSize-int64(BlockSize))
	return
}

func (bkd *BkdTree) extractNode(dstF, srcF *os.File, meta *KdTreeExtMeta, nodeOffset int64) (err error) {
	if nodeOffset < 0 {
		srcF.Seek(nodeOffset, 2)
	} else {
		dstF.Seek(nodeOffset, 0)
	}
	var node KdTreeExtIntraNode
	err = node.Read(srcF)
	if err != nil {
		return
	}
	pointSize := bkd.numDims*8 + 8
	for _, child := range node.children {
		if child.offset < meta.idxBegin {
			//leaf node
			//TODO: use Linux syscall.Splice() instead?
			_, err = srcF.Seek(int64(child.offset), 0)
			if err != nil {
				return
			}
			length := int64(child.numPoints) * int64(pointSize)
			_, err = io.CopyN(dstF, srcF, length)
			if err != nil {
				return
			}
		} else {
			//intra node
			err = bkd.extractNode(dstF, srcF, meta, int64(child.offset))
			if err != nil {
				return
			}
		}
	}
	return
}

func (bkd *BkdTree) bulkLoad(tmpF *os.File) (meta *KdTreeExtMeta, err error) {
	idxBegin, err := tmpF.Seek(0, 1) //get current position, where index begins
	if err != nil {
		return
	}
	leafCap := BlockSize / bkd.pointSize //how many points can be stored in one leaf node
	intraCap := (BlockSize - 8) / 24     //how many children can be stored in one intra node
	numPoints := int(idxBegin / int64(bkd.pointSize))
	bkd.createKDTreeExt(tmpF, 0, numPoints, 0, leafCap, intraCap)
	//record meta info at end: idxBegin, numDims, numPoints
	_, err = alignBlockSize(tmpF)
	meta = &KdTreeExtMeta{
		numDims:     uint32(bkd.numDims),
		bytesPerDim: uint32(bkd.bytesPerDim),
		idxBegin:    uint64(idxBegin),
		numPoints:   uint64(numPoints),
	}
	err = binary.Write(tmpF, binary.BigEndian, meta)
	if err != nil {
		return
	}
	err = tmpF.Close()
	return
}

func alignBlockSize(tmpF *os.File) (offset int64, err error) {
	curOff, err := tmpF.Seek(0, 1) //get current position
	if err != nil {
		return
	}
	offset = ((curOff + int64(BlockSize) - 1) / int64(BlockSize)) * int64(BlockSize)
	// fill with 0 till aligned to BlockSize
	for i := curOff; i < offset; i++ {
		err = binary.Write(tmpF, binary.BigEndian, byte(0))
		if err != nil {
			return
		}
	}
	return
}

func (bkd *BkdTree) createKDTreeExt(tmpF *os.File, begin, end int, depth, leafCap, intraCap int) (offset int64, err error) {
	if begin >= end {
		err = fmt.Errorf("assertion begin>=end failed, begin %v, end %v", begin, end)
		return
	}

	splitDim := depth % bkd.numDims
	numStrips := (int(end-begin) + leafCap - 1) / leafCap
	if numStrips > intraCap {
		numStrips = intraCap
	}

	pae := PointArrayExt{
		f:           tmpF,
		offBegin:    int64(begin * bkd.pointSize),
		numPoints:   end - begin,
		byDim:       splitDim,
		bytesPerDim: bkd.bytesPerDim,
		numDims:     bkd.numDims,
		pointSize:   bkd.bytesPerDim*bkd.numDims + 8,
	}
	splitValues, splitPoses := SplitPoints(&pae, numStrips)

	children := make([]KdTreeExtNodeInfo, 0, numStrips)
	var childOffset int64
	for strip := 0; strip < numStrips; strip++ {
		posBegin := begin
		if strip != 0 {
			posBegin = splitPoses[strip-1]
		}
		posEnd := end
		if strip != numStrips-1 {
			posEnd = splitPoses[strip]
		}
		if posEnd-posBegin <= leafCap {
			info := KdTreeExtNodeInfo{
				offset:    uint64(posBegin * bkd.pointSize),
				numPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		} else {
			childOffset, err = bkd.createKDTreeExt(tmpF, posBegin, posEnd, depth+1, leafCap, intraCap)
			if err != nil {
				return
			}
			info := KdTreeExtNodeInfo{
				offset:    uint64(childOffset),
				numPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		}
	}

	offset, err = alignBlockSize(tmpF)
	if err != nil {
		return
	}

	node := KdTreeExtIntraNode{
		splitDim:    uint32(splitDim),
		numStrips:   uint32(numStrips),
		splitValues: splitValues,
		children:    children,
	}
	err = binary.Write(tmpF, binary.BigEndian, node)
	return
}
