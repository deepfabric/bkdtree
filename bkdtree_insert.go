package bkdtree

import (
	"errors"
	"fmt"
	"math"
	"os"
	"syscall"
	"path/filepath"
	"encoding/binary"
	"log"
)

//Insert inserts given point. Fail if the tree is full.
func (bkd *BKDTree) Insert(point Point) (err error) {
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
		sum += bkd.trees[k].numPoints
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
	if err != nil {	return }

	err = bkd.extractT0M(tmpF)
	if err != nil {	return }
	for i = 0; i <= k; i++ {
		err = bkd.extractTi(tmpF, i)
		if err != nil {	return }
	}
	err = bkd.bulkLoad(tmpF, numDims)
	if err != nil {	return }
	err = os.Rename(tmpFp, fp) //TODO: what happen if tmpF is open?
	if err != nil {	return }
	bkd.trees[k] = KDTreeExt {
		number: k,
		numPoints: sum
	}
	//empty T0M and Ti, 0<=i<k
	bkd.t0m = make([]Point, 0, bkd.t0mCap)
	for i = 0; i <= k; i++ {
		bkd.trees[i] = KDTreeExt {
			number: i,
			numPoints: 0
		}
	}
	bkd.numPoints++
	return
}

func(bkd *BKDTree) extraT0M(tmpF *os.File) (err error) {
	for _, point := range bkd.t0m {
		for dim :=0; dim<bkd.numDims; dim++ {
			val := point.GetValue(dim)
			err = binary.Write(tmpF, binary.BigEndian, val)
			if err != nil {	return }
		}
		userData := point.GetUserData()
		err = binary.Write(tmpF, binary.BigEndian, userData)
		if err != nil {	return }
	}
}

func (bkd *BKDTree) extraTi(dstF *os.File, idx int) (err error) {
	if bkd.trees[idx].numPoints <= 0 {
		return
	}
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, idx))
	srcF, err := os.Open(fp)
	if err != nil {	return }
	defer f.Close()

	srcF.Seek(-KdMetaSize, 2)
	var meta KdTreeExtMeta
	binary.Read(srcF, binary.BigEndian, &meta)
	if err != nil {	return }
	//TODO: check if meta equals to bkd.trees[idx].Meta

	//depth-first extracting from the root node
	err = bkd.extractNode(dstF, srcF, &meta, -KdMetaSize-BlockSize)
	return
}

func (bkd *BKDTree) extractNode(dstF, srcF *os.File, meta *KdTreeExtMeta, nodeOffset int64) (err error){
	if nodeOffset < 0 {
		srcF.Seek(nodeOffset, 2)
	} else {
		dstF.Seek(nodeOffset, 0)
	}
	var node KdTreeExtIntraNode
	err = node.Read(srcF, binary.BigEndian)
	if err != nil {	return }
	pointSize := bkd.numDims*8 + 8
	for _, child := range node.children {
		if child.offset < meta.idxBegin {
			//leaf node
			roff := int64(child.offset)
			woff := dstF.Seek(0, 1) //get current position
			lengh := info.numPoints * pointSize
			n, err = syscall.Splice(srcF.Fd, &roff, tmp.Fd, &woff, length, 0)
			if err != nil {	return }
		} else {
			//intra node
			err = bkd.extractNode(dstF, srcF, meta, child.offset)
			if err != nil {	return }
		}
	}
}

func (bkd *BKDTree)	bulkLoad(tmpF *os.File) (err error) {
	idxBegin := tmpF.Seek(0, 1) //get current position, where index begins
	pointSize := bkd.numDims*8 + 8
	leafCap := BlockSize / pointSize //how many points can be stored in one leaf node
	intraCap := (BlockSize - 8) / 24 //how many children can be stored in one intra node
	numPoints := woff / pointSize
	createKDTreeExt(tmpF, 0, numPoints, 0, bkd.numDims, leafCap, intraCap)
	//record meta info at end: idxBegin, numDims, numPoints
	_, err = alignBlockSize(tmpF)
	meta := KdTreeExtMeta {
		idxBegin: idxBegin,
		numDims: numDims,
		numPoints: numPoints,
	}
	err = binary.Write(tmpF, binary.BigEndian, &meta)
	if err != nil {	return }
	err = tmpF.close()
	if err != nil {	return }
}

func alignBlockSize(tmpF *os.File) (offset int64, err error) {
	curOff = tmpF.Seek(0, 1) //get current position
	offset = ((curOff + BlockSize - 1) / BlockSize) * BlockSize //align to BlockSize
	for i:=curOff; i<offset; i++ {
		err = binary.Write(tmpF, binary.BigEndian, byte(0))
		if err != nil {	return }
	}
	return
}

func createKDTreeExt(tmpF *os.File, begin, end int64, depth, numDims, leafCap, intraCap int) (offset int64, err error) {
	if begin >= end {
		log.Fatalf("assertion begin>=end failed, begin %v, end %v\n", begin, end)
		return
	}

	splitDim := depth % numDims
	numStrips := (end - begin + leafCap - 1) / leafCap
	if numStrips > intraCap {
		numStrips = intraCap
	}

	pointSize := numDims*8 + 8
	splitValues, splitPoses := SplitPoints(tmpF, begin, end, numDims, splitDim, numStrips)

	children := make([]KDTreeExtNodeInfo, 0, numStrips)
	for strip := 0; strip < numStrips; strip++ {
		posBegin := begin
		if strip != 0 {
			posBegin = splitPoses[strip-1]
		}
		posEnd := end
		if strip != numStrips-1 {
			posEnd = splitPoses[strip]
		}
		if posEnd - posBegin <= leafCap {
			info := KDTreeExtNodeInfo{
				offset: posBegin * pointSize,
				numPoints: posEnd - posBegin,
			}
			children = append(children, info)
		} else {
			childOffset := createKDTree(tmpF, posBegin, posEnd, depth+1, numDims, leafCap, intraCap)
			info := KDTreeExtNodeInfo{
				offset: childOffset,
				numPoints: posEnd - posBegin,
			}
			children = append(children, info)
		}
	}

	offset, err = alignBlockSize(tmpF)
	if err != nil {	return }

	node := KdTreeExtIntraNode {
		splitDim: uint32(splitDim),
		numStrips: uint32(numStrips),
		splitValues: splitValues,
		children: children,
	}
	err = binary.Write(tmpF, binary.BigEndian, node)
	return
}
