package bkdtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

//Insert inserts given point. Fail if the tree is full.
func (bkd *BkdTree) Insert(point Point) (err error) {
	if bkd.NumPoints >= bkd.bkdCap {
		return errors.New("BKDTree is full")
	}
	//insert into in-memory buffer t0m. If t0m is not full, return.
	bkd.t0m = append(bkd.t0m, point)
	bkd.NumPoints++
	if len(bkd.t0m) < bkd.t0mCap {
		return
	}
	//find the smallest index k in [0, len(trees)) at which trees[k] is empty, or its capacity is no less than the sum of size of t0m + trees[0:k+1]
	sum := len(bkd.t0m)
	var k int
	for k = 0; k < len(bkd.trees); k++ {
		if bkd.trees[k].meta.NumPoints == 0 {
			break
		}
		sum += int(bkd.trees[k].meta.NumPoints)
		capK := bkd.t0mCap << uint(k)
		if capK >= sum {
			break
		}
	}
	if k == len(bkd.trees) {
		kd := BkdSubTree{
			meta: KdTreeExtMeta{
				PointsOffEnd: 0,
				RootOff:      0,
				NumPoints:    0,
				LeafCap:      uint16(bkd.leafCap),
				IntraCap:     uint16(bkd.intraCap),
				NumDims:      uint8(bkd.numDims),
				BytesPerDim:  uint8(bkd.bytesPerDim),
				PointSize:    uint8(bkd.pointSize),
				FormatVer:    0,
			},
		}
		bkd.trees = append(bkd.trees, kd)
	}
	//extract all points from t0m and trees[0:k+1] into a file F
	tmpFpK := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d.tmp", bkd.prefix, k))
	fpK := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, k))
	tmpFK, err := os.OpenFile(tmpFpK, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer tmpFK.Close()

	err = bkd.extractT0M(tmpFK)
	if err != nil {
		return
	}
	for i := 0; i <= k; i++ {
		err = bkd.extractTi(tmpFK, i)
		if err != nil {
			return
		}
	}
	meta, err := bkd.bulkLoad(tmpFK)
	if err != nil {
		return
	}

	//empty T0M and Ti, 0<=i<k
	bkd.t0m = make([]Point, 0, bkd.t0mCap)
	for i := 0; i <= k; i++ {
		if bkd.trees[i].meta.NumPoints <= 0 {
			continue
		} else if err = munmapFile(bkd.trees[i].data); err != nil {
			return
		} else if err = bkd.trees[i].f.Close(); err != nil {
			err = errors.Wrap(err, "")
			return
		} else if err = os.Remove(bkd.trees[i].f.Name()); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		bkd.trees[i].meta.NumPoints = 0
	}
	if err = os.Rename(tmpFpK, fpK); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fK, err := os.OpenFile(fpK, os.O_RDWR, 0600)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	data, err := mmapFile(fK)
	if err != nil {
		return
	}
	bkd.trees[k] = BkdSubTree{
		meta: *meta,
		f:    fK,
		data: data,
	}
	return
}

func (bkd *BkdTree) extractT0M(tmpF *os.File) (err error) {
	b := make([]byte, bkd.pointSize)
	for _, point := range bkd.t0m {
		point.Encode(b, bkd.bytesPerDim)
		_, err = tmpF.Write(b)
		if err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	return
}

func (bkd *BkdTree) extractTi(dstF *os.File, idx int) (err error) {
	if bkd.trees[idx].meta.NumPoints <= 0 {
		return
	}
	fp := filepath.Join(bkd.dir, fmt.Sprintf("%s_%d", bkd.prefix, idx))
	srcF, err := os.Open(fp)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	defer srcF.Close()

	//depth-first extracting from the root node
	meta := &bkd.trees[idx].meta
	err = bkd.extractNode(dstF, bkd.trees[idx].data, meta, int(meta.RootOff))
	return
}

func (bkd *BkdTree) extractNode(dstF *os.File, data []byte, meta *KdTreeExtMeta, nodeOffset int) (err error) {
	var node KdTreeExtIntraNode
	bf := bytes.NewReader(data[nodeOffset:])
	err = node.Read(bf)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	for _, child := range node.Children {
		if child.Offset < meta.PointsOffEnd {
			//leaf node
			length := int(child.NumPoints) * int(meta.PointSize)
			_, err = dstF.Write(data[int(child.Offset) : int(child.Offset)+length])
			if err != nil {
				err = errors.Wrap(err, "")
				return
			}
		} else {
			//intra node
			err = bkd.extractNode(dstF, data, meta, int(child.Offset))
			if err != nil {
				return
			}
		}
	}
	return
}

func (bkd *BkdTree) bulkLoad(tmpF *os.File) (meta *KdTreeExtMeta, err error) {
	pointsOffEnd, err := tmpF.Seek(0, 1) //get current position
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	var data []byte
	if data, err = mmapFile(tmpF); err != nil {
		return
	}
	defer munmapFile(data)

	numPoints := int(pointsOffEnd / int64(bkd.pointSize))
	rootOff, err1 := bkd.createKdTreeExt(tmpF, data, 0, numPoints, 0)
	if err1 != nil {
		err = err1
		return
	}
	//record meta info at end
	meta = &KdTreeExtMeta{
		PointsOffEnd: uint64(pointsOffEnd),
		RootOff:      uint64(rootOff),
		NumPoints:    uint64(numPoints),
		LeafCap:      uint16(bkd.leafCap),
		IntraCap:     uint16(bkd.intraCap),
		NumDims:      uint8(bkd.numDims),
		BytesPerDim:  uint8(bkd.bytesPerDim),
		PointSize:    uint8(bkd.pointSize),
		FormatVer:    0,
	}
	err = binary.Write(tmpF, binary.BigEndian, meta)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func getCurrentOffset(f *os.File) (offset int64, err error) {
	offset, err = f.Seek(0, 1) //get current position
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (bkd *BkdTree) createKdTreeExt(tmpF *os.File, data []byte, begin, end, depth int) (offset int64, err error) {
	if begin >= end {
		err = errors.New(fmt.Sprintf("assertion begin>=end failed, begin %v, end %v", begin, end))
		return
	}

	splitDim := depth % bkd.numDims
	numStrips := (end - begin + bkd.leafCap - 1) / bkd.leafCap
	if numStrips > bkd.intraCap {
		numStrips = bkd.intraCap
	}

	pae := PointArrayExt{
		data:        data[begin*bkd.pointSize:],
		numPoints:   end - begin,
		byDim:       splitDim,
		bytesPerDim: bkd.bytesPerDim,
		numDims:     bkd.numDims,
		pointSize:   bkd.pointSize,
	}
	splitValues, splitPoses := SplitPoints(&pae, numStrips)

	children := make([]KdTreeExtNodeInfo, 0, numStrips)
	var childOffset int64
	for strip := 0; strip < numStrips; strip++ {
		posBegin := begin
		if strip != 0 {
			posBegin = begin + splitPoses[strip-1]
		}
		posEnd := end
		if strip != numStrips-1 {
			posEnd = begin + splitPoses[strip]
		}
		if posEnd-posBegin <= bkd.leafCap {
			info := KdTreeExtNodeInfo{
				Offset:    uint64(posBegin * bkd.pointSize),
				NumPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		} else {
			childOffset, err = bkd.createKdTreeExt(tmpF, data, posBegin, posEnd, depth+1)
			if err != nil {
				return
			}
			info := KdTreeExtNodeInfo{
				Offset:    uint64(childOffset),
				NumPoints: uint64(posEnd - posBegin),
			}
			children = append(children, info)
		}
	}

	offset, err = getCurrentOffset(tmpF)
	if err != nil {
		return
	}

	node := &KdTreeExtIntraNode{
		SplitDim:    uint32(splitDim),
		NumStrips:   uint32(numStrips),
		SplitValues: splitValues,
		Children:    children,
	}
	err = node.Write(tmpF)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}
