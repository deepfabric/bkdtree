package bkdtree

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	"fmt"

	"github.com/keegancsmith/nth"
)

type Point struct {
	Vals     []uint64
	UserData uint64
}

type PointArray interface {
	sort.Interface
	GetPoint(idx int) (Point, error)
	GetValue(idx int) (uint64, error)
	SubArray(begin, end int) PointArray
	Erase(point Point) (bool, error)
}

type PointArrayMem struct {
	points []Point
	byDim  int
}

type PointArrayExt struct {
	f           *os.File
	offBegin    int64
	numPoints   int
	byDim       int
	bytesPerDim int
	numDims     int
	pointSize   int
}

func (p *Point) Inside(lowPoint, highPoint Point) (isInside bool) {
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] < lowPoint.Vals[dim] || p.Vals[dim] > highPoint.Vals[dim] {
			return
		}
	}
	isInside = true
	return
}

func (p *Point) Equal(rhs Point) (isEqual bool) {
	if p.UserData != rhs.UserData || len(p.Vals) != len(rhs.Vals) {
		return
	}
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] != rhs.Vals[dim] {
			return
		}
	}
	return
}

//TODO: optimize point encoding/decoding
func (p *Point) Encode(bytesPerDim int) (res []byte) {
	numDims := len(p.Vals)
	pointSize := bytesPerDim*numDims + 8
	res = make([]byte, pointSize)
	buf := new(bytes.Buffer)
	idx := 0
	for dim := 0; dim < numDims; dim++ {
		val := p.Vals[dim]
		err := binary.Write(buf, binary.BigEndian, val)
		if err != nil {
			return
		}
		bs := buf.Bytes()
		for i := 8 - bytesPerDim; i < 8; i++ {
			res[idx] = bs[i]
			idx++
		}
		buf.Reset()
	}
	err := binary.Write(buf, binary.BigEndian, p.UserData)
	if err != nil {
		return
	}
	bs := buf.Bytes()
	for i := 0; i < 8; i++ {
		res[idx] = bs[i]
		idx++
	}
	return
}

func (p *Point) Decode(bytesP []byte, numDims int, bytesPerDim int) (err error) {
	p.Vals = make([]uint64, numDims)
	pointSize := bytesPerDim*numDims + 8
	if len(bytesP) < pointSize {
		err = fmt.Errorf("byte array is too short to decode")
		return
	}
	for dim := 0; dim < numDims; dim++ {
		var val uint64
		for i := 0; i < bytesPerDim; i++ {
			val *= 8
			val += uint64(bytesP[dim*bytesPerDim+i])
		}
		p.Vals[dim] = val
	}
	var userData uint64
	for i := numDims * bytesPerDim; i < pointSize; i++ {
		userData *= 8
		userData += uint64(bytesP[i])
	}
	p.UserData = userData
	return
}

// Len is part of sort.Interface.
func (s *PointArrayMem) Len() int {
	return len(s.points)
}

// Swap is part of sort.Interface.
func (s *PointArrayMem) Swap(i, j int) {
	s.points[i], s.points[j] = s.points[j], s.points[i]
}

// Less is part of sort.Interface.
func (s *PointArrayMem) Less(i, j int) bool {
	return s.points[i].Vals[s.byDim] < s.points[j].Vals[s.byDim]
}

func (s *PointArrayMem) GetPoint(idx int) (point Point, err error) {
	point = s.points[idx]
	return
}

func (s *PointArrayMem) GetValue(idx int) (val uint64, err error) {
	val = s.points[idx].Vals[s.byDim]
	return
}

func (s *PointArrayMem) SubArray(begin, end int) (sub PointArray) {
	sub = &PointArrayMem{
		points: s.points[begin:end],
		byDim:  s.byDim,
	}
	return
}

func (s *PointArrayMem) Erase(point Point) (found bool, err error) {
	idx := 0
	for i, point2 := range s.points {
		//assumes each point's userData is unique
		if point.Equal(point2) {
			idx = i
			found = true
			break
		}
	}
	if found {
		s.points = append(s.points[:idx], s.points[idx+1:]...)
	}
	return
}

func (s *PointArrayMem) ToExt(f *os.File, bytesPerDim int) (pae *PointArrayExt, err error) {
	if len(s.points) == 0 {
		err = fmt.Errorf("cannot determine numDims for empty PointArrayMem")
		return
	}
	numDims := len(s.points[0].Vals)
	pointSize := numDims*bytesPerDim + 8
	offBegin, err1 := f.Seek(0, 1)
	if err1 != nil {
		err = err1
		return
	}
	pae = &PointArrayExt{
		f:           f,
		offBegin:    offBegin,
		numPoints:   len(s.points),
		byDim:       s.byDim,
		numDims:     numDims,
		bytesPerDim: bytesPerDim,
		pointSize:   pointSize,
	}
	for _, point := range s.points {
		bytesP := point.Encode(bytesPerDim)
		_, err = f.Write(bytesP)
		if err != nil {
			return
		}
	}
	return
}

// Len is part of sort.Interface.
func (s *PointArrayExt) Len() int {
	return s.numPoints
}

// Swap is part of sort.Interface.
func (s *PointArrayExt) Swap(i, j int) {
	//TODO: optimize via mmap?
	pi := make([]byte, s.pointSize)
	pj := make([]byte, s.pointSize)
	offI := s.offBegin + int64(i*s.pointSize)
	offJ := s.offBegin + int64(j*s.pointSize)
	if _, err := s.f.ReadAt(pi, offI); err != nil {
		panic(err)
	}
	if _, err := s.f.ReadAt(pj, offJ); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(pi, offJ); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(pj, offI); err != nil {
		panic(err)
	}
}

// Less is part of sort.Interface.
func (s *PointArrayExt) Less(i, j int) bool {
	pi := make([]byte, s.pointSize)
	pj := make([]byte, s.pointSize)
	offI := s.offBegin + int64(i*s.pointSize)
	offJ := s.offBegin + int64(j*s.pointSize)
	if _, err := s.f.ReadAt(pi, offI); err != nil {
		panic(err)
	}
	if _, err := s.f.ReadAt(pj, offJ); err != nil {
		panic(err)
	}
	for idx := s.byDim * s.bytesPerDim; idx < (s.byDim+1)*s.bytesPerDim; idx++ {
		if pi[idx] > pj[idx] {
			return false
		}
	}
	return true
}

func (s *PointArrayExt) GetPoint(idx int) (point Point, err error) {
	pi := make([]byte, s.pointSize)
	offI := s.offBegin + int64(idx*s.pointSize)
	if _, err = s.f.ReadAt(pi, offI); err != nil {
		return
	}
	if err = point.Decode(pi, s.numDims, s.bytesPerDim); err != nil {
		return
	}
	return
}

func (s *PointArrayExt) GetValue(idx int) (val uint64, err error) {
	pi := make([]byte, s.pointSize)
	offI := s.offBegin + int64(idx*s.pointSize)
	if _, err = s.f.ReadAt(pi, offI); err != nil {
		return
	}
	val = 0
	for i := s.bytesPerDim * s.byDim; i < s.bytesPerDim*(s.byDim+1); i++ {
		val = val*8 + uint64(pi[i])
	}
	return
}

func (s *PointArrayExt) SubArray(begin, end int) (sub PointArray) {
	sub = &PointArrayExt{
		f:           s.f,
		offBegin:    s.offBegin + int64(begin*s.pointSize),
		numPoints:   end - begin,
		byDim:       s.byDim,
		bytesPerDim: s.bytesPerDim,
		numDims:     s.numDims,
		pointSize:   s.pointSize,
	}
	return
}

func (s *PointArrayExt) Erase(point Point) (found bool, err error) {
	bytesP := point.Encode(s.bytesPerDim)
	var off int64
	for off = s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		pi := make([]byte, s.pointSize)
		_, err = s.f.ReadAt(pi, off)
		if err != nil {
			return
		}
		found = bytes.Equal(bytesP, pi)
		if found {
			break
		}
	}
	if found {
		//replace the matched point with the last point and decrease the array length
		idxLast := s.numPoints - 1
		pLast := make([]byte, s.pointSize)
		offLast := s.offBegin + int64(idxLast*s.pointSize)
		_, err = s.f.ReadAt(pLast, offLast) //TODO: handle error?
		if err != nil {
			return
		}
		_, err = s.f.WriteAt(pLast, off)
		if err != nil {
			return
		}
		s.numPoints--
	}
	return
}

func (s *PointArrayExt) ToMem() (pam *PointArrayMem, err error) {
	points := make([]Point, 0, s.numPoints)
	var p Point
	for off := s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		pi := make([]byte, s.pointSize)
		_, err = s.f.ReadAt(pi, off) //TODO: handle error?
		if err != nil {
			return
		}
		err = p.Decode(pi, s.numDims, s.bytesPerDim)
		if err != nil {
			return
		}
		points = append(points, p)
	}
	pam = &PointArrayMem{
		points: points,
		byDim:  s.byDim,
	}
	return
}

// SplitPoints splits points per byDim
func SplitPoints(points PointArray, numStrips int) (splitValues []uint64, splitPoses []int) {
	if numStrips <= 1 {
		return
	}
	splitPos := points.Len() / 2
	nth.Element(points, splitPos)
	splitValue, err := points.GetValue(splitPos)
	if err != nil {
		panic("points.GetValue error")
	}

	numStrips1 := (numStrips + 1) / 2
	numStrips2 := numStrips - numStrips1
	splitValues1, splitPoses1 := SplitPoints(points.SubArray(0, splitPos), numStrips1)
	splitValues = append(splitValues, splitValues1...)
	splitPoses = append(splitPoses, splitPoses1...)
	splitValues = append(splitValues, splitValue)
	splitPoses = append(splitPoses, splitPos)
	splitValues2, splitPoses2 := SplitPoints(points.SubArray(splitPos, points.Len()), numStrips2)
	splitValues = append(splitValues, splitValues2...)
	splitPoses = append(splitPoses, splitPoses2...)
	return
}
