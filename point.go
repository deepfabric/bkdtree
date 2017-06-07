package bkdtree

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	"github.com/keegancsmith/nth"
)

type Point interface {
	// Return the value X_{dim}, dim is started from 0
	GetValue(dim int) uint64
	GetUserData() uint64
}

type PointBase struct {
	Vals     []uint64
	UserData uint64
}

type PointArray interface {
	sort.Interface
	GetPoint(idx int) Point
	GetValue(idx int) uint64
	SubArray(begin, end int) PointArray
	Erase(point Point) (bool, error)
}

type PointArrayMem struct {
	points  []Point
	byDim   int
	numDims int
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

func IsInside(point, lowPoint, highPoint Point, numDims int) (isInside bool) {
	isInside = true
	for dim := 0; dim < numDims; dim++ {
		if point.GetValue(dim) < lowPoint.GetValue(dim) || point.GetValue(dim) > highPoint.GetValue(dim) {
			isInside = false
			break
		}
	}
	return
}

func Equals(lhs, rhs Point, numDims int) (isEqual bool) {
	isEqual = true
	for dim := 0; dim < numDims; dim++ {
		if lhs.GetValue(dim) != rhs.GetValue(dim) {
			isEqual = false
			return
		}
	}
	return
}

func (b *PointBase) GetValue(dim int) (val uint64) {
	val = b.Vals[dim]
	return
}

func (b *PointBase) GetUserData() (userData uint64) {
	userData = b.UserData
	return
}

func NewPointBase(vals []uint64, userData uint64) (pb *PointBase) {
	pb = &PointBase{Vals: vals, UserData: userData}
	return
}

//TODO: optimize point encoding/decoding
func EncodePoint(point Point, numDims, bytesPerDim int) (res []byte) {
	pointSize := bytesPerDim*numDims + 8
	res = make([]byte, pointSize)
	buf := new(bytes.Buffer)
	idx := 0
	for dim := 0; dim < numDims; dim++ {
		val := point.GetValue(dim)
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
	userData := point.GetUserData()
	err := binary.Write(buf, binary.BigEndian, userData)
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

func DecodePoint(bytesP []byte, numDims int, bytesPerDim int) (p Point) {
	pointSize := bytesPerDim*numDims + 8
	if len(bytesP) < pointSize {
		return
	}
	pb := &PointBase{}
	p = pb
	for dim := 0; dim < numDims; dim++ {
		var val uint64 = 0
		for i := 0; i < bytesPerDim; i++ {
			val *= 8
			val += uint64(bytesP[dim*bytesPerDim+i])
		}
		pb.Vals = append(pb.Vals, val)
	}
	var userData uint64 = 0
	for i := numDims * bytesPerDim; i < pointSize; i++ {
		userData *= 8
		userData += uint64(bytesP[i])
	}
	pb.UserData = userData
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
	return s.points[i].GetValue(s.byDim) < s.points[j].GetValue(s.byDim)
}

func (s *PointArrayMem) GetPoint(idx int) (point Point) {
	point = s.points[idx]
	return
}

func (s *PointArrayMem) GetValue(idx int) (val uint64) {
	val = s.points[idx].GetValue(s.byDim)
	return
}

func (s *PointArrayMem) SubArray(begin, end int) (sub PointArray) {
	sub = &PointArrayMem{
		points:  s.points[begin:end],
		byDim:   s.byDim,
		numDims: s.numDims,
	}
	return
}

func (s *PointArrayMem) Erase(point Point) (found bool, err error) {
	found = false
	idx := 0
	for i, point2 := range s.points {
		//assumes each point's userData is unique
		if Equals(point, point2, s.numDims) && point.GetUserData() == point2.GetUserData() {
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
	pae = &PointArrayExt{
		f:           f,
		offBegin:    0,
		numPoints:   len(s.points),
		byDim:       s.byDim,
		numDims:     s.numDims,
		bytesPerDim: bytesPerDim,
		pointSize:   s.numDims*bytesPerDim + 8,
	}
	pae.offBegin, err = f.Seek(0, 1)
	if err != nil {
		return
	}
	for _, point := range s.points {
		bytesP := EncodePoint(point, s.numDims, bytesPerDim)
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
	s.f.ReadAt(pi, offI) //TODO: handle error?
	s.f.ReadAt(pj, offJ)
	s.f.WriteAt(pi, offJ)
	s.f.WriteAt(pj, offI)
}

// Less is part of sort.Interface.
func (s *PointArrayExt) Less(i, j int) bool {
	pi := make([]byte, s.pointSize)
	pj := make([]byte, s.pointSize)
	offI := s.offBegin + int64(i*s.pointSize)
	offJ := s.offBegin + int64(j*s.pointSize)
	s.f.ReadAt(pi, offI) //TODO: handle error?
	s.f.ReadAt(pj, offJ)
	for idx := s.byDim * s.bytesPerDim; idx < (s.byDim+1)*s.bytesPerDim; idx++ {
		if pi[idx] > pj[idx] {
			return false
		}
	}
	return true
}

func (s *PointArrayExt) GetPoint(idx int) (point Point) {
	pb := &PointBase{Vals: make([]uint64, s.numDims), UserData: 0}
	point = pb
	pi := make([]byte, s.pointSize)
	offI := s.offBegin + int64(idx*s.pointSize)
	s.f.ReadAt(pi, offI) //TODO: handle error?
	for dim := 0; dim < s.numDims; dim++ {
		for i := s.bytesPerDim * dim; i < s.bytesPerDim*(dim+1); i++ {
			pb.Vals[i] *= 8
			pb.Vals[i] += uint64(pi[i])
		}
	}
	for i := s.bytesPerDim * s.numDims; i < s.pointSize; i++ {
		pb.UserData *= 8
		pb.UserData += uint64(pi[i])
	}
	return
}

func (s *PointArrayExt) GetValue(idx int) (val uint64) {
	pi := make([]byte, s.pointSize)
	offI := s.offBegin + int64(idx*s.pointSize)
	s.f.ReadAt(pi, offI) //TODO: handle error?
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
	bytesP := EncodePoint(point, s.numDims, s.bytesPerDim)
	off := int64(0)
	for off = s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		pi := make([]byte, s.pointSize)
		_, err = s.f.ReadAt(pi, off) //TODO: handle error?
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
	off := int64(0)
	for off = s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		pi := make([]byte, s.pointSize)
		_, err = s.f.ReadAt(pi, off) //TODO: handle error?
		if err != nil {
			return
		}
		point := DecodePoint(pi, s.numDims, s.bytesPerDim)
		points = append(points, point)
	}
	pam = &PointArrayMem{
		points:  points,
		byDim:   s.byDim,
		numDims: s.numDims,
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
	splitValue := points.GetValue(splitPos)

	numStrips1 := (numStrips + 1) / 2
	numStrips2 := numStrips - numStrips1
	splitValues1, splitPoses1 := SplitPoints(points.SubArray(0, splitPos), numStrips1)
	splitValues2, splitPoses2 := SplitPoints(points.SubArray(splitPos, points.Len()), numStrips2)
	for _, val := range splitValues1 {
		splitValues = append(splitValues, val)
	}
	for _, pos := range splitPoses1 {
		splitPoses = append(splitPoses, pos)
	}
	splitValues = append(splitValues, splitValue)
	splitPoses = append(splitPoses, splitPos)
	for _, val := range splitValues2 {
		splitValues = append(splitValues, val)
	}
	for _, pos := range splitPoses2 {
		splitPoses = append(splitPoses, pos+splitPos)
	}
	return
}
