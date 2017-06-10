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

func (p *Point) Equal(rhs Point) (res bool) {
	if p.UserData != rhs.UserData || len(p.Vals) != len(rhs.Vals) {
		return
	}
	for dim := 0; dim < len(p.Vals); dim++ {
		if p.Vals[dim] != rhs.Vals[dim] {
			return
		}
	}
	res = true
	return
}

//Encode encode in place. Refers to binary.Write impl in standard library.
//len(b) shall be no less than bytesPerDim*numDims+8
func (p *Point) Encode(b []byte, bytesPerDim int) {
	numDims := len(p.Vals)
	for i := 0; i < numDims; i++ {
		switch bytesPerDim {
		case 1:
			b[i] = byte(p.Vals[i])
		case 2:
			binary.BigEndian.PutUint16(b[2*i:], uint16(p.Vals[i]))
		case 4:
			binary.BigEndian.PutUint32(b[4*i:], uint32(p.Vals[i]))
		case 8:
			binary.BigEndian.PutUint64(b[8*i:], p.Vals[i])
		}
	}
	binary.BigEndian.PutUint64(b[numDims*bytesPerDim:], p.UserData)
	return
}

func (p *Point) Decode(b []byte, numDims int, bytesPerDim int) {
	p.Vals = make([]uint64, numDims)
	for i := 0; i < numDims; i++ {
		switch bytesPerDim {
		case 1:
			p.Vals[i] = uint64(b[i])
		case 2:
			p.Vals[i] = uint64(binary.BigEndian.Uint16(b[2*i:]))
		case 4:
			p.Vals[i] = uint64(binary.BigEndian.Uint32(b[4*i:]))
		case 8:
			p.Vals[i] = binary.BigEndian.Uint64(b[8*i:])
		}
	}
	p.UserData = binary.BigEndian.Uint64(b[numDims*bytesPerDim:])
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
	b := make([]byte, pointSize)
	for _, point := range s.points {
		point.Encode(b, bytesPerDim)
		_, err = f.Write(b)
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
	point.Decode(pi, s.numDims, s.bytesPerDim)
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
	b := make([]byte, s.pointSize)
	point.Encode(b, s.bytesPerDim)
	var off int64
	for off = s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		pi := make([]byte, s.pointSize)
		_, err = s.f.ReadAt(pi, off)
		if err != nil {
			return
		}
		found = bytes.Equal(b, pi)
		if found {
			break
		}
	}
	if found {
		//replace the matched point with the last point and decrease the array length
		idxLast := s.numPoints - 1
		pLast := make([]byte, s.pointSize)
		offLast := s.offBegin + int64(idxLast*s.pointSize)
		_, err = s.f.ReadAt(pLast, offLast)
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
	pi := make([]byte, s.pointSize)
	for off := s.offBegin; off < s.offBegin+int64(s.numPoints*s.pointSize); off += int64(s.pointSize) {
		_, err = s.f.ReadAt(pi, off)
		if err != nil {
			return
		}
		p.Decode(pi, s.numDims, s.bytesPerDim)
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
	for i := 0; i < len(splitPoses2); i++ {
		splitPoses = append(splitPoses, splitPos+splitPoses2[i])
	}
	return
}
