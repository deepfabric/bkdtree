package bkdtree

import (
	"encoding/binary"
	"io"
	"os"
	"syscall"

	"github.com/pkg/errors"
)

type KdTreeExtNodeInfo struct {
	Offset    uint64 //offset in file. It's a leaf if less than pointsOffEnd, otherwise an intra node.
	NumPoints uint64 //number of points of subtree rooted at this node
}

const KdTreeExtNodeInfoSize int64 = 8 + 8

//KdTreeExtIntraNode is struct of intra node.
/**
 * invariants:
 * 1. NumStrips == 1 + len(SplitValues) == len(Children).
 * 2. values in SplitValues are in non-decreasing order.
 * 3. offset in Children are in increasing order.
 */
type KdTreeExtIntraNode struct {
	SplitDim    uint32
	NumStrips   uint32
	SplitValues []uint64
	Children    []KdTreeExtNodeInfo
}

// KdTreeExtMeta is persisted at the end of file.
/**
 * Some fields are redundant in order to make the file be self-descriptive.
 * Attention:
 * 1. Keep all fields exported to allow one invoke of binary.Read() to parse the whole struct.
 * 2. Keep KdTreeExtMeta be 4 bytes aligned.
 * 3. Keep formatVer one byte, and be the last member.
 * 4. Keep KdMetaSize be sizeof(KdTreeExtMeta);
 */
type KdTreeExtMeta struct {
	PointsOffEnd uint64 //the offset end of points
	RootOff      uint64 //the offset of root KdTreeExtIntraNode
	NumPoints    uint64 //the current number of points. Deleting points could trigger rebuilding the tree.
	LeafCap      uint16
	IntraCap     uint16
	NumDims      uint8
	BytesPerDim  uint8
	PointSize    uint8
	FormatVer    uint8 //the file format version. shall be the last byte of the file.
}

//KdTreeExtMetaSize is sizeof(KdTreeExtMeta)
const KdTreeExtMetaSize int = 8*3 + 4 + 4

type BkdSubTree struct {
	meta KdTreeExtMeta
	f    *os.File
	data []byte //file content via mmap
}

//BkdTree is a BKD tree
type BkdTree struct {
	bkdCap      int // N in the paper
	t0mCap      int // M in the paper, the capacity of in-memory buffer
	numDims     int // number of point dimensions
	bytesPerDim int // number of bytes of each encoded dimension
	pointSize   int
	leafCap     int    // limit of points a leaf node can hold
	intraCap    int    // limit of children of a intra node can hold
	dir         string //directory of files which hold the persisted kdtrees
	prefix      string //prefix of file names
	NumPoints   int
	t0m         []Point // T0M in the paper, in-memory buffer.
	trees       []BkdSubTree
}

func (n *KdTreeExtIntraNode) Read(r io.Reader) (err error) {
	//According to https://golang.org/pkg/encoding/binary/#Read,
	//"Data must be a pointer to a fixed-size value or a slice of fixed-size values."
	//Slice shall be adjusted to the expected length before calling binary.Read().
	err = binary.Read(r, binary.BigEndian, &n.SplitDim)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Read(r, binary.BigEndian, &n.NumStrips)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	n.SplitValues = make([]uint64, n.NumStrips-1)
	err = binary.Read(r, binary.BigEndian, &n.SplitValues)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	n.Children = make([]KdTreeExtNodeInfo, n.NumStrips)
	err = binary.Read(r, binary.BigEndian, &n.Children) //TODO: why n.children doesn't work?
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (n *KdTreeExtIntraNode) Write(w io.Writer) (err error) {
	//According to https://golang.org/pkg/encoding/binary/#Write,
	//"Data must be a fixed-size value or a slice of fixed-size values, or a pointer to such data."
	//Structs with slice members can not be used with binary.Write. Slice members shall be write explictly.
	err = binary.Write(w, binary.BigEndian, &n.SplitDim)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.NumStrips)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.SplitValues)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = binary.Write(w, binary.BigEndian, &n.Children)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//NewBkdTree creates a BKDTree
func NewBkdTree(t0mCap, bkdCap, numDims, bytesPerDim, leafCap, intraCap int, dir, prefix string) (bkd *BkdTree) {
	if t0mCap <= 0 || bkdCap < t0mCap || numDims <= 0 ||
		(bytesPerDim != 1 && bytesPerDim != 2 && bytesPerDim != 4 && bytesPerDim != 8) ||
		leafCap <= 0 || leafCap >= int(^uint16(0)) || intraCap <= 2 || intraCap >= int(^uint16(0)) {
		return
	}
	bkd = &BkdTree{
		bkdCap:      bkdCap,
		t0mCap:      t0mCap,
		numDims:     numDims,
		bytesPerDim: bytesPerDim,
		pointSize:   numDims*bytesPerDim + 8,
		leafCap:     leafCap,
		intraCap:    intraCap,
		dir:         dir,
		prefix:      prefix,
		t0m:         make([]Point, 0, t0mCap),
		trees:       make([]BkdSubTree, 0),
	}
	return
}

func (bkd *BkdTree) GetCap() int {
	return bkd.bkdCap
}

//https://medium.com/@arpith/adventures-with-mmap-463b33405223
func mmapFile(f *os.File) (data []byte, err error) {
	info, err1 := f.Stat()
	if err1 != nil {
		err = errors.Wrap(err1, "")
		return
	}
	prots := []int{syscall.PROT_WRITE | syscall.PROT_READ, syscall.PROT_READ}
	for _, prot := range prots {
		data, err = syscall.Mmap(int(f.Fd()), 0, int(info.Size()), prot, syscall.MAP_SHARED)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func munmapFile(data []byte) (err error) {
	err = syscall.Munmap(data)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}
