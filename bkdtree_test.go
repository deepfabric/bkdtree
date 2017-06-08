package bkdtree

import (
	"io"
	"reflect"
	"testing"
)

func TestBkdInsert(t *testing.T) {
	t0mCap := 1000
	treesCap := 5
	numDims := 2
	bytesPerDim := 4
	blockSize := 512
	dir := "/tmp"
	prefix := "bkd"
	bkd := NewBkdTree(t0mCap, treesCap, numDims, bytesPerDim, blockSize, dir, prefix)
	if bkd == nil {
		t.Fatalf("bkd is nil")
	}

	bkdCap := t0mCap<<uint(treesCap) - 1
	maxVal := uint64(1000)
	size := bkdCap + 1
	points := NewRandPoints(numDims, maxVal, size)

	//insert points
	for i := 0; i < bkdCap; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %v", i, err)
		}
		if bkd.numPoints != i+1 {
			t.Fatalf("incorrect numPoints. numPoints=%v, i=%v", bkd.numPoints, i)
		}
		remained := bkd.numPoints % bkd.t0mCap
		quotient := bkd.numPoints / bkd.t0mCap
		if len(bkd.t0m) != remained {
			t.Fatalf("bkd.numPoints %d, len(bkd.t0m) %d is incorect, want %d", bkd.numPoints, len(bkd.t0m), remained)
		}
		for i := 0; i < len(bkd.trees); i++ {
			tiCap := bkd.t0mCap << uint(i)
			want := tiCap * (quotient % 2)
			if bkd.trees[i].numPoints != uint64(want) {
				t.Fatalf("bkd.numPoints %d, bkd.tree[%d].numPoints %d is incorrect, want %d", bkd.numPoints, i, bkd.trees[i].numPoints, want)
			}
			quotient >>= 1
		}
	}
	err := bkd.Insert(points[bkdCap])
	if err == nil {
		t.Fatalf("bkd.Insert shall fail if tree is full")
	}
}

func TestKdTreeExtIntraNode_Read(t *testing.T) {
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name    string
		n       *KdTreeExtIntraNode
		args    args
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.n.Read(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("KdTreeExtIntraNode.Read() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewBkdTree(t *testing.T) {
	type args struct {
		bkdCap      int
		t0mCap      int
		numDims     int
		bytesPerDim int
		blockSize   int
		dir         string
		prefix      string
	}
	tests := []struct {
		name    string
		args    args
		wantBkd *BkdTree
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotBkd := NewBkdTree(tt.args.bkdCap, tt.args.t0mCap, tt.args.numDims, tt.args.bytesPerDim, tt.args.blockSize, tt.args.dir, tt.args.prefix); !reflect.DeepEqual(gotBkd, tt.wantBkd) {
				t.Errorf("NewBkdTree() = %v, want %v", gotBkd, tt.wantBkd)
			}
		})
	}
}
