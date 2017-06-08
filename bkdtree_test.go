package bkdtree

import (
	"io"
	"reflect"
	"testing"
)

func TestBkdInsert(t *testing.T) {
	bkdCap := 1000000
	t0mCap := 1000
	numDims := 2
	bytesPerDim := 4
	blockSize := 512
	dir := "/tmp"
	prefix := "bkd"
	bkd := NewBkdTree(bkdCap, t0mCap, numDims, bytesPerDim, blockSize, dir, prefix)
	if bkd == nil {
		t.Fatalf("bkd is nil")
	}

	maxVal := uint64(1000)
	size := 10 * t0mCap
	points := NewRandPoints(numDims, maxVal, size)

	//insert points
	for i := 0; i < size; i++ {
		err := bkd.Insert(points[i])
		if err != nil {
			t.Fatalf("bkd.Insert failed, i=%v, err: %v", i, err)
		}
		if bkd.numPoints != i+1 {
			t.Fatalf("incorrect numPoints. numPoints=%v, i=%v", bkd.numPoints, i)
		}
		if bkd.numPoints < t0mCap {
			// all points shall be at T0M
			if len(bkd.t0m) != bkd.numPoints {
				t.Fatalf("incorrect point distribution")
			}
		} else if bkd.numPoints < 2*t0mCap {
			// points are at T0M and T0
			if len(bkd.t0m) != bkd.numPoints-t0mCap || int(bkd.trees[0].numPoints) != t0mCap {
				t.Fatalf("incorrect point distribution")
			}
		}
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
