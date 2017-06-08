package bkdtree

import (
	"reflect"
	"testing"
)

func TestKdIntersectSome(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, PageSize4K)

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, 1)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	for _, point := range visitor.points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}
func TestKdIntersectAll(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, PageSize4K)

	lowPoint := Point{[]uint64{0, 0, 0}, 0}
	highPoint := Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0, size)}
	kdt.Intersect(visitor)
	if len(visitor.points) != size {
		t.Errorf("found %v matchs, however %v expected", len(visitor.points), size)
	}
}

func TestKdIntersect(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 100000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, PageSize4K)

	lowPoint := Point{[]uint64{20, 30, 40}, 0}
	highPoint := Point{[]uint64{maxVal, maxVal, maxVal}, 0}
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	for _, point := range visitor.points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
	}
}

func TestKdInsert(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, PageSize4K)

	newPoint := Point{[]uint64{40, 30, 20}, maxVal} //use unique userData
	kdt.Insert(newPoint)

	lowPoint := newPoint
	highPoint := newPoint
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) <= 0 {
		t.Errorf("found 0 matchs, however some expected")
	}
	numMatchs := 0
	for _, point := range visitor.points {
		isInside := point.Inside(lowPoint, highPoint)
		if !isInside {
			t.Errorf("point %v is ouside of range", point)
		}
		if point.UserData == newPoint.UserData {
			numMatchs++
		}
	}
	if numMatchs != 1 {
		t.Errorf("found %v matchs, however 1 expected", numMatchs)
	}
}

func TestKdErase(t *testing.T) {
	numDims := 3
	maxVal := uint64(1000)
	size := 1000
	points := NewRandPoints(numDims, maxVal, size)
	kdt := NewKdTree(points, numDims, PageSize4K)

	kdt.Erase(points[0])

	lowPoint := points[0]
	highPoint := points[0]
	visitor := &IntersectCollector{lowPoint, highPoint, make([]Point, 0)}
	kdt.Intersect(visitor)

	//fmt.Printf("%v\n", visitor.points)
	if len(visitor.points) != 0 {
		t.Errorf("found %v matchs, however 0 expected", len(visitor.points))
	}
}

func TestU64Slice_Len(t *testing.T) {
	tests := []struct {
		name string
		a    U64Slice
		want int
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Len(); got != tt.want {
				t.Errorf("U64Slice.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestU64Slice_Swap(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		a    U64Slice
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.Swap(tt.args.i, tt.args.j)
		})
	}
}

func TestU64Slice_Less(t *testing.T) {
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name string
		a    U64Slice
		args args
		want bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("U64Slice.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntersectCollector_GetLowPoint(t *testing.T) {
	tests := []struct {
		name string
		d    *IntersectCollector
		want Point
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.GetLowPoint(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("IntersectCollector.GetLowPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntersectCollector_GetHighPoint(t *testing.T) {
	tests := []struct {
		name string
		d    *IntersectCollector
		want Point
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.d.GetHighPoint(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("IntersectCollector.GetHighPoint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIntersectCollector_VisitPoint(t *testing.T) {
	type args struct {
		point Point
	}
	tests := []struct {
		name string
		d    *IntersectCollector
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.d.VisitPoint(tt.args.point)
		})
	}
}

func TestNewKdTree(t *testing.T) {
	type args struct {
		points    []Point
		numDims   int
		blockSize int
	}
	tests := []struct {
		name string
		args args
		want *KdTree
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewKdTree(tt.args.points, tt.args.numDims, tt.args.blockSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewKdTree() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createKdTree(t *testing.T) {
	type args struct {
		points   []Point
		depth    int
		numDims  int
		leafCap  int
		intraCap int
	}
	tests := []struct {
		name string
		args args
		want KdTreeNode
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createKdTree(tt.args.points, tt.args.depth, tt.args.numDims, tt.args.leafCap, tt.args.intraCap); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createKdTree() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKdTreeIntraNode_intersect(t *testing.T) {
	type args struct {
		visitor IntersectVisitor
		numDims int
	}
	tests := []struct {
		name string
		n    *KdTreeIntraNode
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n.intersect(tt.args.visitor, tt.args.numDims)
		})
	}
}

func TestKdTreeLeafNode_intersect(t *testing.T) {
	type args struct {
		visitor IntersectVisitor
		numDims int
	}
	tests := []struct {
		name string
		n    *KdTreeLeafNode
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n.intersect(tt.args.visitor, tt.args.numDims)
		})
	}
}

func TestKdTree_Intersect(t *testing.T) {
	type args struct {
		visitor IntersectVisitor
	}
	tests := []struct {
		name string
		t    *KdTree
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t.Intersect(tt.args.visitor)
		})
	}
}

func TestKdTreeIntraNode_insert(t *testing.T) {
	type args struct {
		point   Point
		numDims int
	}
	tests := []struct {
		name string
		n    *KdTreeIntraNode
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n.insert(tt.args.point, tt.args.numDims)
		})
	}
}

func TestKdTreeLeafNode_insert(t *testing.T) {
	type args struct {
		point   Point
		numDims int
	}
	tests := []struct {
		name string
		n    *KdTreeLeafNode
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n.insert(tt.args.point, tt.args.numDims)
		})
	}
}

func TestKdTree_Insert(t *testing.T) {
	type args struct {
		point Point
	}
	tests := []struct {
		name string
		t    *KdTree
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t.Insert(tt.args.point)
		})
	}
}

func TestKdTreeIntraNode_erase(t *testing.T) {
	type args struct {
		point   Point
		numDims int
	}
	tests := []struct {
		name      string
		n         *KdTreeIntraNode
		args      args
		wantFound bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFound := tt.n.erase(tt.args.point, tt.args.numDims); gotFound != tt.wantFound {
				t.Errorf("KdTreeIntraNode.erase() = %v, want %v", gotFound, tt.wantFound)
			}
		})
	}
}

func TestKdTreeLeafNode_erase(t *testing.T) {
	type args struct {
		point   Point
		numDims int
	}
	tests := []struct {
		name      string
		n         *KdTreeLeafNode
		args      args
		wantFound bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFound := tt.n.erase(tt.args.point, tt.args.numDims); gotFound != tt.wantFound {
				t.Errorf("KdTreeLeafNode.erase() = %v, want %v", gotFound, tt.wantFound)
			}
		})
	}
}

func TestKdTree_Erase(t *testing.T) {
	type args struct {
		point Point
	}
	tests := []struct {
		name string
		t    *KdTree
		args args
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t.Erase(tt.args.point)
		})
	}
}
