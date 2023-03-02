package tstorage

type mockPartition struct {
	minT             int64
	maxT             int64
	numPoints        int
	IsActive         bool
	partitionMaxSize bool
	last             map[uint32]*DataPoint
	lko              lko
	err              error
}

func (f *mockPartition) insertRows(rows []Row) ([]Row, error) {
	for _, row := range rows {
		f.lko.acceptRow(row)
	}
	return nil, f.err
}

func (f *mockPartition) selectDataPoints(_ uint32, _, _ int64) ([]*DataPoint, error) {
	return nil, f.err
}

func (f *mockPartition) minTimestamp() int64 {
	return f.minT
}

func (f *mockPartition) maxTimestamp() int64 {
	return f.maxT
}

func (f *mockPartition) size() int {
	return f.numPoints
}

func (f *mockPartition) active() bool {
	return f.IsActive
}

func (f *mockPartition) underMaxSize() bool {
	return f.partitionMaxSize
}

func (f *mockPartition) clean() error {
	return nil
}

func (f *mockPartition) expired() bool {
	return false
}
