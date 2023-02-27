package tstorage

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_storage_Select(t *testing.T) {
	tests := []struct {
		name    string
		storage storage
		metric  uint32
		start   int64
		end     int64
		want    []*DataPoint
		wantErr bool
	}{
		{
			name:   "select from single partition",
			metric: metric1,
			start:  1,
			end:    4,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: metric1},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
			},
		},
		{
			name:   "select from three partitions",
			metric: metric1,
			start:  1,
			end:    10,
			storage: func() storage {
				part1 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err := part1.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 1}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 2}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 3}, Metric: metric1},
				})
				if err != nil {
					panic(err)
				}
				part2 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part2.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 4}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 5}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 6}, Metric: metric1},
				})
				if err != nil {
					panic(err)
				}
				part3 := newMemoryPartition(nil, 1*time.Hour, Seconds, math.MaxInt64)
				_, err = part3.insertRows([]Row{
					{DataPoint: DataPoint{Timestamp: 7}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 8}, Metric: metric1},
					{DataPoint: DataPoint{Timestamp: 9}, Metric: metric1},
				})
				if err != nil {
					panic(err)
				}
				list := newPartitionList()
				list.insert(part1)
				list.insert(part2)
				list.insert(part3)

				return storage{
					partitionList:  list,
					workersLimitCh: make(chan struct{}, defaultWorkersLimit),
				}
			}(),
			want: []*DataPoint{
				{Timestamp: 1},
				{Timestamp: 2},
				{Timestamp: 3},
				{Timestamp: 4},
				{Timestamp: 5},
				{Timestamp: 6},
				{Timestamp: 7},
				{Timestamp: 8},
				{Timestamp: 9},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.storage.Select(tt.metric, tt.start, tt.end)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStorage_Poll(t *testing.T) {
	storage, err := NewStorage(
		WithTimestampPrecision(Seconds),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			panic(err)
		}
	}()
	err = storage.InsertRows([]Row{
		{Metric: metric1, DataPoint: DataPoint{Timestamp: 1600000000, Value: 0.1}},
		{Metric: metric1, DataPoint: DataPoint{Timestamp: 1600000002, Value: 0.2}},
		{Metric: metric1, DataPoint: DataPoint{Timestamp: 1600000001, Value: 0.3}},
		{Metric: metric1, DataPoint: DataPoint{Timestamp: 1600000003, Value: 0.4}},
		{Metric: metric2, DataPoint: DataPoint{Timestamp: 1600000004, Value: 0.5}},
	})

	d := storage.Poll(metric1)
	assert.Equal(t, 0.4, d.Value)
}
