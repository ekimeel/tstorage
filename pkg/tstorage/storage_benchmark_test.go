package tstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStorage_InsertRows(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: metric1, DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

func BenchmarkStorage_InsertRows_WithoutLko(b *testing.B) {
	storage, err := NewStorage(WithoutLkoStorage())
	require.NoError(b, err)
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: metric1, DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

// Select data points among a thousand data in memory
func BenchmarkStorage_SelectAmongThousandPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	for i := 1; i < 1000; i++ {
		storage.InsertRows([]Row{
			{Metric: metric1, DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_, _ = storage.Select(metric1, 10, 100)
	}
}

// Select data points among a million data in memory
func BenchmarkStorage_SelectAmongMillionPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	for i := 1; i < 1000000; i++ {
		storage.InsertRows([]Row{
			{Metric: metric1, DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_, _ = storage.Select(metric1, 10, 100)
	}
}

// Select data points among a million data in memory
func BenchmarkStorage_PollAmongMillionPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)

	p := 0
	for i := 1; i < 1000000; i++ {
		storage.InsertRows([]Row{
			{Metric: uint32(p), DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
		if p < 1000 {
			p = p + 1
		} else {
			p = 0
		}
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_ = storage.Poll(uint32(i))
	}
}
