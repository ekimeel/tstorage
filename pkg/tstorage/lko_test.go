package tstorage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getLkoStorage_signleton(t *testing.T) {
	a := getLko()
	b := getLko()
	assert.Equal(t, a, b)
}

func Test_LkoStorage_poll(t *testing.T) {
	s := getLko()

	s.accept(metric1, &DataPoint{Timestamp: 100, Value: 100})
	s.accept(metric1, &DataPoint{Timestamp: 99, Value: 99})
	s.accept(metric1, &DataPoint{Timestamp: 98, Value: 98})
	s.accept(metric2, &DataPoint{Timestamp: 101, Value: 101})
	s.accept(metric1, &DataPoint{Timestamp: 90, Value: 90})

	assert.Equal(t, float64(100), s.poll(metric1).Value)
	assert.Equal(t, float64(101), s.poll(metric2).Value)
}
