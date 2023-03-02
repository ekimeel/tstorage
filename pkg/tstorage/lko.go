package tstorage

import (
	"sync"
)

var lkoImpl *lko

type lko struct {
	lko      map[uint32]*DataPoint
	disabled bool
	sync.RWMutex
}

func getLko() *lko {
	if lkoImpl == nil {
		lkoImpl = &lko{}
		lkoImpl.lko = make(map[uint32]*DataPoint)
	}

	return lkoImpl
}

func (s *lko) poll(metric uint32) *DataPoint {
	if s.disabled {
		return nil
	}
	s.RLock()
	defer s.RUnlock()
	return s.lko[metric]
}

func (s *lko) acceptRow(row Row) bool {
	return s.accept(row.Metric, &row.DataPoint)
}

func (s *lko) accept(metric uint32, point *DataPoint) bool {
	if s.disabled {
		return false
	}

	cur := s.lko[metric]
	if cur == nil {
		s.Lock()
		s.lko[metric] = point
		defer s.Unlock()
		return true
	} else {
		if point.Timestamp > cur.Timestamp {
			s.Lock()
			s.lko[metric] = point
			defer s.Unlock()
			return true
		}
	}

	return false
}

func (s *lko) clear() {
	s.lko = make(map[uint32]*DataPoint)
}
