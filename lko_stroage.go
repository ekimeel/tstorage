package tstorage

var lkoImpl *lkoStorage

type lkoStorage struct {
	lko      map[uint32]*DataPoint
	disabled bool
	//sync.RWMutex
}

func getLkoStorage() *lkoStorage {
	if lkoImpl == nil {
		lkoImpl = &lkoStorage{}
		lkoImpl.lko = make(map[uint32]*DataPoint)
	}

	return lkoImpl
}

func (s *lkoStorage) poll(metric uint32) *DataPoint {
	if s.disabled {
		return nil
	}
	//s.RLock()
	//defer s.RUnlock()
	return s.lko[metric]
}

func (s *lkoStorage) acceptRow(row Row) bool {
	return s.accept(row.Metric, &row.DataPoint)
}

func (s *lkoStorage) accept(metric uint32, point *DataPoint) bool {
	if s.disabled {
		return false
	}

	cur := s.lko[metric]
	if cur == nil {
		//s.Lock()
		s.lko[metric] = point
		//defer s.Unlock()
		return true
	} else {
		if point.Timestamp > cur.Timestamp {
			//s.Lock()
			s.lko[metric] = point
			//defer s.Unlock()
			return true
		}
	}

	return false
}

func (s *lkoStorage) clear() {
	s.lko = make(map[uint32]*DataPoint)
}
