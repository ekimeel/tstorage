package tstorage

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ekimeel/tstorage/internal/cgroup"
	"github.com/ekimeel/tstorage/internal/timerpool"
	log "github.com/sirupsen/logrus"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

var (
	ErrNoDataPoints = errors.New("no data points found")

	// Limit the concurrency for data ingestion to GOMAXPROCS, since this operation
	// is CPU bound, so there is no sense in running more than GOMAXPROCS concurrent
	// goroutines on data ingestion path.
	defaultWorkersLimit = cgroup.AvailableCPUs()

	partitionDirRegex   = regexp.MustCompile(`^p-.+`)
	partitionFilesRegex = regexp.MustCompile(`p-.+[/\\\\].+`)
)

// TimestampPrecision represents precision of timestamps. See WithTimestampPrecision
type TimestampPrecision string

const (
	Nanoseconds  TimestampPrecision = "ns"
	Microseconds TimestampPrecision = "us"
	Milliseconds TimestampPrecision = "ms"
	Seconds      TimestampPrecision = "s"

	defaultPartitionDuration  = 1 * time.Hour
	defaultRetention          = 336 * time.Hour
	defaultTimestampPrecision = Nanoseconds
	defaultWriteTimeout       = 30 * time.Second
	defaultWALBufferedSize    = 4096

	defaultPartitionMaxSize = 1024 * 50        // 50KiB per partition
	defaultDatabaseMaxSize  = 1024 * 1024 * 10 // 10MiB per database
	defaultMaxPartitions    = 200              // 200 partitions per database

	writablePartitionsNum = 2
	checkExpiredInterval  = time.Hour

	walDirName = "wal"
)

// Storage provides goroutine safe capabilities of insertion into and retrieval from the time-series storage.
type Storage interface {
	Reader
	// InsertRows ingests the given rows to the time-series storage.
	// If the timestamp is empty, it uses the machine's local timestamp in UTC.
	// The precision of timestamps is nanoseconds by default. It can be changed using WithTimestampPrecision.
	InsertRows(rows []Row) error
	// Close gracefully shutdowns by flushing any unwritten data to the underlying disk partition.
	Close() error
}

// Reader provides reading access to time series data.
type Reader interface {
	// Select gives back a list of data points that matches a set of the given metric and
	// labels within the given start-end range. Keep in mind that start is inclusive, end is exclusive,
	// and both must be Unix timestamp. ErrNoDataPoints will be returned if no data points found.
	Select(metric uint32, start, end int64) (points []*DataPoint, err error)
	Poll(metric uint32) *DataPoint
}

// DataPoint represents a data point, the smallest unit of time series data.
type DataPoint struct {
	// The actual value. This field must be set.
	Value float64
	// Unix timestamp.
	Timestamp int64
}

// NewStorage gives back a new storage, which stores time-series data in the process memory by default.
//
// Give the WithDataPath option for running as a on-disk storage. Specify a directory with data already exists,
// then it will be read as the initial data.
func NewStorage(opts ...Option) (Storage, error) {
	s := &storage{
		partitionList:      newPartitionList(),
		workersLimitCh:     make(chan struct{}, defaultWorkersLimit),
		partitionDuration:  defaultPartitionDuration,
		retention:          defaultRetention,
		timestampPrecision: defaultTimestampPrecision,
		writeTimeout:       defaultWriteTimeout,
		walBufferedSize:    defaultWALBufferedSize,
		wal:                newNopWal(),
		logger:             log.New(),
		doneCh:             make(chan struct{}, 0),
		databaseMaxSize:    defaultDatabaseMaxSize,
		maxPartitions:      defaultMaxPartitions,
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.inMemoryMode() {
		err := s.newPartition(nil, false)
		if err != nil {
			log.Error(err)
		}
		return s, nil
	}

	// TODO: Possible point for multiple database implementation (for T8 integration)
	if err := os.MkdirAll(s.dataPath, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to make data directory %s: %w", s.dataPath, err)
	}

	walDir := filepath.Join(s.dataPath, walDirName)
	if s.walBufferedSize >= 0 {
		wal, err := newDiskWAL(walDir, s.walBufferedSize)
		if err != nil {
			return nil, err
		}
		s.wal = wal
	}

	// Read existent partitions from the disk.
	dirs, err := os.ReadDir(s.dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data directory: %w", err)
	}
	if len(dirs) == 0 {
		err := s.newPartition(nil, false)
		if err != nil {
			log.Error(err)
		}
		return s, nil
	}
	isPartitionDir := func(f fs.DirEntry) bool {
		return f.IsDir() && partitionDirRegex.MatchString(f.Name())
	}
	partitions := make([]partition, 0, len(dirs))
	for _, e := range dirs {
		if !isPartitionDir(e) {
			continue
		}
		path := filepath.Join(s.dataPath, e.Name())
		part, err := openDiskPartition(path, s.retention)
		if errors.Is(err, ErrNoDataPoints) {
			continue
		}
		if errors.Is(err, errInvalidPartition) {
			// It should be recovered by WAL
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to open disk partition for %s: %w", path, err)
		}
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].minTimestamp() < partitions[j].minTimestamp()
	})
	for _, p := range partitions {
		err := s.newPartition(p, false)
		if err != nil {
			log.Error(err)
		}
	}
	// Start WAL recovery if there is.
	if err := s.recoverWAL(walDir); err != nil {
		return nil, fmt.Errorf("failed to recover WAL: %w", err)
	}
	err = s.newPartition(nil, false)
	if err != nil {
		log.Error(err)
	}

	register(s)

	return s, nil
}

type storage struct {
	partitionList partitionList

	walBufferedSize    int
	wal                wal
	walRecoveryOption  WalRecoveryOption
	partitionDuration  time.Duration
	retention          time.Duration
	timestampPrecision TimestampPrecision
	dataPath           string
	writeTimeout       time.Duration
	databaseMaxSize    int64
	maxPartitions      int64

	logger         *log.Logger
	workersLimitCh chan struct{}
	// wg must be incremented to guarantee all writes are done gracefully.
	wg sync.WaitGroup

	doneCh chan struct{}
}

func (s *storage) InsertRows(rows []Row) error {
	s.wg.Add(1)
	defer s.wg.Done()

	insert := func() error {
		defer func() { <-s.workersLimitCh }()
		if err := s.ensureActiveHead(); err != nil {
			return err
		}
		if err := s.ensureHeadInSize(); err != nil {
			return err
		}
		iterator := s.partitionList.newIterator()
		n := s.partitionList.size()
		rowsToInsert := rows

		// Starting at the head partition, try to insert rows, and loop to insert outdated rows
		// into older partitions. Any rows more than `writablePartitionsNum` partitions out
		// of date are dropped.
		for i := 0; i < n && i < writablePartitionsNum; i++ {
			if len(rowsToInsert) == 0 {
				break
			}
			if !iterator.next() {
				break
			}
			outdatedRows, err := iterator.value().insertRows(rowsToInsert)
			if err != nil {
				return fmt.Errorf("failed to insert rows: %w", err)
			}
			rowsToInsert = outdatedRows
		}
		return nil
	}

	// Limit the number of concurrent goroutines to prevent from out of memory
	// errors and CPU trashing even if too many goroutines attempt to write.
	select {
	case s.workersLimitCh <- struct{}{}:
		return insert()
	default:
	}

	// Seems like all workers are busy; wait for up to writeTimeout

	t := timerpool.Get(s.writeTimeout)
	select {
	case s.workersLimitCh <- struct{}{}:
		timerpool.Put(t)
		return insert()
	case <-t.C:
		timerpool.Put(t)
		return fmt.Errorf("failed to write a data point in %s, since it is overloaded with %d concurrent writers",
			s.writeTimeout, defaultWorkersLimit)
	}
}

// ensureActiveHead ensures the head of partitionList is an active partition.
// If none, it creates a new one.
func (s *storage) ensureActiveHead() error {
	head := s.partitionList.getHead()
	if head != nil && head.active() {
		return nil
	}

	// All partitions seem to be inactive so add a new partition to the list.
	if err := s.newPartition(nil, true); err != nil {
		return err
	}
	go func() {
		if err := s.flushPartitions(); err != nil {
			s.logger.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()
	return nil
}

// ensureHeadInSize ensures the head of partitionList is under its maximum defined size.
// If none, it creates a new one.
func (s *storage) ensureHeadInSize() error {
	head := s.partitionList.getHead()
	if head != nil && !head.expired() {
		return nil
	}

	// Head partition does not exist or exceeds size
	if err := s.newPartition(nil, true); err != nil {
		return err
	}
	go func() {
		if err := s.flushPartitions(); err != nil {
			s.logger.Printf("failed to flush in-memory partitions: %v", err)
		}
	}()

	return nil
}

func (s *storage) Select(metric uint32, start, end int64) ([]*DataPoint, error) {
	if metric == 0 {
		return nil, fmt.Errorf("metric must be set")
	}
	if start >= end {
		return nil, fmt.Errorf("the given start is greater than end")
	}
	points := make([]*DataPoint, 0)

	// Iterate over all partitions from the newest one.
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return nil, fmt.Errorf("unexpected empty partition found")
		}
		if part.minTimestamp() == 0 {
			// Skip the partition that has no points.
			continue
		}
		if part.maxTimestamp() < start {
			// No need to keep going anymore
			break
		}
		if part.minTimestamp() > end {
			continue
		}
		ps, err := part.selectDataPoints(metric, start, end)
		if errors.Is(err, ErrNoDataPoints) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to select data points: %w", err)
		}
		// in order to keep the order in ascending.
		points = append(ps, points...)
	}
	if len(points) == 0 {
		return nil, ErrNoDataPoints
	}
	return points, nil
}

func (s *storage) Poll(metric uint32) *DataPoint {
	return getLko().poll(metric)
}

func (s *storage) Close() error {
	s.wg.Wait()
	close(s.doneCh)
	if err := s.wal.flush(); err != nil {
		return fmt.Errorf("failed to flush buffered WAL: %w", err)
	}

	// TODO: Prevent from new goroutines calling InsertRows(), for graceful shutdown.

	// Make all writable partitions read-only by inserting as same number of those.
	for i := 0; i < writablePartitionsNum; i++ {
		if err := s.newPartition(nil, true); err != nil {
			return err
		}

		if err := s.flushPartitions(); err != nil {
			return fmt.Errorf("failed to close storage: %w", err)
		}
	}

	if err := s.removeExpiredPartitions(); err != nil {
		return fmt.Errorf("failed to remove expired partitions: %w", err)
	}
	// All partitions have been flushed, so WAL isn't needed anymore.
	if err := s.wal.removeAll(); err != nil {
		return fmt.Errorf("failed to remove WAL: %w", err)
	}
	return nil
}

func (s *storage) newPartition(p partition, punctuateWal bool) error {
	if p == nil {
		// Check for max database size
		// Only when there are disk partitions, hence more than 2
		if s.partitionList.size() > 2 {
			overSize, err := s.checkDBOverMaxSize()
			if err != nil {
				return fmt.Errorf("failed to check database maximum size: %w", err)
			}

			for overSize {
				tail := s.partitionList.getTail()
				err = s.partitionList.remove(tail)
				if err != nil {
					log.Error(err)
				}

				overSize, err = s.checkDBOverMaxSize()
				if err != nil {
					return fmt.Errorf("failed to check database maximum size: %w", err)
				}
			}
		}

		p = newMemoryPartition(s.wal, s.partitionDuration, s.timestampPrecision)
	}
	s.partitionList.insert(p)

	// Check for max partitions
	// Only when there are disk partitions, hence more than 2
	// If in memory mode, unnecessary
	if !s.inMemoryMode() && s.partitionList.size() > 2 {
		numPart := s.partitionList.size() - 2
		for ; numPart > int(s.maxPartitions); numPart-- {
			tail := s.partitionList.getTail()
			err := s.partitionList.remove(tail)
			if err != nil {
				log.Error(err)
			}
		}
	}

	if punctuateWal {
		return s.wal.punctuate()
	}
	return nil
}

// Returns whether the database has exceeded its maximum size
func (s *storage) checkDBOverMaxSize() (bool, error) {
	var size int64
	err := filepath.WalkDir(s.dataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !partitionFilesRegex.MatchString(path) || d.IsDir() {
			return nil
		}

		fInfo, err := d.Info()
		if err != nil {
			return err
		}
		size += fInfo.Size()

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to walk database directory: %w", err)
	}

	return size > s.databaseMaxSize, err
}

// flushPartitions persists all in-memory partitions ready to persisted.
// For the in-memory mode, just removes it from the partition list.
func (s *storage) flushPartitions() error {
	// Keep the first two partitions as is even if they are inactive,
	// to accept out-of-order data points.
	i := 0
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		if i < writablePartitionsNum {
			i++
			continue
		}
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected empty partition found")
		}
		memPart, ok := part.(*memoryPartition)
		if !ok {
			continue
		}

		if s.inMemoryMode() {
			if err := s.partitionList.remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}

		// Start swapping in-memory partition for disk one.
		// The disk partition will place at where in-memory one existed.

		dir := filepath.Join(s.dataPath, fmt.Sprintf("p-%d-%d", memPart.minTimestamp(), memPart.maxTimestamp()))
		if err := s.flush(dir, memPart); err != nil {
			return fmt.Errorf("failed to compact memory partition into %s: %w", dir, err)
		}
		newPart, err := openDiskPartition(dir, s.retention)
		if errors.Is(err, ErrNoDataPoints) {
			if err := s.partitionList.remove(part); err != nil {
				return fmt.Errorf("failed to remove partition: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to generate disk partition for %s: %w", dir, err)
		}
		if err := s.partitionList.swap(part, newPart); err != nil {
			return fmt.Errorf("failed to swap partitions: %w", err)
		}

		if err := s.wal.removeOldest(); err != nil {
			return fmt.Errorf("failed to remove oldest WAL segment: %w", err)
		}
	}
	return nil
}

// flush compacts the data points in the given partition and flushes them to the given directory.
func (s *storage) flush(dirPath string, m *memoryPartition) error {
	if dirPath == "" {
		return fmt.Errorf("dir path is required")
	}

	if err := os.MkdirAll(dirPath, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory %q: %w", dirPath, err)
	}

	f, err := os.Create(filepath.Join(dirPath, dataFileName))
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", dirPath, err)
	}
	defer f.Close()
	encoder := newSeriesEncoder(f)

	metrics := map[uint32]diskMetric{}
	m.metrics.Range(func(key, value interface{}) bool {
		mt, ok := value.(*memoryMetric)
		if !ok {
			s.logger.Printf("unknown value found\n")
			return false
		}
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			s.logger.Printf("failed to set file offset of metric %q: %v\n", mt.name, err)
			return false
		}

		if err := mt.encodeAllPoints(encoder); err != nil {
			s.logger.Printf("failed to encode a data point that metric is %q: %v\n", mt.name, err)
			return false
		}

		if err := encoder.flush(); err != nil {
			s.logger.Printf("failed to flush data points that metric is %q: %v\n", mt.name, err)
			return false
		}

		totalNumPoints := mt.size + int64(len(mt.outOfOrderPoints))
		metrics[mt.name] = diskMetric{
			Name:          mt.name,
			Offset:        offset,
			MinTimestamp:  mt.minTimestamp,
			MaxTimestamp:  mt.maxTimestamp,
			NumDataPoints: totalNumPoints,
		}
		return true
	})

	b, err := json.Marshal(&meta{
		MinTimestamp:  m.minTimestamp(),
		MaxTimestamp:  m.maxTimestamp(),
		NumDataPoints: m.size(),
		Metrics:       metrics,
		CreatedAt:     time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	// It should write the meta file at lko because what valid meta file exists proves the disk partition is valid.
	metaPath := filepath.Join(dirPath, metaFileName)
	if err := os.WriteFile(metaPath, b, fs.ModePerm); err != nil {
		return fmt.Errorf("failed to write metadata to %s: %w", metaPath, err)
	}
	return nil
}

func (s *storage) removeExpiredPartitions() error {
	expiredList := make([]partition, 0)
	iterator := s.partitionList.newIterator()
	for iterator.next() {
		part := iterator.value()
		if part == nil {
			return fmt.Errorf("unexpected nil partition found")
		}
		if part.expired() {
			expiredList = append(expiredList, part)
		}
	}

	for i := range expiredList {
		if err := s.partitionList.remove(expiredList[i]); err != nil {
			return fmt.Errorf("failed to remove expired partition")
		}
	}
	return nil
}

// recoverWAL inserts all records within the given wal, and then removes all WAL segment files.
func (s *storage) recoverWAL(walDir string) error {
	reader, err := newDiskWALReader(walDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	if s.partitionList.getTail() == nil {
		i := s.partitionList.newIterator()
		for i.next() {
			s.partitionList.
		}
	}

	if err := reader.readAll(s.walRecoveryOption); err != nil {
		log.Errorf("failed to read WAL: %s", err)
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(reader.rowsToInsert) == 0 {
		return nil
	}
	if err := s.InsertRows(reader.rowsToInsert); err != nil {
		log.Errorf("failed to insert rows recovered from WAL: %s", err)
		return fmt.Errorf("failed to insert rows recovered from WAL: %w", err)
	}
	return s.wal.refresh()
}

func (s *storage) inMemoryMode() bool {
	return s.dataPath == "" || s.maxPartitions == 0 || s.databaseMaxSize == 0
}
