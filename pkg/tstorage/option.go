package tstorage

import (
	log "github.com/sirupsen/logrus"
	"time"
)

// Option is an optional setting for NewStorage.
type Option func(*storage)

// WithDatabaseMaxSize specifies the maximum size the database can have.
// Higher size leads to deletion of oldest partitions until the database
// fits within the maximum stablished size.
//
// Defaults to 10MiB.
func WithDatabaseMaxSize(databaseMaxSize int64) Option {
	return func(s *storage) {
		s.databaseMaxSize = databaseMaxSize
	}
}

// WithMaxPartitions specifies the maximum amount of partitions the database
// can have. Higher number leads to deletion of oldest partitions, until the
// database fits within the maximum. Memory partitions are not taken in
// consideration.
//
// Defaults to 200 partitions. A value of 0 implies the same behaviour as the
// in memory mode, which means no data will be persisted.
func WithMaxPartitions(maxPartitions int64) Option {
	return func(s *storage) {
		s.maxPartitions = maxPartitions
	}
}

// WithDataPath specifies the path to directory that stores time-series data.
// Use this to make time-series data persistent on disk.
//
// Defaults to empty string which means no data will get persisted.
func WithDataPath(dataPath string) Option {
	return func(s *storage) {
		s.dataPath = dataPath
	}
}

// WithPartitionDuration specifies the timestamp range of partitions.
// Once it exceeds the given time range, the new partition gets inserted.
//
// A partition is a chunk of time-series data with the timestamp range.
// It acts as a fully independent database containing all data
// points for its time range.
//
// Defaults to 1h
func WithPartitionDuration(duration time.Duration) Option {
	return func(s *storage) {
		s.partitionDuration = duration
	}
}

// WithRetention specifies when to remove old data.
// Data points will get automatically removed from the disk after a
// specified period of time after a disk partition was created.
// Defaults to 14d.
func WithRetention(retention time.Duration) Option {
	return func(s *storage) {
		s.retention = retention
	}
}

// WithTimestampPrecision specifies the precision of timestamps to be used by all operations.
//
// Defaults to Nanoseconds
func WithTimestampPrecision(precision TimestampPrecision) Option {
	return func(s *storage) {
		s.timestampPrecision = precision
	}
}

// WithWriteTimeout specifies the timeout to wait when workers are busy.
//
// The storage limits the number of concurrent goroutines to prevent from out of memory
// errors and CPU trashing even if too many goroutines attempt to write.
//
// Defaults to 30s.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(s *storage) {
		s.writeTimeout = timeout
	}
}

// WithLogLevel specifies the log level to emit output.
//
// Defaults to a InfoLevel
func WithLogLevel(level log.Level) Option {
	return func(s *storage) {
		s.logger.SetLevel(level)
	}
}

// WithWALBufferedSize specifies the buffered byte size before flushing a WAL file.
// The larger the size, the less frequently the file is written and more write performance at the expense of durability.
// Giving 0 means it writes to a file whenever data point comes in.
// Giving -1 disables using WAL.
//
// Defaults to 4096.
func WithWALBufferedSize(size int) Option {
	return func(s *storage) {
		s.walBufferedSize = size
	}
}

// WithoutLkoStorage turns off the last known observation (LKO) storage feature. LKO storage is useful when the current
// or last known value of a metric is required to be known often. When enabled a separate space in memory is allocated
// to store the last known observation for every metric.
func WithoutLkoStorage() Option {
	return func(s *storage) {
		getLko().disabled = true
		getLko().clear()
	}
}

func WithLkoStorage() Option {
	return func(s *storage) {
		getLko().disabled = false
	}
}

func WithWalRecovery(option WalRecoveryOption) Option {
	return func(s *storage) {
		s.walRecoveryOption = option
	}
}
