package tstorage

// Row includes a data point along with properties to identify a kind of metrics.
type Row struct {
	// The unique name of metric.
	// This field must be set.
	Metric uint32
	// This field must be set.
	DataPoint
}
