package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// CircularBuffer stores time series data with fixed capacity
// When full, it overwrites the oldest data (FIFO)
type CircularBuffer struct {
	capacity int
	data     []DataPoint
	head     int // oldest data
	tail     int // next write position
	size     int
	mu       sync.RWMutex
}

// DataPoint represents a single metric observation
type DataPoint struct {
	Time   time.Time
	Value  float64
	Labels map[string]string
}

// NewCircularBuffer creates a buffer with fixed capacity (number of points)
func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		capacity: capacity,
		data:     make([]DataPoint, capacity),
		head:     0,
		tail:     0,
		size:     0,
	}
}

// Add inserts a new data point, evicting oldest if buffer is full
func (cb *CircularBuffer) Add(dp DataPoint) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.data[cb.tail] = dp
	cb.tail = (cb.tail + 1) % cb.capacity

	if cb.size < cb.capacity {
		cb.size++
	} else {
		// Buffer is full, move head forward (evict oldest)
		cb.head = (cb.head + 1) % cb.capacity
	}
}

// Query returns all points within the time range
func (cb *CircularBuffer) Query(start, end time.Time) []DataPoint {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	result := make([]DataPoint, 0, cb.size)

	for i := 0; i < cb.size; i++ {
		idx := (cb.head + i) % cb.capacity
		dp := cb.data[idx]

		if !dp.Time.Before(start) && !dp.Time.After(end) {
			result = append(result, dp)
		}
	}

	return result
}

// Size returns the current number of points in the buffer
func (cb *CircularBuffer) Size() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.size
}

// GetOldestTime returns the timestamp of the oldest point
func (cb *CircularBuffer) GetOldestTime() time.Time {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.size == 0 {
		return time.Time{}
	}
	return cb.data[cb.head].Time
}

// TimeSeriesBuffer manages multiple metrics with memory limits
type TimeSeriesBuffer struct {
	buffers map[MetricKey]*CircularBuffer
	mu      sync.RWMutex

	// Memory-based limits instead of time-based
	maxMemoryBytes  int64
	pointsPerMetric int

	// LRU tracking
	accessOrder []MetricKey
	accessMap   map[MetricKey]int // metricKey -> index in accessOrder
	accessMu    sync.Mutex
}

// NewTimeSeriesBuffer creates a buffer with memory-based limits
// maxMemoryMB: total memory budget in megabytes
// pointsPerMetric: how many points to keep per unique metric
func NewTimeSeriesBuffer(maxMemoryMB int, pointsPerMetric int) *TimeSeriesBuffer {
	return &TimeSeriesBuffer{
		buffers:         make(map[MetricKey]*CircularBuffer),
		maxMemoryBytes:  int64(maxMemoryMB) * 1024 * 1024,
		pointsPerMetric: pointsPerMetric,
		accessOrder:     make([]MetricKey, 0),
		accessMap:       make(map[MetricKey]int),
	}
}

func (tsb *TimeSeriesBuffer) trackAccess(key MetricKey) {
	tsb.accessMu.Lock()
	defer tsb.accessMu.Unlock()

	// Remove from current position if exists
	if idx, exists := tsb.accessMap[key]; exists {
		tsb.accessOrder = append(tsb.accessOrder[:idx], tsb.accessOrder[idx+1:]...)
		// Update indices for remaining keys
		for i := idx; i < len(tsb.accessOrder); i++ {
			tsb.accessMap[tsb.accessOrder[i]] = i
		}
	}

	// Add to end (most recently used)
	tsb.accessOrder = append(tsb.accessOrder, key)
	tsb.accessMap[key] = len(tsb.accessOrder) - 1
}

func (tsb *TimeSeriesBuffer) evictLRU() {
	tsb.accessMu.Lock()
	if len(tsb.accessOrder) == 0 {
		tsb.accessMu.Unlock()
		return
	}

	// Get least recently used metric
	lruKey := tsb.accessOrder[0]
	tsb.accessOrder = tsb.accessOrder[1:]
	delete(tsb.accessMap, lruKey)

	// Update indices
	for i := 0; i < len(tsb.accessOrder); i++ {
		tsb.accessMap[tsb.accessOrder[i]] = i
	}
	tsb.accessMu.Unlock()

	// Remove the buffer
	tsb.mu.Lock()
	delete(tsb.buffers, lruKey)
	tsb.mu.Unlock()
}

func (tsb *TimeSeriesBuffer) getCurrentMemoryUsage() int64 {
	tsb.mu.RLock()
	defer tsb.mu.RUnlock()

	// Estimate: each DataPoint ≈ 100 bytes (time=24, float64=8, map overhead≈68)
	totalPoints := 0
	for _, buffer := range tsb.buffers {
		totalPoints += buffer.Size()
	}

	return int64(totalPoints * 100)
}

func (tsb *TimeSeriesBuffer) getOrCreateBuffer(key MetricKey) *CircularBuffer {
	tsb.mu.RLock()
	buffer, exists := tsb.buffers[key]
	tsb.mu.RUnlock()

	if exists {
		tsb.trackAccess(key)
		return buffer
	}

	// Check if we need to evict before creating new buffer
	for tsb.getCurrentMemoryUsage() >= tsb.maxMemoryBytes {
		tsb.evictLRU()
	}

	tsb.mu.Lock()
	// Double-check after acquiring write lock
	if buffer, exists = tsb.buffers[key]; exists {
		tsb.mu.Unlock()
		tsb.trackAccess(key)
		return buffer
	}

	buffer = NewCircularBuffer(tsb.pointsPerMetric)
	tsb.buffers[key] = buffer
	tsb.mu.Unlock()

	tsb.trackAccess(key)
	return buffer
}

// Storage implementation using memory-limited circular buffers
type Storage struct {
	log      zerolog.Logger
	tsBuffer *TimeSeriesBuffer
}

// NewStorage creates storage with memory-based limits
// maxMemoryMB: total memory budget (e.g., 1024 for 1GB)
// pointsPerMetric: points to keep per unique metric (e.g., 3600 = 1 hour at 1/sec)
func NewStorage(maxMemoryMB int, pointsPerMetric int) *Storage {
	return &Storage{
		tsBuffer: NewTimeSeriesBuffer(maxMemoryMB, pointsPerMetric),
	}
}

func (s *Storage) SaveDataPoints(_ context.Context, dataPoints []*Datapoint) error {

	for _, d := range dataPoints {
		mk := MetricKey{
			Metric:    d.Metric,
			FlowID:    d.Attrs["flowID"],
			ProjectID: d.Attrs["projectID"],
			AttrHash:  d.AttrsHash,
		}

		var value float64
		switch d.Instrument {
		case InstrumentCounter:
			value = d.Sum
		case InstrumentAdditive:
			value = d.Gauge
		case InstrumentGauge:
			value = d.Gauge
		default:
			continue
		}

		buffer := s.tsBuffer.getOrCreateBuffer(mk)
		buffer.Add(DataPoint{
			Time:   d.Time,
			Value:  value,
			Labels: d.Attrs,
		})
	}

	return nil
}

// QueryMetrics retrieves data points for a metric within time range
func (s *Storage) QueryMetrics(ctx context.Context, mk MetricKey, start, end time.Time) ([]DataPoint, error) {
	s.tsBuffer.mu.RLock()
	buffer, exists := s.tsBuffer.buffers[mk]
	s.tsBuffer.mu.RUnlock()

	if !exists {
		return []DataPoint{}, nil
	}

	return buffer.Query(start, end), nil
}

// GetMemoryUsage returns current memory usage in bytes
func (s *Storage) GetMemoryUsage() int64 {
	return s.tsBuffer.getCurrentMemoryUsage()
}

// GetMetricsCount returns number of unique metrics being tracked
func (s *Storage) GetMetricsCount() int {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()
	return len(s.tsBuffer.buffers)
}

// GetStats returns detailed storage statistics
func (s *Storage) GetStats() StorageStats {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	stats := StorageStats{
		MetricsCount:    len(s.tsBuffer.buffers),
		MemoryUsageMB:   int(s.tsBuffer.getCurrentMemoryUsage() / 1024 / 1024),
		MaxMemoryMB:     int(s.tsBuffer.maxMemoryBytes / 1024 / 1024),
		PointsPerMetric: s.tsBuffer.pointsPerMetric,
	}

	var totalPoints, oldestDataAge int
	var oldestTime time.Time

	for _, buffer := range s.tsBuffer.buffers {
		totalPoints += buffer.Size()
		bufferOldest := buffer.GetOldestTime()
		if !bufferOldest.IsZero() && (oldestTime.IsZero() || bufferOldest.Before(oldestTime)) {
			oldestTime = bufferOldest
		}
	}

	if !oldestTime.IsZero() {
		oldestDataAge = int(time.Since(oldestTime).Minutes())
	}

	stats.TotalDataPoints = totalPoints
	stats.OldestDataMinutes = oldestDataAge

	return stats
}

type StorageStats struct {
	MetricsCount      int
	TotalDataPoints   int
	MemoryUsageMB     int
	MaxMemoryMB       int
	PointsPerMetric   int
	OldestDataMinutes int // How old is the oldest data
}

// Usage example:
// storage := NewStorage(flowRepo, 1024, 3600)
// This creates storage with:
// - 1GB memory limit
// - 3600 points per metric (1 hour at 1 sample/sec)
// - Automatic LRU eviction when memory is full
// - Keeps most recently accessed metrics in memory
