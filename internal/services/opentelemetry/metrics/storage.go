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

// fifoNode represents a node in the FIFO queue
type fifoNode struct {
	key       MetricKey
	createdAt time.Time
}

// TimeSeriesBuffer manages multiple metrics with memory limits using FIFO eviction
type TimeSeriesBuffer struct {
	buffers map[MetricKey]*CircularBuffer
	mu      sync.RWMutex

	// Memory-based limits
	maxMemoryBytes  int64
	pointsPerMetric int

	// FIFO tracking - evicts oldest created metrics
	fifoQueue []fifoNode
	fifoIndex map[MetricKey]int // maps key to its index in fifoQueue
	fifoMu    sync.Mutex
}

// NewTimeSeriesBuffer creates a buffer with memory-based limits using FIFO eviction
// maxMemoryMB: total memory budget in megabytes
// pointsPerMetric: how many points to keep per unique metric
func NewTimeSeriesBuffer(maxMemoryMB int, pointsPerMetric int) *TimeSeriesBuffer {
	return &TimeSeriesBuffer{
		buffers:         make(map[MetricKey]*CircularBuffer),
		maxMemoryBytes:  int64(maxMemoryMB) * 1024 * 1024,
		pointsPerMetric: pointsPerMetric,
		fifoQueue:       make([]fifoNode, 0),
		fifoIndex:       make(map[MetricKey]int),
	}
}

func (tsb *TimeSeriesBuffer) addToFIFO(key MetricKey) {
	tsb.fifoMu.Lock()
	defer tsb.fifoMu.Unlock()

	// Check if already exists
	if _, exists := tsb.fifoIndex[key]; exists {
		return
	}

	// Add to end of queue
	node := fifoNode{
		key:       key,
		createdAt: time.Now(),
	}
	tsb.fifoQueue = append(tsb.fifoQueue, node)
	tsb.fifoIndex[key] = len(tsb.fifoQueue) - 1
}

func (tsb *TimeSeriesBuffer) evictFIFO() {
	tsb.fifoMu.Lock()
	if len(tsb.fifoQueue) == 0 {
		tsb.fifoMu.Unlock()
		return
	}

	// Get oldest (first in queue)
	oldestNode := tsb.fifoQueue[0]
	oldestKey := oldestNode.key

	// Remove from queue
	tsb.fifoQueue = tsb.fifoQueue[1:]
	delete(tsb.fifoIndex, oldestKey)

	// Rebuild index after removal
	for i := range tsb.fifoQueue {
		tsb.fifoIndex[tsb.fifoQueue[i].key] = i
	}
	tsb.fifoMu.Unlock()

	// Remove the buffer
	tsb.mu.Lock()
	delete(tsb.buffers, oldestKey)
	tsb.mu.Unlock()
}

func (tsb *TimeSeriesBuffer) getCurrentMemoryUsage() int64 {
	tsb.mu.RLock()
	defer tsb.mu.RUnlock()

	// Estimate memory usage more accurately
	var totalBytes int64
	for key, buffer := range tsb.buffers {
		// Base overhead per buffer
		totalBytes += 200 // struct overhead

		// Data points
		pointSize := int64(buffer.Size())
		// Each DataPoint: time (24 bytes) + float64 (8 bytes) + map overhead (~100 bytes average)
		totalBytes += pointSize * 132

		// Labels in map - estimate based on key
		// Rough estimate: metric name + flowID + projectID strings
		totalBytes += int64(len(key.Metric) + len(key.FlowID) + len(key.ProjectID))
	}

	return totalBytes
}

func (tsb *TimeSeriesBuffer) getOrCreateBuffer(key MetricKey) *CircularBuffer {
	tsb.mu.RLock()
	buffer, exists := tsb.buffers[key]
	tsb.mu.RUnlock()

	if exists {
		return buffer
	}

	// Check if we need to evict before creating new buffer
	for tsb.getCurrentMemoryUsage() >= tsb.maxMemoryBytes {
		tsb.evictFIFO()
	}

	tsb.mu.Lock()
	// Double-check after acquiring write lock
	if buffer, exists = tsb.buffers[key]; exists {
		tsb.mu.Unlock()
		return buffer
	}

	buffer = NewCircularBuffer(tsb.pointsPerMetric)
	tsb.buffers[key] = buffer
	tsb.mu.Unlock()

	tsb.addToFIFO(key)
	return buffer
}

// Storage implementation using memory-limited circular buffers with FIFO eviction
type Storage struct {
	log      zerolog.Logger
	tsBuffer *TimeSeriesBuffer
}

// NewStorage creates storage with memory-based limits using FIFO eviction
// maxMemoryMB: total memory budget (e.g., 1024 for 1GB)
// pointsPerMetric: points to keep per unique metric (e.g., 3600 = 1 hour at 1/sec)
func NewStorage(maxMemoryMB int, pointsPerMetric int) *Storage {
	return &Storage{
		tsBuffer: NewTimeSeriesBuffer(maxMemoryMB, pointsPerMetric),
	}
}

func (s *Storage) SaveDataPoints(ctx context.Context, dataPoints []*Datapoint) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
// storage := NewStorage(1024, 3600)
// This creates storage with:
// - 1GB memory limit (estimated)
// - 3600 points per metric (1 hour at 1 sample/sec)
// - Automatic FIFO eviction when memory limit is approached
// - Maintains timeline consistency by evicting oldest metrics first
