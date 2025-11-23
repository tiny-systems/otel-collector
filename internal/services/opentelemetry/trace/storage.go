package trace

import (
	"github.com/rs/zerolog/log"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"time"
)

// NewTraceStorage creates storage with memory-based limits
// maxMemoryMB: total memory budget in megabytes
func NewTraceStorage(maxMemoryMB int) *Storage {
	return &Storage{
		traces:         make(map[string]*Entry),
		maxMemoryBytes: int64(maxMemoryMB) * 1024 * 1024,
		projectIndex:   make(map[string][]string),
		flowIndex:      make(map[string][]string),
		accessOrder:    make([]string, 0),
		accessMap:      make(map[string]int),
	}
}

// trackAccess updates LRU ordering when a trace is accessed
func (ts *Storage) trackAccess(traceID string) {
	ts.accessMu.Lock()
	defer ts.accessMu.Unlock()

	// Remove from current position if exists
	if idx, exists := ts.accessMap[traceID]; exists {
		ts.accessOrder = append(ts.accessOrder[:idx], ts.accessOrder[idx+1:]...)
		// Update indices for remaining keys
		for i := idx; i < len(ts.accessOrder); i++ {
			ts.accessMap[ts.accessOrder[i]] = i
		}
	}

	// Add to end (most recently used)
	ts.accessOrder = append(ts.accessOrder, traceID)
	ts.accessMap[traceID] = len(ts.accessOrder) - 1
}

// getCurrentMemoryUsage calculates total memory used by all traces
func (ts *Storage) getCurrentMemoryUsage() int64 {
	var total int64
	for _, entry := range ts.traces {
		total += int64(entry.EstimateSize())
	}
	return total
}

// evictLRU removes the least recently used trace
func (ts *Storage) evictLRU() bool {
	ts.accessMu.Lock()
	if len(ts.accessOrder) == 0 {
		ts.accessMu.Unlock()
		return false
	}

	// Get least recently used trace (first in list)
	lruTraceID := ts.accessOrder[0]
	ts.accessOrder = ts.accessOrder[1:]
	delete(ts.accessMap, lruTraceID)

	// Update indices for remaining traces
	for i := 0; i < len(ts.accessOrder); i++ {
		ts.accessMap[ts.accessOrder[i]] = i
	}
	ts.accessMu.Unlock()

	// Remove the trace from storage
	ts.mu.Lock()
	entry, exists := ts.traces[lruTraceID]
	if exists {
		delete(ts.traces, lruTraceID)

		// Remove from indexes
		ts.removeFromIndex(ts.projectIndex, entry.ProjectID, lruTraceID)
		if entry.FlowID != "" {
			ts.removeFromIndex(ts.flowIndex, entry.FlowID, lruTraceID)
		}

		log.Debug().
			Str("traceID", lruTraceID).
			Int("spans", entry.SpansCount).
			Msg("evicted trace due to memory limit")
	}
	ts.mu.Unlock()

	return exists
}

// removeFromIndex removes a traceID from an index
func (ts *Storage) removeFromIndex(index map[string][]string, key, traceID string) {
	traces := index[key]
	for i, id := range traces {
		if id == traceID {
			// Swap with last element and truncate
			traces[i] = traces[len(traces)-1]
			index[key] = traces[:len(traces)-1]
			break
		}
	}

	// Clean up empty index entries
	if len(index[key]) == 0 {
		delete(index, key)
	}
}

// addToIndex adds a traceID to an index
func (ts *Storage) addToIndex(index map[string][]string, key, traceID string) {
	index[key] = append(index[key], traceID)
}

// addOrUpdateTrace adds or updates a trace, with automatic LRU eviction
func (ts *Storage) addOrUpdateTrace(traceID string, stat *Stat) {
	ts.mu.Lock()

	// Check memory and evict LRU traces if needed
	for ts.getCurrentMemoryUsage() >= ts.maxMemoryBytes {
		ts.mu.Unlock()
		evicted := ts.evictLRU()
		if !evicted {
			// Nothing left to evict, we're at minimum
			log.Warn().Msg("memory limit reached but nothing to evict")
			break
		}
		ts.mu.Lock()
	}

	entry, exists := ts.traces[traceID]
	if !exists {
		entry = &Entry{
			TraceID:   traceID,
			FlowID:    stat.flowID,
			ProjectID: stat.projectID,
			Spans:     make([]*v1.Span, 0),
			CreatedAt: time.Now(),
		}
		ts.traces[traceID] = entry

		// Add to indexes
		ts.addToIndex(ts.projectIndex, stat.projectID, traceID)
		if stat.flowID != "" {
			ts.addToIndex(ts.flowIndex, stat.flowID, traceID)
		}
	}

	// Update trace data
	entry.Spans = append(entry.Spans, stat.spans...)
	entry.StartTime = time.Unix(0, int64(stat.StartTimeMin))
	entry.EndTime = time.Unix(0, int64(stat.EndTimeMax))
	entry.SpansCount += len(stat.spans)
	entry.ErrorsCount += stat.ErrorCounter
	entry.DataCount += stat.DataCounter
	entry.DataLength += stat.DataLength
	if stat.LongestSpan > entry.DurationNs {
		entry.DurationNs = stat.LongestSpan
	}
	entry.LastAccess = time.Now()

	ts.mu.Unlock()

	// Track access for LRU
	ts.trackAccess(traceID)
}
