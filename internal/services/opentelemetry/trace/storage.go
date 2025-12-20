package trace

import (
	"github.com/rs/zerolog/log"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"sort"
	"sync"
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
		timeIndex:      make([]*timeIndexEntry, 0),
		creationOrder:  make([]string, 0),
	}
}

// timeIndexEntry stores trace metadata for time-based queries
type timeIndexEntry struct {
	traceID   string
	projectID string
	flowID    string
	startTime time.Time
	endTime   time.Time
}

// Storage stores traces in memory with FIFO eviction based on memory limit
type Storage struct {
	traces         map[string]*Entry
	maxMemoryBytes int64
	mu             sync.RWMutex

	// Indexes for fast lookups
	projectIndex map[string][]string // projectID -> []traceID
	flowIndex    map[string][]string // flowID -> []traceID
	timeIndex    []*timeIndexEntry   // sorted by startTime for range queries
	timeIndexMu  sync.RWMutex

	// FIFO tracking - evicts oldest traces first
	creationOrder []string // traceIDs in creation order (oldest first)
	creationMu    sync.Mutex
}

// getCurrentMemoryUsage calculates total memory used by all traces
func (ts *Storage) getCurrentMemoryUsage() int64 {
	var total int64
	for _, entry := range ts.traces {
		total += int64(entry.EstimateSize())
	}
	return total
}

// evictOldest removes the oldest trace (FIFO eviction)
func (ts *Storage) evictOldest() bool {
	ts.creationMu.Lock()
	if len(ts.creationOrder) == 0 {
		ts.creationMu.Unlock()
		return false
	}

	// Get the oldest trace (first in list)
	oldestTraceID := ts.creationOrder[0]
	ts.creationOrder = ts.creationOrder[1:]
	ts.creationMu.Unlock()

	// Remove the trace from storage
	ts.mu.Lock()
	entry, exists := ts.traces[oldestTraceID]
	if exists {
		delete(ts.traces, oldestTraceID)

		// Remove from indexes
		ts.removeFromIndex(ts.projectIndex, entry.ProjectID, oldestTraceID)
		if entry.FlowID != "" {
			ts.removeFromIndex(ts.flowIndex, entry.FlowID, oldestTraceID)
		}

		log.Debug().
			Str("traceID", oldestTraceID).
			Int("spans", entry.SpansCount).
			Time("createdAt", entry.CreatedAt).
			Msg("evicted oldest trace due to memory limit")
	}
	ts.mu.Unlock()

	// Remove from time index
	if exists {
		ts.removeFromTimeIndex(oldestTraceID)
	}

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

// addToTimeIndex adds an entry to the time index (maintains sorted order)
func (ts *Storage) addToTimeIndex(entry *Entry) {
	ts.timeIndexMu.Lock()
	defer ts.timeIndexMu.Unlock()

	timeEntry := &timeIndexEntry{
		traceID:   entry.TraceID,
		projectID: entry.ProjectID,
		flowID:    entry.FlowID,
		startTime: entry.StartTime,
		endTime:   entry.EndTime,
	}

	ts.timeIndex = append(ts.timeIndex, timeEntry)

	// Keep sorted by startTime (newest first) for efficient range queries
	// Only sort if we've accumulated enough entries to make it worthwhile
	if len(ts.timeIndex)%100 == 0 {
		sort.Slice(ts.timeIndex, func(i, j int) bool {
			return ts.timeIndex[i].startTime.After(ts.timeIndex[j].startTime)
		})
	}
}

// removeFromTimeIndex removes an entry from the time index
func (ts *Storage) removeFromTimeIndex(traceID string) {
	ts.timeIndexMu.Lock()
	defer ts.timeIndexMu.Unlock()

	for i, entry := range ts.timeIndex {
		if entry.traceID == traceID {
			ts.timeIndex = append(ts.timeIndex[:i], ts.timeIndex[i+1:]...)
			break
		}
	}
}

// addOrUpdateTrace adds or updates a trace, with automatic FIFO eviction
// Returns true if this is a NEW trace (first time seeing it)
func (ts *Storage) addOrUpdateTrace(traceID string, stat *Stat) bool {
	// First, check if we need to evict without holding the main lock
	needsEviction := false
	isNewTrace := false

	ts.mu.RLock()
	if ts.getCurrentMemoryUsage() >= ts.maxMemoryBytes {
		needsEviction = true
	}
	exists := ts.traces[traceID] != nil
	isNewTrace = !exists
	ts.mu.RUnlock()

	// Evict oldest traces if needed (without holding main lock)
	if needsEviction && !exists {
		for {
			evicted := ts.evictOldest()
			if !evicted {
				log.Warn().Msg("memory limit reached but nothing to evict")
				break
			}

			// Check if we have enough space now
			ts.mu.RLock()
			currentUsage := ts.getCurrentMemoryUsage()
			ts.mu.RUnlock()

			if currentUsage < ts.maxMemoryBytes {
				break
			}
		}
	}

	// Now acquire write lock and perform the update
	ts.mu.Lock()
	defer ts.mu.Unlock()

	entry, entryExists := ts.traces[traceID]
	if !entryExists {
		entry = &Entry{
			TraceID:   traceID,
			FlowID:    stat.flowID,
			ProjectID: stat.projectID,
			Spans:     make([]*v1.Span, 0, len(stat.spans)),
			CreatedAt: time.Now(),
		}

		ts.traces[traceID] = entry

		// Add to indexes
		ts.addToIndex(ts.projectIndex, stat.projectID, traceID)
		if stat.flowID != "" {
			ts.addToIndex(ts.flowIndex, stat.flowID, traceID)
		}

		// Add to creation order for FIFO eviction
		ts.creationMu.Lock()
		ts.creationOrder = append(ts.creationOrder, traceID)
		ts.creationMu.Unlock()
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

	// Add to time index if new entry
	if !entryExists {
		log.Debug().
			Str("traceID", traceID).
			Time("startTime", entry.StartTime).
			Time("endTime", entry.EndTime).
			Str("projectID", entry.ProjectID).
			Str("flowID", entry.FlowID).
			Msg("new trace added to storage")

		ts.addToTimeIndex(entry)
	}

	return isNewTrace
}

// QueryTraces returns traces matching the given criteria with pagination
// Parameters:
//   - projectID: required project identifier
//   - flowID: optional flow identifier (empty string to match all flows)
//   - startTime: optional start time filter (zero value to ignore)
//   - endTime: optional end time filter (zero value to ignore)
//   - offset: number of traces to skip
//   - limit: maximum number of traces to return (max 1000)
func (ts *Storage) QueryTraces(projectID, flowID string, startTime, endTime time.Time, offset, limit int) []*Entry {
	if limit > 1000 {
		limit = 1000
	}
	if limit <= 0 {
		limit = 100
	}

	// First, ensure time index is sorted (newest first)
	ts.timeIndexMu.Lock()
	sort.Slice(ts.timeIndex, func(i, j int) bool {
		return ts.timeIndex[i].startTime.After(ts.timeIndex[j].startTime)
	})
	ts.timeIndexMu.Unlock()

	// Collect matching traceIDs from time index
	matchingIDs := make([]string, 0)

	ts.timeIndexMu.RLock()
	for _, timeEntry := range ts.timeIndex {
		// Filter by project
		if timeEntry.projectID != projectID {
			continue
		}

		// Filter by flow if specified
		if flowID != "" && timeEntry.flowID != flowID {
			continue
		}

		// Filter by time range if specified
		if !startTime.IsZero() && timeEntry.endTime.Before(startTime) {
			continue // Trace ended before query range
		}
		if !endTime.IsZero() && timeEntry.startTime.After(endTime) {
			continue // Trace started after query range
		}

		matchingIDs = append(matchingIDs, timeEntry.traceID)
	}
	ts.timeIndexMu.RUnlock()

	log.Debug().
		Str("projectID", projectID).
		Str("flowID", flowID).
		Time("queryStart", startTime).
		Time("queryEnd", endTime).
		Int("totalInIndex", len(ts.timeIndex)).
		Int("matchingCount", len(matchingIDs)).
		Int("offset", offset).
		Int("limit", limit).
		Msg("queryTraces executed")

	// Apply offset and limit
	if offset >= len(matchingIDs) {
		return []*Entry{}
	}

	end := offset + limit
	if end > len(matchingIDs) {
		end = len(matchingIDs)
	}

	selectedIDs := matchingIDs[offset:end]

	// Fetch actual entries
	result := make([]*Entry, 0, len(selectedIDs))
	ts.mu.RLock()
	for _, traceID := range selectedIDs {
		if entry, exists := ts.traces[traceID]; exists {
			result = append(result, entry)
		}
	}
	ts.mu.RUnlock()

	return result
}

// GetTraceByID retrieves a trace entry by its ID and project ID
// Returns nil if the trace is not found or doesn't belong to the specified project
func (ts *Storage) GetTraceByID(traceID, projectID string) *Entry {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	entry, exists := ts.traces[traceID]
	if !exists {
		return nil
	}

	// Verify the trace belongs to the requested project
	if entry.ProjectID != projectID {
		return nil
	}

	return entry
}

// GetStats returns storage statistics
func (ts *Storage) GetStats() StorageStats {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var totalSpans int
	var oldestTime time.Time

	for _, entry := range ts.traces {
		totalSpans += entry.SpansCount
		if oldestTime.IsZero() || entry.CreatedAt.Before(oldestTime) {
			oldestTime = entry.CreatedAt
		}
	}

	oldestDataMinutes := 0
	if !oldestTime.IsZero() {
		oldestDataMinutes = int(time.Since(oldestTime).Minutes())
	}

	return StorageStats{
		TracesCount:       len(ts.traces),
		SpansCount:        totalSpans,
		ProjectsCount:     len(ts.projectIndex),
		FlowsCount:        len(ts.flowIndex),
		MemoryUsageMB:     int(ts.getCurrentMemoryUsage() / 1024 / 1024),
		MaxMemoryMB:       int(ts.maxMemoryBytes / 1024 / 1024),
		OldestDataMinutes: oldestDataMinutes,
	}
}

type StorageStats struct {
	TracesCount       int
	SpansCount        int
	ProjectsCount     int
	FlowsCount        int
	MemoryUsageMB     int
	MaxMemoryMB       int
	OldestDataMinutes int
}
