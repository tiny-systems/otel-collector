package metrics

import (
	"context"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry"
	"sync"
	"time"
)

// AggregatedMetric represents a 1-second aggregated metric value
type AggregatedMetric struct {
	Metric    string
	ProjectID string
	FlowID    string
	Timestamp time.Time
	Value     float64
}

// SubscriptionKey uniquely identifies a subscription
type SubscriptionKey struct {
	ProjectID string
	FlowID    string // Empty string means "all flows for this project"
}

func (sk SubscriptionKey) matchesDatapoint(projectID, flowID string) bool {
	if sk.ProjectID != projectID {
		return false
	}
	// If subscription FlowID is empty, match all flows
	if sk.FlowID == "" {
		return true
	}
	// Otherwise must match exactly
	return sk.FlowID == flowID
}

// Subscriber receives aggregated metrics
type Subscriber struct {
	Key    SubscriptionKey
	Ch     chan AggregatedMetric
	ctx    context.Context
	cancel context.CancelFunc
}

// RealtimeAggregator aggregates metrics over 1-second windows and distributes to subscribers
type RealtimeAggregator struct {
	// Current aggregation window
	currentWindow     time.Time
	windowDuration    time.Duration
	currentAggregates map[AggregationKey]*MetricAggregate

	// Subscribers
	subscribers map[SubscriptionKey][]*Subscriber
	subMu       sync.RWMutex

	// Metrics to track
	trackedMetrics map[string]struct{}

	// Simple limit for cardinality protection
	maxAggregates int

	// Statistics
	totalEvictions  int64
	totalAggregated int64
	totalFlushed    int64
	droppedNoSub    int64
	droppedFullChan int64

	mu sync.Mutex
}

type AggregationKey struct {
	Metric    string
	ProjectID string
	FlowID    string
}

type MetricAggregate struct {
	Count      int
	Sum        float64
	Min        float64
	Max        float64
	Last       float64
	LastUpdate time.Time
}

func NewRealtimeAggregator() *RealtimeAggregator {
	ra := &RealtimeAggregator{
		currentWindow:     time.Now().Truncate(time.Second),
		windowDuration:    time.Second,
		currentAggregates: make(map[AggregationKey]*MetricAggregate),
		subscribers:       make(map[SubscriptionKey][]*Subscriber),
		trackedMetrics: map[string]struct{}{
			opentelemetry.MetricSpanErrorCount: {},
			opentelemetry.MetricTraceCount:     {},
		},
		maxAggregates: 10000, // Limit to 10k unique metric combinations per window
	}
	return ra
}

// AddDatapoint processes a datapoint and aggregates it
func (ra *RealtimeAggregator) AddDatapoint(dp *Datapoint) {
	// Only track specific metrics
	if _, tracked := ra.trackedMetrics[dp.Metric]; !tracked {
		return
	}

	projectID := dp.Attrs["projectID"]
	flowID := dp.Attrs["flowID"]

	// Skip if no project/flow context
	if projectID == "" || flowID == "" {
		return
	}

	// Check if there's an active subscription for this project/flow
	// Check both specific flow subscription AND project-level subscription
	hasSubscription := false

	ra.subMu.RLock()
	// Check for exact match (project + flow)
	exactKey := SubscriptionKey{ProjectID: projectID, FlowID: flowID}
	_, hasSubscription = ra.subscribers[exactKey]

	// Check for project-level subscription (empty flowID)
	if !hasSubscription {
		projectKey := SubscriptionKey{ProjectID: projectID, FlowID: ""}
		_, hasSubscription = ra.subscribers[projectKey]
	}
	ra.subMu.RUnlock()

	// Skip aggregation if no one is subscribed to this project/flow
	if !hasSubscription {
		return
	}

	ra.mu.Lock()
	defer ra.mu.Unlock()

	// Check if we need to flush the current window
	windowTime := dp.Time.Truncate(ra.windowDuration)
	if windowTime.After(ra.currentWindow) {
		// Flush current window
		ra.flushWindow()
		ra.currentWindow = windowTime
		// Clear aggregates for new window
		ra.currentAggregates = make(map[AggregationKey]*MetricAggregate)
	}

	// Aggregate the datapoint
	key := AggregationKey{
		Metric:    dp.Metric,
		ProjectID: projectID,
		FlowID:    flowID,
	}

	// Simple cardinality protection: reject if at limit and new key
	if len(ra.currentAggregates) >= ra.maxAggregates {
		if _, exists := ra.currentAggregates[key]; !exists {
			// At limit and this is a new key - reject it
			ra.totalEvictions++
			return
		}
	}

	agg, exists := ra.currentAggregates[key]
	if !exists {
		agg = &MetricAggregate{
			Min: dp.Sum,
			Max: dp.Sum,
		}
		ra.currentAggregates[key] = agg
	}

	// Update aggregate based on instrument type
	var value float64
	switch dp.Instrument {
	case InstrumentCounter:
		value = dp.Sum
		agg.Sum += value
	case InstrumentGauge:
		value = dp.Gauge
		agg.Last = value
		if value < agg.Min {
			agg.Min = value
		}
		if value > agg.Max {
			agg.Max = value
		}
	case InstrumentAdditive:
		value = dp.Gauge
		agg.Sum += value
	default:
		return
	}

	agg.Count++
	agg.LastUpdate = dp.Time
	ra.totalAggregated++
}

// flushWindow sends aggregated metrics to subscribers
func (ra *RealtimeAggregator) flushWindow() {
	if len(ra.currentAggregates) == 0 {
		return
	}

	// Get all active subscriptions
	ra.subMu.RLock()
	activeSubscriptions := make(map[SubscriptionKey][]*Subscriber)
	for subKey, subs := range ra.subscribers {
		activeSubscriptions[subKey] = subs
	}
	ra.subMu.RUnlock()

	// Group metrics by what subscribers need
	// We need to handle both specific flow subscriptions and project-level subscriptions
	metricsToSend := make(map[SubscriptionKey][]AggregatedMetric)

	for key, agg := range ra.currentAggregates {
		// Calculate final value based on metric type
		var finalValue float64
		if key.Metric == opentelemetry.MetricTraceCount || key.Metric == opentelemetry.MetricSpanErrorCount {
			// For counters, use sum
			finalValue = agg.Sum
		} else {
			// For gauges, use last value
			finalValue = agg.Last
		}

		metric := AggregatedMetric{
			Metric:    key.Metric,
			ProjectID: key.ProjectID,
			FlowID:    key.FlowID,
			Timestamp: ra.currentWindow,
			Value:     finalValue,
		}

		// Send to all matching subscriptions
		for subKey := range activeSubscriptions {
			if subKey.matchesDatapoint(key.ProjectID, key.FlowID) {
				metricsToSend[subKey] = append(metricsToSend[subKey], metric)
			}
		}
	}

	// Send to subscribers
	for subKey, metricList := range metricsToSend {
		if subs, exists := activeSubscriptions[subKey]; exists {
			for _, sub := range subs {
				for _, metric := range metricList {
					select {
					case sub.Ch <- metric:
						ra.totalFlushed++
					case <-sub.ctx.Done():
						// Subscriber cancelled, will be cleaned up
					default:
						// Channel full, skip (non-blocking)
						ra.droppedFullChan++
					}
				}
			}
		}
	}

	// Clear aggregates for next window
	ra.currentAggregates = make(map[AggregationKey]*MetricAggregate)
}

// Subscribe creates a new subscription for a project/flow combination
func (ra *RealtimeAggregator) Subscribe(ctx context.Context, projectID, flowID string) *Subscriber {
	subCtx, cancel := context.WithCancel(ctx)

	sub := &Subscriber{
		Key: SubscriptionKey{
			ProjectID: projectID,
			FlowID:    flowID,
		},
		Ch:     make(chan AggregatedMetric, 100),
		ctx:    subCtx,
		cancel: cancel,
	}

	ra.subMu.Lock()
	defer ra.subMu.Unlock()
	ra.subscribers[sub.Key] = append(ra.subscribers[sub.Key], sub)
	return sub
}

// Unsubscribe removes a subscription
func (ra *RealtimeAggregator) Unsubscribe(sub *Subscriber) {
	sub.cancel()
	defer close(sub.Ch)

	ra.subMu.Lock()
	subs := ra.subscribers[sub.Key]
	for i, s := range subs {
		if s == sub {
			ra.subscribers[sub.Key] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	lastSubscriber := len(ra.subscribers[sub.Key]) == 0
	if lastSubscriber {
		delete(ra.subscribers, sub.Key)
	}
	ra.subMu.Unlock()

	if !lastSubscriber {
		return
	}

	// Check if cleanup is needed - MUST check BEFORE acquiring mu lock
	ra.subMu.RLock()
	needsCleanup := true
	if sub.Key.FlowID == "" {
		for subKey := range ra.subscribers {
			if subKey.ProjectID == sub.Key.ProjectID {
				needsCleanup = false
				break
			}
		}
	} else {
		projectKey := SubscriptionKey{ProjectID: sub.Key.ProjectID, FlowID: ""}
		if _, exists := ra.subscribers[projectKey]; exists {
			needsCleanup = false
		}
	}
	ra.subMu.RUnlock()

	if !needsCleanup {
		return
	}

	// Now safe to acquire main lock
	ra.mu.Lock()
	defer ra.mu.Unlock()

	// Remove aggregates
	for aggKey := range ra.currentAggregates {
		shouldRemove := false
		if sub.Key.FlowID == "" {
			shouldRemove = aggKey.ProjectID == sub.Key.ProjectID
		} else {
			shouldRemove = aggKey.ProjectID == sub.Key.ProjectID &&
				aggKey.FlowID == sub.Key.FlowID
		}
		if shouldRemove {
			delete(ra.currentAggregates, aggKey)
		}
	}
}

// FlushLoop periodically flushes the aggregation window
func (ra *RealtimeAggregator) FlushLoop(ctx context.Context) {
	ticker := time.NewTicker(ra.windowDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ra.mu.Lock()
			ra.flushWindow()
			ra.currentWindow = time.Now().Truncate(ra.windowDuration)
			// Clear aggregates for new window
			ra.currentAggregates = make(map[AggregationKey]*MetricAggregate)
			ra.mu.Unlock()
		case <-ctx.Done():
			// Final flush
			ra.mu.Lock()
			ra.flushWindow()
			ra.mu.Unlock()
			return
		}
	}
}

// GetSubscriberCount returns the number of active subscribers
func (ra *RealtimeAggregator) GetSubscriberCount() int {
	ra.subMu.RLock()
	defer ra.subMu.RUnlock()

	count := 0
	for _, subs := range ra.subscribers {
		count += len(subs)
	}
	return count
}

// GetStats returns statistics about the aggregator
func (ra *RealtimeAggregator) GetStats() RealtimeStats {
	ra.mu.Lock()
	aggregateCount := len(ra.currentAggregates)
	totalEvictions := ra.totalEvictions
	totalAggregated := ra.totalAggregated
	totalFlushed := ra.totalFlushed
	droppedNoSub := ra.droppedNoSub
	droppedFullChan := ra.droppedFullChan
	ra.mu.Unlock()

	ra.subMu.RLock()
	subscriberCount := 0
	uniqueKeys := len(ra.subscribers)
	for _, subs := range ra.subscribers {
		subscriberCount += len(subs)
	}
	ra.subMu.RUnlock()

	return RealtimeStats{
		SubscriberCount:     subscriberCount,
		UniqueSubscriptions: uniqueKeys,
		CurrentAggregates:   aggregateCount,
		MaxAggregates:       ra.maxAggregates,
		WindowDurationMs:    int(ra.windowDuration.Milliseconds()),
		TrackedMetricsCount: len(ra.trackedMetrics),
		TotalEvictions:      totalEvictions,
		TotalAggregated:     totalAggregated,
		TotalFlushed:        totalFlushed,
		DroppedNoSub:        droppedNoSub,
		DroppedFullChan:     droppedFullChan,
	}
}

type RealtimeStats struct {
	SubscriberCount     int
	UniqueSubscriptions int
	CurrentAggregates   int
	MaxAggregates       int
	WindowDurationMs    int
	TrackedMetricsCount int
	TotalEvictions      int64
	TotalAggregated     int64
	TotalFlushed        int64
	DroppedNoSub        int64
	DroppedFullChan     int64
}
