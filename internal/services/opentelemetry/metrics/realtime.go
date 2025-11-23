package metrics

import (
	"context"
	"github.com/tiny-systems/otel-server/internal/services/opentelemetry"
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
	FlowID    string
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

	// Simple limit for memory protection
	maxAggregates int

	// Statistics
	totalRejected   int64 // New: tracks rejected metrics when at capacity
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
	subKey := SubscriptionKey{
		ProjectID: projectID,
		FlowID:    flowID,
	}

	ra.subMu.RLock()
	_, hasSubscription := ra.subscribers[subKey]
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
	}

	// Aggregate the datapoint
	key := AggregationKey{
		Metric:    dp.Metric,
		ProjectID: projectID,
		FlowID:    flowID,
	}

	agg, exists := ra.currentAggregates[key]
	if !exists {
		// Check if we've hit the limit for new metrics in this window
		if len(ra.currentAggregates) >= ra.maxAggregates {
			ra.totalRejected++
			return // Reject new metric to stay within memory bounds
		}

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

	// Only send metrics for subscriptions that exist
	ra.subMu.RLock()
	activeSubscriptions := make(map[SubscriptionKey]bool)
	for subKey := range ra.subscribers {
		activeSubscriptions[subKey] = true
	}
	ra.subMu.RUnlock()

	// Group by subscription key - only for active subscriptions
	metrics := make(map[SubscriptionKey][]AggregatedMetric)

	for key, agg := range ra.currentAggregates {
		subKey := SubscriptionKey{
			ProjectID: key.ProjectID,
			FlowID:    key.FlowID,
		}

		// Skip if no active subscription for this key
		if !activeSubscriptions[subKey] {
			continue
		}

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

		metrics[subKey] = append(metrics[subKey], metric)
	}

	// Send to subscribers
	ra.subMu.RLock()
	for subKey, metricList := range metrics {
		if subs, exists := ra.subscribers[subKey]; exists {
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
	ra.subMu.RUnlock()

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
	ra.subscribers[sub.Key] = append(ra.subscribers[sub.Key], sub)
	ra.subMu.Unlock()

	return sub
}

// Unsubscribe removes a subscription
func (ra *RealtimeAggregator) Unsubscribe(sub *Subscriber) {
	sub.cancel()

	ra.subMu.Lock()
	subs := ra.subscribers[sub.Key]
	for i, s := range subs {
		if s == sub {
			ra.subscribers[sub.Key] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	// Check if this was the last subscriber for this key
	lastSubscriber := len(ra.subscribers[sub.Key]) == 0
	if lastSubscriber {
		delete(ra.subscribers, sub.Key)
	}
	ra.subMu.Unlock()

	// If no more subscribers for this key, clean up aggregates immediately
	if lastSubscriber {
		ra.mu.Lock()
		// Remove all aggregates for this subscription key
		for aggKey := range ra.currentAggregates {
			if aggKey.ProjectID == sub.Key.ProjectID && aggKey.FlowID == sub.Key.FlowID {
				delete(ra.currentAggregates, aggKey)
			}
		}
		ra.mu.Unlock()
	}

	close(sub.Ch)
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
	totalRejected := ra.totalRejected
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
		TotalRejected:       totalRejected,
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
	TotalRejected       int64
	TotalAggregated     int64
	TotalFlushed        int64
	DroppedNoSub        int64
	DroppedFullChan     int64
}
