package metrics

import (
	"context"
	"github.com/rs/zerolog/log"
	"reflect"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/exp/slices"
)

type Handler func(context.Context, []*Datapoint) error

type AttrMap map[string]string

func (m AttrMap) Merge(other AttrMap) {
	for k, v := range other {
		m[k] = v
	}
}

type Datapoint struct {
	Metric      string
	Description string
	Unit        string
	Instrument  Instrument

	Time      time.Time
	AttrsHash uint64

	Min   float64
	Max   float64
	Sum   float64
	Count uint64
	Gauge float64

	Attrs        AttrMap
	StringKeys   []string
	StringValues []string

	StartTimeUnixNano uint64
	CumPoint          any
}

type DatapointProcessor struct {
	batchSize int
	dropAttrs map[string]struct{}

	queue chan *Datapoint

	// Worker pool
	workers   int
	workerWg  sync.WaitGroup
	workQueue chan []*Datapoint

	c2d *CumToDeltaConv

	handler Handler

	// Realtime aggregator for streaming
	realtimeAggregator *RealtimeAggregator

	// Metrics
	droppedPoints    int64
	outOfOrderPoints int64
	processedPoints  int64
	mu               sync.Mutex
}

func NewDatapointProcessor(handler Handler) *DatapointProcessor {
	workers := 4 // configurable

	p := &DatapointProcessor{
		batchSize:          100,
		queue:              make(chan *Datapoint, 100000),
		workers:            workers,
		workQueue:          make(chan []*Datapoint, workers*2),
		c2d:                NewCumToDeltaConv(100000),
		handler:            handler,
		realtimeAggregator: NewRealtimeAggregator(),
	}

	p.dropAttrs = map[string]struct{}{
		"telemetry_sdk_language": {},
		"telemetry_sdk_name":     {},
		"telemetry_sdk_version":  {},
	}

	// Start worker pool
	for i := 0; i < workers; i++ {
		p.workerWg.Add(1)
		go p.worker()
	}

	return p
}

func (p *DatapointProcessor) worker() {
	defer p.workerWg.Done()

	for datapoints := range p.workQueue {
		p._processDataPoints(newDatapointContext(context.Background()), datapoints)
	}
}

func (p *DatapointProcessor) AddDatapoint(_ context.Context, datapoint *Datapoint) {
	select {
	case p.queue <- datapoint:
	default:
		p.mu.Lock()
		p.droppedPoints++
		p.mu.Unlock()
		log.Error().Msgf("datapoint buffer is full (consider increasing metrics.buffer_size) queue_len=%d", len(p.queue))
	}
}

func (p *DatapointProcessor) ProcessLoop(ctx context.Context) {
	const timeout = time.Millisecond * 500

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	dataPoints := make([]*Datapoint, 0, p.batchSize)

loop:
	for {
		select {
		case datapoint := <-p.queue:
			dataPoints = append(dataPoints, datapoint)
			if len(dataPoints) < p.batchSize {
				break
			}

			p.processDataPoints(ctx, dataPoints)
			// reset
			dataPoints = dataPoints[:0]

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(timeout)
		case <-timer.C:
			if len(dataPoints) > 0 {
				p.processDataPoints(ctx, dataPoints)
				// reset
				dataPoints = dataPoints[:0]
			}
			timer.Reset(timeout)
		case <-ctx.Done():
			break loop
		}
	}

	// final dataPoints
	if len(dataPoints) > 0 {
		p.processDataPoints(ctx, dataPoints)
	}

	// Shutdown worker pool
	close(p.workQueue)
	p.workerWg.Wait()
}

func (p *DatapointProcessor) processDataPoints(ctx context.Context, src []*Datapoint) {
	// Check context before processing
	select {
	case <-ctx.Done():
		return
	default:
	}

	dataPoints := make([]*Datapoint, len(src))
	copy(dataPoints, src)

	select {
	case p.workQueue <- dataPoints:
	default:
		// If work queue is full, process synchronously to avoid dropping data
		p._processDataPoints(newDatapointContext(ctx), dataPoints)
	}
}

func (p *DatapointProcessor) _processDataPoints(ctx *datapointContext, datapoints []*Datapoint) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return
	default:
	}

	for i := len(datapoints) - 1; i >= 0; i-- {
		dp := datapoints[i]

		p.initDatapoint(ctx, dp)

		// Feed to realtime aggregator BEFORE cumToDelta conversion
		// This ensures we capture the original cumulative values
		p.realtimeAggregator.AddDatapoint(dp)

		if !p.cumToDelta(ctx, dp) {
			datapoints = append(datapoints[:i], datapoints[i+1:]...)
			p.mu.Lock()
			p.droppedPoints++
			p.mu.Unlock()
			continue
		}
	}

	if len(datapoints) == 0 {
		return
	}

	p.mu.Lock()
	p.processedPoints += int64(len(datapoints))
	p.mu.Unlock()

	if err := p.handler(ctx, datapoints); err != nil {
		log.Warn().Msgf("unable to handle datapoints: %v", err)
	}
}

func (p *DatapointProcessor) cumToDelta(ctx *datapointContext, datapoint *Datapoint) bool {
	switch point := datapoint.CumPoint.(type) {
	case nil:
		return true
	case *NumberPoint:
		if !p.convertNumberPoint(ctx, datapoint, point) {
			return false
		}
		datapoint.CumPoint = nil
		return true
	default:
		log.Error().Msgf("unknown cum point type %v", reflect.TypeOf(point).String())
		return false
	}
}

func (p *DatapointProcessor) initDatapoint(ctx *datapointContext, datapoint *Datapoint) {

	keys := make([]string, 0, len(datapoint.Attrs))
	values := make([]string, 0, len(datapoint.Attrs))

	for key := range datapoint.Attrs {
		if _, ok := p.dropAttrs[key]; ok {
			delete(datapoint.Attrs, key)
			continue
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)

	digest := ctx.ResettedDigest()
	for _, key := range keys {
		value := datapoint.Attrs[key]
		values = append(values, value)
		_, _ = digest.WriteString(key)
		_, _ = digest.WriteString(value)
	}

	datapoint.AttrsHash = digest.Sum64()
	datapoint.StringKeys = keys
	datapoint.StringValues = values
}

func (p *DatapointProcessor) convertNumberPoint(
	_ context.Context, datapoint *Datapoint, point *NumberPoint,
) bool {
	key := DatapointKey{
		Metric:            datapoint.Metric,
		AttrsHash:         datapoint.AttrsHash,
		StartTimeUnixNano: datapoint.StartTimeUnixNano,
	}

	prevPointAny := p.c2d.SwapPoint(key, point, datapoint.Time)
	if prevPointAny == nil {
		// First data point for this metric, cannot calculate delta
		return false
	}

	prevPoint, ok := prevPointAny.(*NumberPoint)
	if !ok {
		log.Error().Msgf("type assertion failed for previous point, expected *NumberPoint, got %T", prevPointAny)
		return false
	}

	// Calculate delta
	if delta := point.Int - prevPoint.Int; delta > 0 {
		datapoint.Sum = float64(delta)
		return true
	} else if delta := point.Double - prevPoint.Double; delta > 0 {
		datapoint.Sum = delta
		return true
	}

	// Delta is zero or negative (possible counter reset)
	// Set sum to 0 and log if negative (counter reset)
	if point.Int < prevPoint.Int || point.Double < prevPoint.Double {
		log.Debug().
			Str("metric", datapoint.Metric).
			Uint64("attrs_hash", datapoint.AttrsHash).
			Msg("counter reset detected (negative delta)")
		p.mu.Lock()
		p.outOfOrderPoints++
		p.mu.Unlock()
	}

	datapoint.Sum = 0
	return true
}

type MetricKey struct {
	Metric    string
	FlowID    string
	AttrHash  uint64
	ProjectID string
}

// GetStats returns processing statistics
func (p *DatapointProcessor) GetStats() ProcessorStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	realtimeStats := p.realtimeAggregator.GetStats()

	return ProcessorStats{
		ProcessedPoints:  p.processedPoints,
		DroppedPoints:    p.droppedPoints,
		OutOfOrderPoints: p.outOfOrderPoints,
		QueueSize:        len(p.queue),
		CacheSize:        p.c2d.Len(),
		RealtimeStats:    realtimeStats,
	}
}

type ProcessorStats struct {
	ProcessedPoints  int64
	DroppedPoints    int64
	OutOfOrderPoints int64
	QueueSize        int
	CacheSize        int
	RealtimeStats    RealtimeStats
}

// Subscribe creates a subscription for realtime metrics
// Returns a Subscriber that will receive aggregated metrics every 1 second
// for the specified projectID and flowID combination
func (p *DatapointProcessor) Subscribe(ctx context.Context, projectID, flowID string) *Subscriber {
	return p.realtimeAggregator.Subscribe(ctx, projectID, flowID)
}

// Unsubscribe removes a subscription and closes its channel
func (p *DatapointProcessor) Unsubscribe(sub *Subscriber) {
	p.realtimeAggregator.Unsubscribe(sub)
}

// StartRealtimeAggregation starts the realtime aggregation flush loop
// This must be called to enable realtime metric streaming
// Should be called once when the processor is started
func (p *DatapointProcessor) StartRealtimeAggregation(ctx context.Context) {
	p.realtimeAggregator.FlushLoop(ctx)
}

//------------------------------------------------------------------------------

type datapointContext struct {
	context.Context
	digest *xxhash.Digest
}

func newDatapointContext(ctx context.Context) *datapointContext {
	return &datapointContext{
		Context: ctx,
		digest:  xxhash.New(),
	}
}

func (c *datapointContext) ResettedDigest() *xxhash.Digest {
	c.digest.Reset()
	return c.digest
}
