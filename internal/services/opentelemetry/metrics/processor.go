package metrics

import (
	"context"
	"github.com/rs/zerolog/log"
	"reflect"
	"runtime"
	"time"

	"github.com/cespare/xxhash/v2"
	"go4.org/syncutil"
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
	gate  *syncutil.Gate

	c2d *CumToDeltaConv

	handler Handler
}

func NewDatapointProcessor(handler Handler) *DatapointProcessor {

	p := &DatapointProcessor{
		batchSize: 100,
		queue:     make(chan *Datapoint, 100000),
		gate:      syncutil.NewGate(runtime.GOMAXPROCS(0)),
		c2d:       NewCumToDeltaConv(100000),
		handler:   handler,
	}

	p.dropAttrs = map[string]struct{}{
		"telemetry_sdk_language": {},
		"telemetry_sdk_name":     {},
		"telemetry_sdk_version":  {},
	}
	return p
}

func (p *DatapointProcessor) AddDatapoint(_ context.Context, datapoint *Datapoint) {
	select {
	case p.queue <- datapoint:
	default:
		log.Error().Msgf("datapoint buffer is full (consider increasing metrics.buffer_size) %s", len(p.queue))
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
}

func (p *DatapointProcessor) processDataPoints(ctx context.Context, src []*Datapoint) {

	p.gate.Start()
	dataPoints := make([]*Datapoint, len(src))
	copy(dataPoints, src)

	go func() {
		defer p.gate.Done()
		p._processDataPoints(newDatapointContext(ctx), dataPoints)
	}()
}

func (p *DatapointProcessor) _processDataPoints(ctx *datapointContext, datapoints []*Datapoint) {

	for i := len(datapoints) - 1; i >= 0; i-- {
		dp := datapoints[i]

		p.initDatapoint(ctx, dp)
		if !p.cumToDelta(ctx, dp) {
			datapoints = append(datapoints[:i], datapoints[i+1:]...)
			log.Warn().Msgf("droppped")
			continue
		}

	}
	if len(datapoints) == 0 {
		return
	}

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

	prevPoint, ok := p.c2d.SwapPoint(key, point, datapoint.Time).(*NumberPoint)
	if !ok {
		return false
	}

	if delta := point.Int - prevPoint.Int; delta > 0 {
		datapoint.Sum = float64(delta)
	} else if delta := point.Double - prevPoint.Double; delta > 0 {
		datapoint.Sum = delta
	}
	return true
}

type MetricKey struct {
	Metric    string
	FlowID    string
	AttrHash  uint64
	ProjectID string
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
