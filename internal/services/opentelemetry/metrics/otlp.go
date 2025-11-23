package metrics

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-server/pkg/attrkey"
	"github.com/tiny-systems/otel-server/pkg/bunconv"
	"github.com/tiny-systems/otel-server/pkg/otlpconv"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"golang.org/x/exp/maps"
	"time"
)

type Instrument string

const (
	InstrumentDeleted   Instrument = "deleted"
	InstrumentGauge     Instrument = "gauge"
	InstrumentAdditive  Instrument = "additive"
	InstrumentHistogram Instrument = "histogram"
	InstrumentCounter   Instrument = "counter"
	InstrumentSummary   Instrument = "summary"
)

const (
	deltaAggTemp = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
)

type DataPointHandler func(ctx context.Context, dp *Datapoint)

type otlpProcessor struct {
	handler DataPointHandler
}

func (p *otlpProcessor) close(_ context.Context) {
	// do achievements
}

func (p *otlpProcessor) otlpGauge(
	ctx context.Context,
	_ *commonpb.InstrumentationScope,
	scopeAttrs AttrMap,
	metric *metricspb.Metric,
	data *metricspb.Metric_Gauge,
) {
	for _, dp := range data.Gauge.DataPoints {
		if dp.Flags&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0 {
			continue
		}

		dest := p.otlpNewDatapoint(scopeAttrs, metric, InstrumentGauge, dp.Attributes, dp.TimeUnixNano)
		switch num := dp.Value.(type) {
		case nil:
			dest.Gauge = 0
			p.enqueue(ctx, dest)
		case *metricspb.NumberDataPoint_AsInt:
			dest.Gauge = float64(num.AsInt)
			p.enqueue(ctx, dest)
		case *metricspb.NumberDataPoint_AsDouble:
			dest.Gauge = num.AsDouble
			p.enqueue(ctx, dest)
		default:
			log.Error().Msgf("unknown data point value %T", dp.Value)
		}
	}
}

func (p *otlpProcessor) otlpSummary(
	ctx context.Context,
	scopeAttrs AttrMap,
	metric *metricspb.Metric,
	data *metricspb.Metric_Summary,
) {
	for _, dp := range data.Summary.DataPoints {
		if dp.Flags&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0 {
			continue
		}

		avg := dp.Sum / float64(dp.Count)
		mina, maxa := avg, avg
		for _, qv := range dp.QuantileValues {
			if qv.Value < mina {
				mina = qv.Value
			}
			if qv.Value > maxa {
				maxa = qv.Value
			}
		}

		dest := p.otlpNewDatapoint(scopeAttrs, metric, InstrumentSummary, dp.Attributes, dp.TimeUnixNano)
		dest.Min = mina
		dest.Max = maxa
		dest.Sum = dp.Sum
		dest.Count = dp.Count

		p.enqueue(ctx, dest)
	}
}

func (p *otlpProcessor) otlpSum(
	ctx context.Context,
	_ *commonpb.InstrumentationScope,
	scopeAttrs AttrMap,
	metric *metricspb.Metric,
	data *metricspb.Metric_Sum,
) {

	isDelta := data.Sum.AggregationTemporality == deltaAggTemp
	for _, dp := range data.Sum.DataPoints {
		if dp.Flags&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0 {
			continue
		}

		dest := p.otlpNewDatapoint(scopeAttrs, metric, "", dp.Attributes, dp.TimeUnixNano)

		if !data.Sum.IsMonotonic {
			dest.Instrument = InstrumentAdditive
			dest.Gauge = toFloat64(dp.Value)
			p.enqueue(ctx, dest)
			continue
		} else {
			dest.Instrument = InstrumentCounter
		}

		if isDelta {
			dest.Sum = toFloat64(dp.Value)
			p.enqueue(ctx, dest)
			continue
		}

		switch value := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsInt:
			dest.StartTimeUnixNano = dp.StartTimeUnixNano
			dest.CumPoint = &NumberPoint{
				Int: value.AsInt,
			}
			p.enqueue(ctx, dest)
		case *metricspb.NumberDataPoint_AsDouble:
			dest.StartTimeUnixNano = dp.StartTimeUnixNano
			dest.CumPoint = &NumberPoint{
				Double: value.AsDouble,
			}
			p.enqueue(ctx, dest)
		default:
			log.Error().Msgf("unknown point value type type %T", dp.Value)
		}
	}
}

func (p *otlpProcessor) otlpNewDatapoint(
	scopeAttrs AttrMap,
	metric *metricspb.Metric,
	instrument Instrument,
	labels []*commonpb.KeyValue,
	unixNano uint64,
) *Datapoint {

	attrs := make(AttrMap, len(scopeAttrs)+len(labels))
	maps.Copy(attrs, scopeAttrs)
	otlpconv.ForEachKeyValue(labels, func(key string, value any) {
		attrs[key] = fmt.Sprint(value)
	})

	metricName := attrkey.Clean(metric.Name)
	dest := p.newDatapoint(metricName, instrument, attrs, unixNano)

	dest.Description = metric.Description
	dest.Unit = bunconv.NormUnit(metric.Unit)

	return dest
}

func (p *otlpProcessor) newDatapoint(
	metricName string,
	instrument Instrument,
	attrs AttrMap,
	unixNano uint64,
) *Datapoint {
	dest := new(Datapoint)

	dest.Metric = metricName
	dest.Instrument = instrument
	dest.Time = time.Unix(0, int64(unixNano))
	dest.Attrs = attrs
	return dest
}

func (p *otlpProcessor) enqueue(ctx context.Context, datapoint *Datapoint) {
	if datapoint.Metric == "" {
		return
	}
	if datapoint.Instrument == "" {
		return
	}
	if datapoint.Time.IsZero() {
		return
	}
	p.handler(ctx, datapoint)
}

//------------------------------------------------------------------------------

func toFloat64(value any) float64 {
	switch num := value.(type) {
	case *metricspb.NumberDataPoint_AsInt:
		return float64(num.AsInt)
	case *metricspb.NumberDataPoint_AsDouble:
		return num.AsDouble
	default:
		return 0
	}
}
