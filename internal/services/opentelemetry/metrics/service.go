package metrics

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-server/pkg/otlpconv"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"golang.org/x/exp/maps"
	"strings"
)

type Service struct {
	collectormetricspb.UnimplementedMetricsServiceServer
	handler DataPointHandler
	log     zerolog.Logger
}

type (
	TimeSeriesType string
)

type Namer interface {
	GetMetricKey(mk MetricKey, typ TimeSeriesType, bucket string) string
}

func (s Service) Export(ctx context.Context, req *collectormetricspb.ExportMetricsServiceRequest) (*collectormetricspb.ExportMetricsServiceResponse, error) {

	p := otlpProcessor{
		handler: s.handler,
	}
	defer p.close(ctx)

	for _, rms := range req.ResourceMetrics {
		var resource AttrMap
		if rms.Resource != nil {
			resource = make(AttrMap, len(rms.Resource.Attributes))
			otlpconv.ForEachKeyValue(rms.Resource.Attributes, func(key string, value any) {
				resource[key] = fmt.Sprint(value)
			})
		}

		for _, sm := range rms.ScopeMetrics {
			var scopeAttrs AttrMap
			if sm.Scope != nil && len(sm.Scope.Attributes) > 0 {
				scopeAttrs = make(AttrMap, len(resource)+len(sm.Scope.Attributes))
				maps.Copy(scopeAttrs, resource)
				otlpconv.ForEachKeyValue(sm.Scope.Attributes, func(key string, value any) {
					val := fmt.Sprint(value)
					if val == "" {
						return
					}
					scopeAttrs[key] = val
				})
			} else {
				scopeAttrs = resource
			}

			for _, metric := range sm.Metrics {
				if metric == nil {
					continue
				}
				// only native
				if !strings.HasPrefix(metric.Name, "tiny_") {
					continue
				}

				switch data := metric.Data.(type) {
				case *metricspb.Metric_Gauge:
					p.otlpGauge(ctx, sm.Scope, scopeAttrs, metric, data)
				case *metricspb.Metric_Sum:
					p.otlpSum(ctx, sm.Scope, scopeAttrs, metric, data)
				case *metricspb.Metric_Histogram:
				//	p.otlpHistogram(ctx, sm.Scope, scopeAttrs, metric, data)
				case *metricspb.Metric_ExponentialHistogram:
				//	p.otlpExpHistogram(ctx, sm.Scope, scopeAttrs, metric, data)
				case *metricspb.Metric_Summary:
					p.otlpSummary(ctx, scopeAttrs, metric, data)
				default:
					log.Error().Msgf("unknown metric %T", data)
				}
			}
		}
	}
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

func NewService(handler DataPointHandler, log zerolog.Logger) *Service {
	s := &Service{
		handler: handler,

		log: log,
	}
	return s
}
