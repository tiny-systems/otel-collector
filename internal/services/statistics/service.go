package statistics

import (
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry/metrics"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry/trace"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
)

type Service struct {
	api.UnimplementedStatisticsServiceServer
	processor     *metrics.DatapointProcessor
	traceStorage  *trace.Storage
	metricStorage *metrics.Storage
}

func NewService(processor *metrics.DatapointProcessor, traceStorage *trace.Storage, metricStorage *metrics.Storage) *Service {
	return &Service{
		processor:     processor,
		traceStorage:  traceStorage,
		metricStorage: metricStorage,
	}
}
