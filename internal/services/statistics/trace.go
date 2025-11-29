package statistics

import (
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry/trace"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
)

func trace2Api(trace *trace.Entry) *api.TraceInfo {
	return &api.TraceInfo{
		ID:       trace.TraceID,
		Spans:    int64(len(trace.Spans)),
		Errors:   int64(trace.ErrorsCount),
		Data:     int64(trace.DataCount),
		Length:   int64(trace.DataLength),
		Duration: int64(trace.DurationNs),
		Start:    int64(trace.StartTime.Nanosecond()),
		End:      int64(trace.EndTime.Nanosecond()),
	}
}
