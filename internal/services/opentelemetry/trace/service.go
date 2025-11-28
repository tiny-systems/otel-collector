package trace

import (
	"context"
	"encoding/hex"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry/metrics"
	"github.com/tiny-systems/otel-collector/pkg/attrkey"
	"time"

	"github.com/rs/zerolog/log"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"math"
)

const (
	dataEvent         = "data"
	exceptionEvent    = "exception"
	errorEvent        = "error"
	payloadAttr       = "payload"
	flowIDSpanAttr    = "flowID"
	projectIDSpanAttr = "projectID"
)

// Stat holds statistics for a trace
type Stat struct {
	DataCounter  int
	ErrorCounter int
	StartTimeMin float64
	EndTimeMax   float64
	LongestSpan  uint64
	DataLength   int
	spans        []*v1.Span
	flowID       string
	projectID    string
}

func (s Stat) getDataPoints() []*metrics.Datapoint {
	traces := s.newDatapoint(opentelemetry.MetricTraceCount)
	traces.Sum = 1

	spans := s.newDatapoint(opentelemetry.MetricSpanCount)
	spans.Sum = float64(len(s.spans))

	errors := s.newDatapoint(opentelemetry.MetricSpanErrorCount)
	errors.Sum = float64(s.ErrorCounter)

	data := s.newDatapoint(opentelemetry.MetricSpanDataCount)
	data.Sum = float64(s.DataCounter)

	return []*metrics.Datapoint{traces, spans, errors, data}
}

func (s Stat) newDatapoint(metric string) *metrics.Datapoint {
	dest := new(metrics.Datapoint)
	dest.Metric = attrkey.Clean(metric)
	dest.Instrument = metrics.InstrumentCounter
	dest.Time = time.Now()
	dest.Attrs = metrics.AttrMap{
		"flowID":    s.flowID,
		"projectID": s.projectID,
		"metric":    metric,
	}
	return dest
}

// Entry stores a complete trace with all its spans
type Entry struct {
	TraceID     string
	FlowID      string
	ProjectID   string
	Spans       []*v1.Span
	StartTime   time.Time
	EndTime     time.Time
	SpansCount  int
	ErrorsCount int
	DataCount   int
	DataLength  int
	DurationNs  uint64
	CreatedAt   time.Time
	LastAccess  time.Time
}

// EstimateSize returns approximate memory size in bytes
func (te *Entry) EstimateSize() int {
	// Rough estimate: each span ~2KB + metadata ~500 bytes
	return (len(te.Spans) * 2048) + 500
}

// Service implementation with memory-limited storage
type Service struct {
	collectortracepb.UnimplementedTraceServiceServer
	storage *Storage
	handler metrics.DataPointHandler
}

// NewService creates trace service with memory limit in MB
// maxMemoryMB: maximum memory to use for storing traces (e.g., 512 for 512MB)
func NewService(storage *Storage, handler metrics.DataPointHandler) *Service {
	return &Service{
		storage: storage,
		handler: handler,
	}
}

// Export receives and stores traces from OpenTelemetry
func (s *Service) Export(ctx context.Context, request *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	traceStats := make(map[string]*Stat)

	// Process all spans and collect statistics
	for _, resourceSpan := range request.ResourceSpans {
		for _, scopeSpan := range resourceSpan.ScopeSpans {
			for _, span := range scopeSpan.Spans {
				traceID := hex.EncodeToString(span.TraceId)
				if traceID == "" {
					continue
				}

				// Check if this is a relevant span (has our attributes)
				var from, to, port string
				for _, a := range span.Attributes {
					switch a.Key {
					case "from":
						from = a.Value.GetStringValue()
					case "to":
						to = a.Value.GetStringValue()
					case "port":
						port = a.Value.GetStringValue()
					}
				}

				if from == "" && to == "" && port == "" {
					// Not our span, skip it
					continue
				}

				// Get or create stat for this trace
				trace, ok := traceStats[traceID]
				if !ok {
					trace = &Stat{
						spans: make([]*v1.Span, 0),
					}
				}

				trace.spans = append(trace.spans, span)

				// Extract flowID and projectID
				for _, a := range span.Attributes {
					switch a.Key {
					case flowIDSpanAttr:
						trace.flowID = a.Value.GetStringValue()
					case projectIDSpanAttr:
						trace.projectID = a.Value.GetStringValue()
					}
				}

				// Count events
				for _, event := range span.Events {
					switch event.Name {
					case dataEvent:
						trace.DataCounter++
						for _, a := range event.Attributes {
							if a.Key == payloadAttr {
								trace.DataLength += len([]byte(a.Value.GetStringValue()))
							}
						}
					case errorEvent, exceptionEvent:
						trace.ErrorCounter++
					}
				}

				// Track timing
				if trace.StartTimeMin == 0 {
					trace.StartTimeMin = float64(span.StartTimeUnixNano)
				}
				trace.StartTimeMin = math.Min(trace.StartTimeMin, float64(span.StartTimeUnixNano))
				trace.EndTimeMax = math.Max(trace.EndTimeMax, float64(span.EndTimeUnixNano))
				trace.LongestSpan = uint64(math.Max(float64(trace.LongestSpan), float64(span.EndTimeUnixNano-span.StartTimeUnixNano)))

				traceStats[traceID] = trace
			}
		}
	}

	// Store all traces in memory
	for traceID, trace := range traceStats {
		if trace.projectID == "" {
			log.Warn().Msgf("skip trace without projectID: %+v", trace)
			continue
		}
		if len(trace.spans) == 0 {
			continue
		}

		s.storage.addOrUpdateTrace(traceID, trace)

		// Send metrics to metric storage
		for _, dp := range trace.getDataPoints() {
			s.handler(ctx, dp)
		}
	}

	return &collectortracepb.ExportTraceServiceResponse{}, nil
}
