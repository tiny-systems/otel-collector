package statistics

import (
	"context"

	"github.com/tiny-systems/otel-collector/pkg/api-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) GetTraceByID(ctx context.Context, req *api.GetTraceByIDRequest) (*api.GetTraceByIDResponse, error) {
	if req.TraceId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "trace_id is required")
	}

	if req.ProjectId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "project_id is required")
	}

	// Retrieve trace from storage
	entry := s.traceStorage.GetTraceByID(req.TraceId, req.ProjectId)
	if entry == nil {
		return nil, status.Errorf(codes.NotFound, "trace not found")
	}

	// No conversion needed - we use OpenTelemetry Span types directly!
	return &api.GetTraceByIDResponse{
		TraceId:     entry.TraceID,
		ProjectId:   entry.ProjectID,
		FlowId:      entry.FlowID,
		StartTime:   timestamppb.New(entry.StartTime),
		EndTime:     timestamppb.New(entry.EndTime),
		SpansCount:  int32(entry.SpansCount),
		ErrorsCount: int32(entry.ErrorsCount),
		DataCount:   int32(entry.DataCount),
		DataLength:  int64(entry.DataLength),
		DurationNs:  entry.DurationNs,
		Spans:       entry.Spans,
	}, nil
}
