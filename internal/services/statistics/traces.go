package statistics

import (
	"github.com/tiny-systems/otel-collector/pkg/api-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)
import "context"

func (s *Service) GetTraces(ctx context.Context, req *api.StatisticsGetTracesRequest) (*api.StatisticsGetTracesResponse, error) {

	if req.ProjectID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "no project id provided")
	}
	if req.FlowID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "no flow id provided")
	}

	start := req.Start

	if start.Seconds < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "start must be greater than zero")
	}
	end := req.End

	if end.Seconds < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "end must be greater than zero")
	}

	if end.AsTime().Before(start.AsTime()) {
		return nil, status.Errorf(codes.InvalidArgument, "end must be greater than start")
	}

	offset := req.Offset
	if offset < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "offset must be greater than zero")
	}
	if offset > 1000 {
		return nil, status.Errorf(codes.InvalidArgument, "offset must be less than 1000")
	}

	traces := s.traceStorage.QueryTraces(req.ProjectID, req.FlowID, start.AsTime(), end.AsTime(), int(offset), 1000)

	traceApi := make([]*api.TraceInfo, len(traces))
	for i, trace := range traces {
		traceApi[i] = trace2Api(trace)
	}

	return &api.StatisticsGetTracesResponse{

		Offset: req.Offset,
		Total:  int64(len(traceApi)),
		Traces: traceApi,
	}, nil
}
