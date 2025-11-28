package statistics

import (
	"context"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry/metrics"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// QueryMetric retrieves a specific metric with optional downsampling
func (s *Service) QueryMetric(ctx context.Context, req *api.QueryMetricRequest) (*api.QueryMetricResponse, error) {
	// Validate request
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	if req.Metric == "" {
		return nil, status.Error(codes.InvalidArgument, "metric is required")
	}
	if req.StartTime == nil || req.EndTime == nil {
		return nil, status.Error(codes.InvalidArgument, "start_time and end_time are required")
	}

	// Query storage
	result, err := s.metricStorage.QueryMetric(ctx, metrics.QueryRequest{
		ProjectID: req.ProjectId,
		FlowID:    req.FlowId,
		Metric:    req.Metric,
		Start:     req.StartTime.AsTime(),
		End:       req.EndTime.AsTime(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query metric: %v", err)
	}

	// Convert to proto
	response := &api.QueryMetricResponse{
		Metric:    result.Metric,
		ProjectId: result.ProjectID,
		FlowId:    result.FlowID,
		StartTime: timestamppb.New(result.Start),
		EndTime:   timestamppb.New(result.End),
		Stats: &api.MetricStatistics{
			Sum:   result.Sum,
			Avg:   result.Avg,
			Min:   result.Min,
			Max:   result.Max,
			Count: int32(result.Count),
		},
		OriginalPointCount: int32(len(result.Points)),
	}

	points := result.Points

	// Apply downsampling if requested
	if req.Downsample != nil && req.Downsample.Enabled && req.Downsample.MaxPoints > 0 {
		if len(points) > int(req.Downsample.MaxPoints) {
			points = metrics.DownsamplePoints(points, int(req.Downsample.MaxPoints))
			response.Downsampled = true
		}
	}

	// Convert data points
	response.Points = make([]*api.DataPoint, len(points))
	for i, p := range points {
		response.Points[i] = &api.DataPoint{
			Time:   timestamppb.New(p.Time),
			Value:  p.Value,
			Labels: p.Labels,
		}
	}

	return response, nil
}

// GetProjectSummary returns aggregated summary of all metrics for a project
func (s *Service) GetProjectSummary(ctx context.Context, req *api.GetProjectSummaryRequest) (*api.GetProjectSummaryResponse, error) {
	// Validate request
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	if req.StartTime == nil || req.EndTime == nil {
		return nil, status.Error(codes.InvalidArgument, "start_time and end_time are required")
	}

	// Query storage
	summary, err := s.metricStorage.GetProjectSummary(
		ctx,
		req.ProjectId,
		req.FlowId,
		req.StartTime.AsTime(),
		req.EndTime.AsTime(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get project summary: %v", err)
	}

	// Convert to proto
	response := &api.GetProjectSummaryResponse{
		ProjectId: summary.ProjectID,
		FlowId:    summary.FlowID,
		StartTime: timestamppb.New(summary.TimeRange.Start),
		EndTime:   timestamppb.New(summary.TimeRange.End),
		Metrics:   make(map[string]*api.MetricSummary),
	}

	for name, ms := range summary.Metrics {
		response.Metrics[name] = &api.MetricSummary{
			Name:   ms.Name,
			Sum:    ms.Sum,
			Avg:    ms.Avg,
			Min:    ms.Min,
			Max:    ms.Max,
			Count:  int32(ms.Count),
			Latest: ms.Latest,
		}
	}

	return response, nil
}

// GetAvailableFlows returns list of flows for a project
func (s *Service) GetAvailableFlows(ctx context.Context, req *api.GetAvailableFlowsRequest) (*api.GetAvailableFlowsResponse, error) {
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}

	flows := s.metricStorage.GetAvailableFlows(req.ProjectId)

	return &api.GetAvailableFlowsResponse{
		ProjectId: req.ProjectId,
		FlowIds:   flows,
	}, nil
}

// GetAvailableMetrics returns list of available metrics for a project/flow
func (s *Service) GetAvailableMetrics(_ context.Context, req *api.GetAvailableMetricsRequest) (*api.GetAvailableMetricsResponse, error) {
	if req.ProjectId == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}

	metricsList := s.metricStorage.GetAvailableMetrics(req.ProjectId, req.FlowId)

	return &api.GetAvailableMetricsResponse{
		ProjectId: req.ProjectId,
		FlowId:    req.FlowId,
		Metrics:   metricsList,
	}, nil
}
