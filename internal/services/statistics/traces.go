package statistics

import (
	"github.com/tiny-systems/otel-server/pkg/api-go"
)
import "context"

func (s *Service) GetTraces(ctx context.Context, req *api.StatisticsGetTracesRequest) (*api.StatisticsGetTracesResponse, error) {

	traceApi := make([]*api.Trace, 0)
	return &api.StatisticsGetTracesResponse{

		Offset: req.Offset,
		Traces: traceApi,
	}, nil
}
