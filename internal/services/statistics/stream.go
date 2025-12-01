package statistics

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
)

func (s *Service) GetStream(req *api.StatisticsStreamRequest, stream api.StatisticsService_GetStreamServer) error {

	ctx := stream.Context()

	if len(req.Metrics) == 0 {
		return fmt.Errorf("no metrics provided")
	}

	subscription := s.processor.Subscribe(ctx, req.ProjectID, req.FlowID, req.Metrics)
	defer s.processor.Unsubscribe(subscription)

	for {
		select {
		case m := <-subscription.Ch:

			_ = stream.Send(&api.StatisticsStreamResponse{
				Events: []*api.StatsEvent{
					{
						Metric:   m.Metric,
						Value:    m.Value,
						Datetime: m.Timestamp.UnixMilli(),
					},
				},
			})

		case <-ctx.Done():
			log.Info().Msg("stream done")
			return nil
		}
	}
}
