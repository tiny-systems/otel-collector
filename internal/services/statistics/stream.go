package statistics

import (
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
)

func (s *Service) GetStream(req *api.StatisticsStreamRequest, stream api.StatisticsService_GetStreamServer) error {

	ctx := stream.Context()

	subscription := s.processor.Subscribe(ctx, req.ProjectID, req.FlowID)
	defer s.processor.Unsubscribe(subscription)

	for {
		select {
		case m := <-subscription.Ch:

			_ = stream.Send(&api.StatisticsStreamResponse{
				Events: []*api.StatsEvent{
					{
						Metric:   m.Metric,
						Value:    float32(m.Value),
						Datetime: m.Timestamp.Unix(),
					},
				},
			})

		case <-ctx.Done():
			log.Info().Msg("stream done")
			return nil
		}
	}
}
