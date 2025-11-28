package statistics

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-collector/internal/services/opentelemetry"
	"github.com/tiny-systems/otel-collector/pkg/api-go"
)

func (s *Service) GetStream(req *api.StatisticsStreamRequest, stream api.StatisticsService_GetStreamServer) error {

	ctx := stream.Context()

	var (
		datasetsIdx     = make(map[string]int)
		datasetsCounter = 0
	)

	getEvent := func(metric string) *api.StatsEvent {
		event := &api.StatsEvent{}
		// check if dataset registered
		idx, ok := datasetsIdx[metric]
		if !ok {
			// register it
			// in counter
			idx = datasetsCounter
			datasetsCounter++
			datasetsIdx[metric] = idx
			event.Dataset = getDataset(metric)
		}

		event.Index = int32(idx)
		return event
	}

	subscription := s.processor.Subscribe(ctx, req.ProjectID, req.FlowID)
	defer s.processor.Unsubscribe(subscription)

	for {
		select {
		case m := <-subscription.Ch:

			ev := getEvent(m.Metric)
			ev.Value = float32(m.Value)
			ev.Datetime = m.Timestamp.Unix()

			_ = stream.Send(&api.StatisticsStreamResponse{
				Events: []*api.StatsEvent{
					ev,
				},
			})

		case <-ctx.Done():
			log.Info().Msg("stream done")
			return nil
		}
	}
}

func getDataset(metric string) *api.Dataset {

	switch metric {
	case opentelemetry.MetricSpanErrorCount:
		return &api.Dataset{
			Label:           "Errors",
			BackgroundColor: "#ffaaa5",
			BorderColor:     "#ffaaa5",
		}
	case opentelemetry.MetricTraceCount:
		return &api.Dataset{
			Label:           "Traces",
			BackgroundColor: "#a8e6cf",
			BorderColor:     "#a8e6cf",
		}

	default:
		return &api.Dataset{
			Label: fmt.Sprintf("Unknown: %s", metric),
		}
	}
}
