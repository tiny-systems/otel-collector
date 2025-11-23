package statistics

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/otel-server/internal/services/opentelemetry/trace"
	"github.com/tiny-systems/otel-server/pkg/api-go"
)

func (s *Service) GetStream(req *api.StatisticsStreamRequest, stream api.StatisticsService_GetStreamServer) error {

	ctx := stream.Context()

	var (
		events          []*api.StatsEvent
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

	events = append(events, getEvent("event"))

	stream.Send(&api.StatisticsStreamResponse{
		Events: events,
	})

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("stream done")
			return nil
		}
	}
}

func getDataset(metric string) *api.Dataset {

	switch metric {
	case trace.MetricSpanErrorCount:
		return &api.Dataset{
			Label:           "Errors",
			BackgroundColor: "#ffaaa5",
			BorderColor:     "#ffaaa5",
		}
	case trace.MetricTraceCount:
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
