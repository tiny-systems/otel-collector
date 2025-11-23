package statistics

import "github.com/tiny-systems/otel-server/pkg/api-go"

type Trace struct {
	ID       string
	Spans    int64
	Errors   int64
	Data     int64
	Length   int64
	Duration int64
	Start    float64
	End      float64
}

func trace2Api(trace *Trace) *api.Trace {
	return &api.Trace{
		ID:       trace.ID,
		Spans:    trace.Spans,
		Errors:   trace.Errors,
		Data:     trace.Data,
		Length:   trace.Length,
		Duration: trace.Duration,
		Start:    trace.Start,
		End:      trace.End,
	}
}
