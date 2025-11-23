package metrics

import (
	"github.com/rs/zerolog/log"
	"github.com/zyedidia/generic/cache"
	"sync"
	"time"
)

type DatapointKey struct {
	Metric            string
	AttrsHash         uint64
	StartTimeUnixNano uint64
}

type DatapointValue struct {
	Key   DatapointKey
	Point any
	Time  time.Time
}

type CumToDeltaConv struct {
	mu    sync.Mutex
	cache *cache.Cache[DatapointKey, *DatapointValue]
}

func NewCumToDeltaConv(n int) *CumToDeltaConv {
	c := &CumToDeltaConv{
		cache: cache.New[DatapointKey, *DatapointValue](n),
	}
	return c
}

func (c *CumToDeltaConv) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cache.Size()
}

func (c *CumToDeltaConv) SwapPoint(key DatapointKey, point any, time time.Time) any {
	c.mu.Lock()
	defer c.mu.Unlock()

	if value, ok := c.cache.Get(key); ok {
		if time.Before(value.Time) {
			// Out-of-order data point detected
			log.Debug().
				Str("metric", key.Metric).
				Uint64("attrs_hash", key.AttrsHash).
				Time("current_time", value.Time).
				Time("received_time", time).
				Msg("out-of-order data point detected")
			return nil
		}

		prevPoint := value.Point
		value.Point = point
		value.Time = time
		return prevPoint
	}

	c.cache.Put(key, &DatapointValue{
		Point: point,
		Time:  time,
	})
	return nil
}

//------------------------------------------------------------------------------

type NumberPoint struct {
	Int    int64
	Double float64
}
