


### Processor stats Table

| Stat | Type | Critical When | Action Required |
|------|------|---------------|-----------------|
| ProcessedPoints | Cumulative | Not growing | Check data ingestion |
| DroppedPoints | Cumulative | Growing rapidly | Scale up, increase queue |
| OutOfOrderPoints | Cumulative | > 10% of processed | Check data quality |
| QueueSize | Current | > 80k | Immediate scaling needed |
| CacheSize | Current | > 90k | Normal, LRU handles it |


### Storage stats Table
| Stat | Type | Critical When | Action Required |
|------|------|---------------|-----------------|
| MetricsCount | Current | Constantly changing | High churn, may need tuning |
| MemoryUsageMB | Current | > 90% of max | Approaching limit |
| OldestDataMinutes | Current | Much less than expected | Buffers wrapping too fast |

### Realtime stats

| Stat | Type | Resets? | Good Value | Bad Value |
|------|------|---------|------------|-----------|
| SubscriberCount | Current | Yes (dynamic) | > 0 if expecting subscribers | N/A |
| UniqueSubscriptions | Current | Yes (dynamic) | Matches expected streams | N/A |
| CurrentAggregates | Current | Yes (per window) | < MaxAggregates | Near MaxAggregates |
| MaxAggregates | Config | No | 10000+ | Too low for cardinality |
| TotalEvictions | Cumulative | No | 0 | Growing rapidly |
| TotalAggregated | Cumulative | No | Growing | Stagnant when should grow |
| TotalFlushed | Cumulative | No | ≈ TotalAggregated | Much less than aggregated |
| DroppedFullChan | Cumulative | No | 0 | Growing |
