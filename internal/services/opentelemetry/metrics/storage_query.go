package metrics

import (
	"context"
	"sort"
	"time"
)

// QueryRequest represents a request to query metrics
type QueryRequest struct {
	ProjectID string
	FlowID    string    // Optional: if empty, aggregates across all flows
	Metric    string    // e.g., "tiny_span_error_count", "tiny_trace_count"
	Start     time.Time // Start of time range
	End       time.Time // End of time range
}

// QueryResult contains the aggregated result
type QueryResult struct {
	Metric    string
	ProjectID string
	FlowID    string // Empty if aggregated across flows
	Start     time.Time
	End       time.Time

	// Aggregated values
	Sum   float64 // Total sum over time range
	Avg   float64 // Average value
	Min   float64 // Minimum value
	Max   float64 // Maximum value
	Count int     // Number of data points

	// Time series data
	Points []DataPoint
}

// ProjectSummary provides a summary of all metrics for a project
type ProjectSummary struct {
	ProjectID string
	FlowID    string // Empty if all flows
	TimeRange TimeRange

	Metrics map[string]*MetricSummary
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}

type MetricSummary struct {
	Name   string
	Sum    float64
	Avg    float64
	Min    float64
	Max    float64
	Count  int
	Latest float64 // Most recent value
}

// QueryMetric queries a specific metric for a project/flow
func (s *Storage) QueryMetric(ctx context.Context, req QueryRequest) (*QueryResult, error) {
	if req.End.Before(req.Start) {
		req.End = req.Start
	}

	result := &QueryResult{
		Metric:    req.Metric,
		ProjectID: req.ProjectID,
		FlowID:    req.FlowID,
		Start:     req.Start,
		End:       req.End,
		Points:    make([]DataPoint, 0),
	}

	// If FlowID is specified, query single flow
	if req.FlowID != "" {
		return s.querySingleFlow(ctx, req, result)
	}

	// Otherwise, aggregate across all flows for this project
	return s.queryAggregateFlows(ctx, req, result)
}

func (s *Storage) querySingleFlow(ctx context.Context, req QueryRequest, result *QueryResult) (*QueryResult, error) {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	// Find all matching metric keys (different attribute hashes)
	var allPoints []DataPoint

	for mk, buffer := range s.tsBuffer.buffers {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if mk.ProjectID == req.ProjectID &&
			mk.FlowID == req.FlowID &&
			mk.Metric == req.Metric {
			points := buffer.Query(req.Start, req.End)
			allPoints = append(allPoints, points...)
		}
	}

	if len(allPoints) == 0 {
		return result, nil
	}

	// Sort by time
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Time.Before(allPoints[j].Time)
	})

	// Calculate aggregates
	result.Points = allPoints
	result.Count = len(allPoints)
	result.Min = allPoints[0].Value
	result.Max = allPoints[0].Value
	result.Sum = 0

	for _, p := range allPoints {
		result.Sum += p.Value
		if p.Value < result.Min {
			result.Min = p.Value
		}
		if p.Value > result.Max {
			result.Max = p.Value
		}
	}

	result.Avg = result.Sum / float64(result.Count)

	return result, nil
}

func (s *Storage) queryAggregateFlows(ctx context.Context, req QueryRequest, result *QueryResult) (*QueryResult, error) {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	// Collect points from all flows for this project
	var allPoints []DataPoint

	for mk, buffer := range s.tsBuffer.buffers {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if mk.ProjectID == req.ProjectID && mk.Metric == req.Metric {
			points := buffer.Query(req.Start, req.End)
			allPoints = append(allPoints, points...)
		}
	}

	if len(allPoints) == 0 {
		return result, nil
	}

	// Sort by time
	sort.Slice(allPoints, func(i, j int) bool {
		return allPoints[i].Time.Before(allPoints[j].Time)
	})

	// Calculate aggregates
	result.Points = allPoints
	result.Count = len(allPoints)
	result.Min = allPoints[0].Value
	result.Max = allPoints[0].Value
	result.Sum = 0

	for _, p := range allPoints {
		result.Sum += p.Value
		if p.Value < result.Min {
			result.Min = p.Value
		}
		if p.Value > result.Max {
			result.Max = p.Value
		}
	}

	result.Avg = result.Sum / float64(result.Count)

	return result, nil
}

// GetProjectSummary returns a summary of all metrics for a project
func (s *Storage) GetProjectSummary(ctx context.Context, projectID, flowID string, start, end time.Time) (*ProjectSummary, error) {
	summary := &ProjectSummary{
		ProjectID: projectID,
		FlowID:    flowID,
		TimeRange: TimeRange{Start: start, End: end},
		Metrics:   make(map[string]*MetricSummary),
	}

	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	// Group points by metric name
	metricPoints := make(map[string][]DataPoint)

	for mk, buffer := range s.tsBuffer.buffers {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Filter by project and optionally by flow
		if mk.ProjectID != projectID {
			continue
		}
		if flowID != "" && mk.FlowID != flowID {
			continue
		}

		points := buffer.Query(start, end)
		if len(points) > 0 {
			metricPoints[mk.Metric] = append(metricPoints[mk.Metric], points...)
		}
	}

	// Calculate summaries for each metric
	for metricName, points := range metricPoints {
		if len(points) == 0 {
			continue
		}

		// Sort by time to get latest
		sort.Slice(points, func(i, j int) bool {
			return points[i].Time.Before(points[j].Time)
		})

		ms := &MetricSummary{
			Name:   metricName,
			Count:  len(points),
			Min:    points[0].Value,
			Max:    points[0].Value,
			Sum:    0,
			Latest: points[len(points)-1].Value,
		}

		for _, p := range points {
			ms.Sum += p.Value
			if p.Value < ms.Min {
				ms.Min = p.Value
			}
			if p.Value > ms.Max {
				ms.Max = p.Value
			}
		}

		ms.Avg = ms.Sum / float64(ms.Count)
		summary.Metrics[metricName] = ms
	}

	return summary, nil
}

// GetAvailableMetrics returns list of metrics available for a project
func (s *Storage) GetAvailableMetrics(projectID, flowID string) []string {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	metricsSet := make(map[string]struct{})

	for mk := range s.tsBuffer.buffers {
		if mk.ProjectID != projectID {
			continue
		}
		if flowID != "" && mk.FlowID != flowID {
			continue
		}
		metricsSet[mk.Metric] = struct{}{}
	}

	metrics := make([]string, 0, len(metricsSet))
	for m := range metricsSet {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)

	return metrics
}

// GetAvailableFlows returns list of flows for a project
func (s *Storage) GetAvailableFlows(projectID string) []string {
	s.tsBuffer.mu.RLock()
	defer s.tsBuffer.mu.RUnlock()

	flowsSet := make(map[string]struct{})

	for mk := range s.tsBuffer.buffers {
		if mk.ProjectID == projectID && mk.FlowID != "" {
			flowsSet[mk.FlowID] = struct{}{}
		}
	}

	flows := make([]string, 0, len(flowsSet))
	for f := range flowsSet {
		flows = append(flows, f)
	}
	sort.Strings(flows)

	return flows
}

// DownsamplePoints reduces the number of points by aggregating into time buckets
func DownsamplePoints(points []DataPoint, buckets int) []DataPoint {
	if len(points) <= buckets || buckets <= 0 {
		return points
	}

	pointsPerBucket := len(points) / buckets
	if pointsPerBucket == 0 {
		pointsPerBucket = 1
	}

	result := make([]DataPoint, 0, buckets)

	for i := 0; i < len(points); i += pointsPerBucket {
		end := i + pointsPerBucket
		if end > len(points) {
			end = len(points)
		}

		bucket := points[i:end]

		// Calculate bucket aggregates
		sum := 0.0
		minVal := bucket[0].Value
		maxVal := bucket[0].Value

		for _, p := range bucket {
			sum += p.Value
			if p.Value < minVal {
				minVal = p.Value
			}
			if p.Value > maxVal {
				maxVal = p.Value
			}
		}

		avg := sum / float64(len(bucket))

		// Use middle point's time and average value
		midPoint := bucket[len(bucket)/2]

		result = append(result, DataPoint{
			Time:   midPoint.Time,
			Value:  avg,
			Labels: midPoint.Labels,
		})
	}

	return result
}
