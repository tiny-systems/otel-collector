package statistics

import (
	"github.com/tiny-systems/otel-server/pkg/api-go"
)

type Service struct {
	api.UnimplementedStatisticsServiceServer
}

func NewService() *Service {
	return &Service{}
}
