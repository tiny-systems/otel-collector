package cli

import (
	"context"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tiny-systems/otel-server/internal/services/health"
	"github.com/tiny-systems/otel-server/internal/services/opentelemetry/metrics"
	"github.com/tiny-systems/otel-server/internal/services/opentelemetry/trace"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run",
	Long:  `run server`,
	Run: func(cmd *cobra.Command, args []string) {

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		l := log.Logger

		l.Info().Msg("tinysystems otel gRPC server is starting")

		cache, err := ristretto.NewCache(&ristretto.Config[string, string]{
			NumCounters: 1e7,     // number of keys to track frequency of (10M).
			MaxCost:     1 << 30, // maximum cost of cache (1GB).
			BufferItems: 64,      // number of keys per Get buffer.
		})
		if err != nil {
			l.Fatal().Err(err).Msg("cache set error")
		}

		_ = cache

		l.Debug().Msgf("debug is enabled")

		server := grpc.NewServer()

		grpc_health_v1.RegisterHealthServer(server, health.NewChecker())

		wg, ctx := errgroup.WithContext(ctx)

		metricsStore := metrics.NewStorage(512, 3600)
		// processes batches
		mp := metrics.NewDatapointProcessor(metricsStore.SaveDataPoints)
		//
		wg.Go(func() error {
			mp.ProcessLoop(ctx)
			return nil
		})
		//

		traceStorage := trace.NewTraceStorage(512)

		traceService := trace.NewService(traceStorage, mp.AddDatapoint)
		collectormetricspb.RegisterMetricsServiceServer(server, metrics.NewService(mp.AddDatapoint, l))

		collectortracepb.RegisterTraceServiceServer(server, traceService)
		//
		reflection.Register(server)

		go func() {
			ticker := time.NewTicker(1 * time.Minute)
			for range ticker.C {
				stats := traceService.GetStats()
				log.Info().
					Int("traces", stats.TracesCount).
					Int("spans", stats.SpansCount).
					Int("memory_mb", stats.MemoryUsageMB).
					Int("max_mb", stats.MaxMemoryMB).
					Int("oldest_minutes", stats.OldestDataMinutes).
					Msg("trace storage stats")
			}
		}()

		listenAddr := viper.GetString("grpc_listen_address")
		l.Info().Str("grpc", listenAddr).Msg("listening")

		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			l.Fatal().Err(err).Msg("unable to listen")
		}

		defer lis.Close()

		wg.Go(func() error {
			// run grpc server
			return server.Serve(lis)
		})

		<-ctx.Done()
		l.Info().Msg("graceful shutdown")
		server.Stop()

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		l.Info().Msg("all services stopped")
	},
}

func init() {
	RootCmd.AddCommand(runCmd)
}
