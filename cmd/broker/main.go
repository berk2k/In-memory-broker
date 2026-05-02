package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	brokerv1 "github.com/berk2k/mini-go-broker/api/proto/gen"
	"github.com/berk2k/mini-go-broker/internal/broker"
	"github.com/berk2k/mini-go-broker/internal/config"
	"github.com/berk2k/mini-go-broker/internal/observability"
	"github.com/berk2k/mini-go-broker/internal/queue/inmem"
)

func main() {
	godotenv.Load()

	cfg := config.Load()
	logger := observability.NewLogger()

	queue := inmem.NewQueue(logger, cfg)

	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen", slog.String("error", err.Error()))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	srv := &broker.Server{Queue: queue, Logger: logger, Config: cfg}
	brokerv1.RegisterBrokerServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	observability.StartMetricsServer(queue, cfg.MetricsPort, logger)
	observability.RegisterAdminHandlers(queue, logger)

	//NOTE: pprof is for development only, do not expose in production
	go func() {
		logger.Info("pprof_started", slog.String("port", ":6060"))
		if err := http.ListenAndServe(":6060", nil); err != nil {
			logger.Warn("pprof_failed", slog.String("error", err.Error()))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("broker_started", slog.String("port", cfg.GRPCPort))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc_serve_failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown_signal_received")

	queue.Shutdown()

	deadline := time.Now().Add(cfg.DrainTimeout)

	for {
		inflight := queue.InflightSize()

		if inflight == 0 {
			logger.Info("drain_complete")
			break
		}

		if time.Now().After(deadline) {
			logger.Warn("drain_timeout_reached", slog.Int("inflight", inflight))
			queue.ForceRequeueAll()
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	grpcServer.GracefulStop()
	logger.Info("broker_stopped")
}
