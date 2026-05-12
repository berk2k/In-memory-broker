package broker

import (
	"context"
	"log/slog"

	brokerv1 "github.com/berk2k/mini-go-broker/api/proto/gen"
	"github.com/berk2k/mini-go-broker/internal/config"
	"github.com/berk2k/mini-go-broker/internal/queue/inmem"

	"github.com/google/uuid"
)

type Server struct {
	brokerv1.UnimplementedBrokerServiceServer
	Queue  *inmem.Queue
	Logger *slog.Logger
	Config config.Config
}

func (s *Server) Publish(ctx context.Context, req *brokerv1.PublishRequest) (*brokerv1.PublishResponse, error) {
	id := uuid.NewString()

	if err := s.Queue.Enqueue(inmem.Message{
		ID:      id,
		Payload: req.Payload,
	}); err != nil {
		return nil, err
	}

	s.Logger.Info("message_published",
		slog.String("messageID", id),
		slog.Int("queueSize", s.Queue.Size()),
	)

	return &brokerv1.PublishResponse{
		MessageId: id,
	}, nil
}

func (s *Server) Consume(
	req *brokerv1.ConsumeRequest,
	stream brokerv1.BrokerService_ConsumeServer,
) error {
	consumerID := req.ConsumerId
	if consumerID == "" {
		consumerID = "default"
	}

	s.Logger.Info("consumer_connected",
		slog.String("consumerID", consumerID),
	)

	ctx := stream.Context()

	go func() {
		<-ctx.Done()
		s.Logger.Info("consumer_disconnected",
			slog.String("consumerID", consumerID),
		)
		s.Queue.RequeueAllForConsumer(consumerID)
	}()

	for {
		prefetch := int(req.Prefetch)
		if prefetch <= 0 {
			prefetch = s.Config.DefaultPrefetch
		}

		deliveryID, msg := s.Queue.DequeueLeaseBlocking(consumerID, prefetch)

		if deliveryID == "" {
			return nil
		}

		err := stream.Send(&brokerv1.Delivery{
			DeliveryId: deliveryID,
			MessageId:  msg.ID,
			Payload:    msg.Payload,
			Attempt:    uint32(msg.Attempts),
		})

		if err != nil {
			return err
		}
	}
}

func (s *Server) Ack(
	ctx context.Context,
	req *brokerv1.AckRequest,
) (*brokerv1.AckResponse, error) {
	err := s.Queue.Ack(req.DeliveryId, req.ConsumerId)
	if err != nil {
		return nil, err
	}

	return &brokerv1.AckResponse{}, nil
}

func (s *Server) Nack(ctx context.Context, req *brokerv1.NackRequest) (*brokerv1.NackResponse, error) {
	err := s.Queue.Nack(req.DeliveryId, req.ConsumerId, req.Requeue)
	if err != nil {
		return nil, err
	}

	return &brokerv1.NackResponse{}, nil
}
