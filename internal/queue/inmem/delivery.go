package inmem

import (
	"errors"
	"log/slog"
	"time"

	"github.com/berk2k/mini-go-broker/pkg/backoff"
	"github.com/google/uuid"
)

func (q *Queue) Enqueue(msg Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.wal != nil {
		if err := q.wal.appendPublish(msg.ID, msg.Payload); err != nil {
			return err
		}
	}

	q.ready = append(q.ready, DelayedMessage{
		Message: msg,
		ReadyAt: time.Now(),
	})

	q.totalPublished++
	q.cond.Signal()
	return nil
}

func (q *Queue) DequeueLeaseBlocking(consumerID string, prefetch int) (string, Message) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.shuttingDown {
			return "", Message{}
		}

		if q.inflightCount[consumerID] >= prefetch {
			q.cond.Wait()
			continue
		}

		now := time.Now()
		index := -1

		for i, dm := range q.ready {
			if now.After(dm.ReadyAt) {
				index = i
				break
			}
		}

		if index == -1 {
			q.cond.Wait()
			continue
		}

		dm := q.ready[index]
		q.ready = append(q.ready[:index], q.ready[index+1:]...)

		q.inflightCount[consumerID]++

		deliveryID := uuid.NewString()

		q.inflight[deliveryID] = Lease{
			Message:    dm.Message,
			ConsumerID: consumerID,
			Deadline:   time.Now().Add(q.timeout),
			StartedAt:  time.Now(),
		}

		q.logger.Info("lease_created",
			slog.String("deliveryID", deliveryID),
			slog.String("messageID", dm.Message.ID),
			slog.String("consumerID", consumerID),
			slog.Int("attempt", dm.Message.Attempts),
		)

		return deliveryID, dm.Message
	}
}

func (q *Queue) Ack(deliveryID string, consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.inflight[deliveryID]
	if !ok {
		return errors.New("invalid deliveryID")
	}

	if lease.ConsumerID != consumerID {
		return errors.New("consumer mismatch")
	}

	latency := time.Since(lease.StartedAt)

	if q.wal != nil {
		if err := q.wal.appendAck(lease.Message.ID); err != nil {
			return err
		}
	}

	q.totalProcessed++
	q.totalProcessingNanos += uint64(latency.Nanoseconds())

	delete(q.inflight, deliveryID)
	q.inflightCount[consumerID]--
	q.totalAcked++

	q.logger.Info("message_acked",
		slog.String("deliveryID", deliveryID),
		slog.String("messageID", lease.Message.ID),
		slog.String("consumerID", consumerID),
		slog.Float64("latencyMs", float64(latency.Milliseconds())),
	)

	q.cond.Signal()
	return nil
}

func (q *Queue) Nack(deliveryID string, consumerID string, requeue bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.inflight[deliveryID]
	if !ok {
		return errors.New("invalid deliveryID")
	}

	if lease.ConsumerID != consumerID {
		return errors.New("consumer mismatch")
	}

	delete(q.inflight, deliveryID)
	q.inflightCount[consumerID]--

	if requeue {
		lease.Message.Attempts++
		q.totalNacked++

		if lease.Message.Attempts >= q.maxRetries {
			q.logger.Warn("message_dlq",
				slog.String("messageID", lease.Message.ID),
				slog.String("reason", "nack_max_retries"),
				slog.Int("attempts", lease.Message.Attempts),
			)
			q.addToDLQ(lease.Message)
		} else {
			delay := backoff.Exponential(lease.Message.Attempts)

			q.ready = append(q.ready, DelayedMessage{
				Message: lease.Message,
				ReadyAt: time.Now().Add(delay),
			})

			q.logger.Info("message_requeued",
				slog.String("messageID", lease.Message.ID),
				slog.String("reason", "nack"),
				slog.Int("attempt", lease.Message.Attempts),
				slog.Float64("delayMs", float64(delay.Milliseconds())),
			)

			q.totalRedelivered++
			q.cond.Signal()
		}
	}

	return nil
}

func (q *Queue) RenewLease(deliveryID string, consumerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.inflight[deliveryID]
	if !ok {
		return errors.New("invalid deliveryID")
	}

	if lease.ConsumerID != consumerID {
		return errors.New("consumer mismatch")
	}

	lease.Deadline = time.Now().Add(q.timeout)
	q.inflight[deliveryID] = lease

	return nil
}
