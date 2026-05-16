package inmem

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/berk2k/mini-go-broker/internal/config"
)

type Queue struct {
	mu            sync.Mutex
	cond          *sync.Cond
	ready         []DelayedMessage
	inflight      map[string]Lease
	inflightCount map[string]int
	dlq           []Message
	shuttingDown  bool
	wal           *WAL

	totalPublished       uint64
	totalAcked           uint64
	totalRedelivered     uint64
	totalNacked          uint64
	totalDLQ             uint64
	totalProcessed       uint64
	totalProcessingNanos uint64

	maxRetries int
	maxDLQSize int
	timeout    time.Duration

	logger *slog.Logger
}

func NewQueue(logger *slog.Logger, cfg config.Config, wal *WAL) *Queue {
	q := &Queue{
		ready:         make([]DelayedMessage, 0),
		inflight:      make(map[string]Lease),
		inflightCount: make(map[string]int),
		dlq:           make([]Message, 0),
		maxRetries:    cfg.MaxRetries,
		maxDLQSize:    cfg.MaxDLQSize,
		timeout:       cfg.VisibilityTimeout,
		logger:        logger,
		wal:           wal,
	}
	q.cond = sync.NewCond(&q.mu)
	go q.reaper()
	return q
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready) + len(q.inflight)
}

func (q *Queue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *Queue) InflightSize() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inflight)
}

func (q *Queue) Snapshot() Metrics {
	q.mu.Lock()
	defer q.mu.Unlock()
	avg := 0.0
	if q.totalProcessed > 0 {
		avg = float64(q.totalProcessingNanos) /
			float64(q.totalProcessed) /
			1e6
	}

	return Metrics{
		Ready:                len(q.ready),
		Inflight:             len(q.inflight),
		DLQ:                  len(q.dlq),
		TotalPublished:       q.totalPublished,
		TotalAcked:           q.totalAcked,
		TotalRedelivered:     q.totalRedelivered,
		TotalNacked:          q.totalNacked,
		TotalDLQ:             q.totalDLQ,
		TotalProcessed:       q.totalProcessed,
		AverageLatencyMillis: avg,
	}
}

func (q *Queue) DrainDLQ() []Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	messages := q.dlq
	q.dlq = make([]Message, 0)
	return messages
}

func (q *Queue) PeekDLQ() []Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]Message, len(q.dlq))
	copy(result, q.dlq)
	return result
}

func (q *Queue) Recover(records []WALRecord) (int, error) {
	published := make(map[string]Message)
	publishedOrder := make([]string, 0)
	acked := make(map[string]struct{})

	for _, r := range records {
		switch r.Type {
		case "publish":
			if _, exists := published[r.MessageID]; !exists {
				publishedOrder = append(publishedOrder, r.MessageID)
			}
			published[r.MessageID] = Message{ID: r.MessageID, Payload: r.Payload}
		case "ack":
			acked[r.MessageID] = struct{}{}
		default:
			return 0, fmt.Errorf("unknown WAL record type %q for messageID %s", r.Type, r.MessageID)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	count := 0
	for _, id := range publishedOrder {
		if _, done := acked[id]; done {
			continue
		}

		msg := published[id]
		q.ready = append(q.ready, DelayedMessage{
			Message: msg,
			ReadyAt: time.Now(),
		})
		count++
	}

	if count > 0 {
		q.cond.Broadcast()
	}
	return count, nil
}

func (q *Queue) ReplayDLQ() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	count := len(q.dlq)
	for _, msg := range q.dlq {
		msg.Attempts = 0
		q.ready = append(q.ready, DelayedMessage{
			Message: msg,
			ReadyAt: time.Now(),
		})
		q.logger.Info("message_requeued",
			slog.String("messageID", msg.ID),
			slog.String("reason", "dlq_replay"),
		)
	}
	q.dlq = make([]Message, 0)
	q.cond.Broadcast()
	return count
}
