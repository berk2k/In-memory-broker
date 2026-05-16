# Mini Go Broker

A learning-focused message broker written in Go to understand how modern messaging systems (e.g. RabbitMQ, Kafka consumers) work internally.

This project implements core messaging semantics from scratch to explore:

- Delivery guarantees
- Flow control (QoS / prefetch)
- Lease-based processing
- Failure isolation
- Consumer lifecycle management
- Controlled shutdown patterns

This is not intended to replace production brokers.  
The goal is to make distributed messaging mechanics explicit instead of hidden behind abstractions.

---

# Features

## Core Messaging Semantics

- At-least-once delivery
- Lease-based inflight tracking
- Visibility timeout
- Redelivery on failure
- Exponential backoff on retry
- Prefetch (per-consumer flow control)
- Ack / Nack support

## Failure Isolation

- Retry limit per message
- Dead Letter Queue (DLQ)
- Bounded DLQ (drop-oldest policy)

## Consumer Lifecycle

- Immediate inflight requeue on disconnect
- Graceful shutdown with draining
- Timeout-based forced requeue on shutdown

## Optional Persistence

- Opt-in write-ahead log (WAL) via `WAL_PATH`
- Crash recovery for published but unacked messages
- Startup replay from newline-delimited JSON WAL records
- Fsync-before-mutate durability model

## Observability

- Structured JSON logging (slog)
- JSON metrics endpoint (`/metrics/json`)
- Prometheus-compatible metrics endpoint (`/metrics`)
- Ready / Inflight / DLQ gauges
- Delivery counters (published, acked, nacked, redelivered)
- Processing latency measurement (average)
- Total successfully processed messages

---

# How to Run

### Prerequisites

- Go 1.21+

### 1. Clone the repository

```bash
git clone https://github.com/berk2k/mini-go-broker.git
cd mini-go-broker
```

### 2. Configure (optional)

Copy the example env file and adjust values if needed:

```bash
cp .env.example .env
```

Default values work out of the box — no configuration required.

### 3. Run the broker

```bash
go run cmd/broker/main.go
```

Expected output:

```json
{"time":"...","level":"INFO","msg":"metrics_server_started","port":":8080"}
{"time":"...","level":"INFO","msg":"broker_started","port":":50051"}
```

### Run with WAL enabled

By default, the broker runs fully in memory. To enable the optional write-ahead log:

**Windows CMD**

```cmd
set WAL_PATH=.\wal.log
go run .\cmd\broker
```
**Unix/macOS/Git Bash**

```bash
WAL_PATH=./wal.log go run ./cmd/broker
```

When WAL is enabled, the broker writes `publish` and `ack` records before mutating in-memory state. On startup, it replays the WAL and restores published messages that do not have a matching ack.

WAL files may contain message payloads. Do not commit `wal.log`, `*.wal`, or local data directories to Git.
---


# Configuration

All parameters are configurable via environment variables. Defaults are applied if not set.

| Variable | Default | Description |
|---|---|---|
| GRPC_PORT | :50051 | gRPC server listen address |
| METRICS_PORT | :8080 | HTTP metrics server address |
| MAX_RETRIES | 3 | Max delivery attempts before DLQ |
| MAX_DLQ_SIZE | 100 | Maximum DLQ capacity (drop-oldest) |
| VISIBILITY_TIMEOUT_SEC | 5 | Lease deadline in seconds |
| DRAIN_TIMEOUT_SEC | 10 | Graceful shutdown drain window |
| DEFAULT_PREFETCH | 1 | Default per-consumer prefetch limit |
| WAL_PATH | unset | Optional WAL file path. When set, publish/ack records are persisted and replayed on startup. |

---

# Usage Examples

### Publish a message

```bash
grpcurl -plaintext -d '{"payload": "aGVsbG8="}' \
  localhost:50051 broker.v1.BrokerService/Publish
```

### Consume messages (streaming)

```bash
grpcurl -plaintext -d '{"consumer_id": "consumer-1", "prefetch": 5}' \
  localhost:50051 broker.v1.BrokerService/Consume
```

### Ack a delivery

```bash
grpcurl -plaintext -d '{"delivery_id": "<deliveryID>", "consumer_id": "consumer-1"}' \
  localhost:50051 broker.v1.BrokerService/Ack
```

### Nack a delivery (requeue)

```bash
grpcurl -plaintext -d '{"delivery_id": "<deliveryID>", "consumer_id": "consumer-1", "requeue": true}' \
  localhost:50051 broker.v1.BrokerService/Nack
```

### Check metrics

```bash
# JSON snapshot
curl http://localhost:8080/metrics/json

# Prometheus format
curl http://localhost:8080/metrics
```

---

## Admin CLI

A Python admin CLI is available for broker observability and operational control.
```bash
cd cli
pip install -r requirements.txt

python broker_cli.py metrics          # metrics snapshot
python broker_cli.py health           # health check with thresholds
python broker_cli.py dlq-inspect      # DLQ status
python broker_cli.py dlq-replay       # replay DLQ messages to ready queue
python broker_cli.py dlq-purge        # permanently delete DLQ messages
python broker_cli.py config-validate  # validate configuration
```

See [cli/README.md](cli/README.md) for full documentation.

---

# Architecture

```
Producer
↓
gRPC Publish
↓
Ready Queue
↓ (lease)
Inflight Map
↓
Consumer (gRPC streaming)

Timeout / Nack → Ready or DLQ
```

With WAL enabled:
```
Producer
↓
gRPC Publish
↓
WAL append + fsync
↓
Ready Queue

on restart:
WAL replay
↓
publish without ack → Ready Queue

publish with ack → Done
```

---

# Delivery Model

Each delivery creates a lease:

- `messageID` → stable message identity
- `deliveryID` → ephemeral lease identity
- `deadline` → visibility timeout
- `attempt counter` → retry tracking

Lifecycle:

```
Ready → Inflight → Ack → Done
            ↓
          Timeout / Nack
            ↓
        Ready (with backoff)
            ↓
        MaxRetries → DLQ
```

The broker guarantees **at-least-once delivery**.

---

# Load Test Results

Test environment: local machine, `go run ./cmd/loadtest`

## Scaling Test — 500 messages, prefetch 50

| Consumers | msg/sec | avg(ms) | p50(ms) | p99(ms) | Loss |
|-----------|---------|---------|---------|---------|------|
| 3         | 1,225   | 0.777   | 0.999   | 2.000   | 0%   |
| 5         | 1,220   | 0.741   | 0.609   | 1.660   | 0%   |
| 10        | 1,176   | 0.861   | 0.999   | 2.001   | 0%   |
| 20        | 1,232   | 0.784   | 0.999   | 2.000   | 0%   |
| 50        | 1,139   | 0.873   | 0.999   | 2.002   | 0%   |

## Sustained Load — 5,000 messages, 50 consumers, prefetch 50

| Metric | Result |
|--------|--------|
| Messages published | 5,000 |
| Messages acked | 5,000 |
| Throughput | 1,287 msg/sec |
| Avg ack latency | 0.875ms |
| p99 ack latency | 2.002ms |
| Message loss | 0% |
| Errors | 0 |

## Key Observations

**Throughput is stable across consumer counts.** From 3 to 50 consumers, throughput stays within ~8% — there is no scaling cliff. This is consistent with the single-mutex design: the bottleneck is the gRPC transport layer, not the queue's concurrency model.

**pprof CPU profile at 50 consumers confirmed:**

| Function | CPU% |
|----------|------|
| runtime.cgocall (OS network syscalls) | 63% |
| runtime.procyield (mutex spinning) | 2.4% |
| runtime.lock2 (actual mutex lock) | 0.6% |

Mutex contention accounts for less than 1% of CPU time. The dominant cost is gRPC I/O overhead — each ack is a separate RPC round-trip.

**Prefetch has a significant impact on throughput.** With prefetch=5, throughput drops to ~16 msg/sec due to consumer underutilization. With prefetch=50, throughput reaches ~1,225 msg/sec — a 75x difference with the same 3 consumers. Tuning prefetch to match workload characteristics is critical for performance.

_Tested with `go run ./cmd/loadtest --messages 500 --prefetch 50`_

---

# Current Limitations

- Persistence is optional and currently limited to publish/ack WAL recovery
- Retry attempts, Nack/requeue state, and DLQ contents are not durable yet
- WAL compaction/checkpointing is not implemented yet
- Corrupt tail truncation is not implemented yet
- No partitioning
- No exchange/routing model
- No histogram-based latency buckets (average only)

See [DESIGN.md](DESIGN.md) for detailed trade-offs and architectural reasoning.

---

# Roadmap

- [x] Core delivery semantics
- [x] Retry limit + DLQ
- [x] Exponential backoff on retry
- [x] Consumer disconnect handling
- [x] Graceful shutdown (drain + timeout)
- [x] Observability / metrics
- [x] Structured logging (slog)
- [x] Environment-based configuration
- [x] Python admin CLI (metrics, health, DLQ inspect, config validate)
- [x] Load test with scaling analysis and pprof profiling
- [x] Optional WAL persistence for publish/ack recovery
- [ ] Corrupt WAL tail truncation
- [ ] Retry/Nack/DLQ persistence
- [ ] WAL checkpointing / compaction
- [ ] Per-consumer inflight index (O(k) disconnect)

---

# Learning Focus

This project explores:

- Lease-based message processing
- Backpressure and flow control
- Failure isolation patterns
- Distributed shutdown strategies
- Concurrency design in Go (`sync.Cond` vs channels)
- Structured observability

For detailed design decisions and trade-offs, see:

👉 [DESIGN.md](DESIGN.md)
