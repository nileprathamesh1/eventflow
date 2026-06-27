# EventFlow

A production-grade real-time event processing platform with full observability stack.

## Architecture

```
Client → REST API → Redis (rate limit + dedup) → Kafka (3 partitions) → Consumer → PostgreSQL
                                                        ↓ on failure (after 2 retries)
                                                    Dead Letter Topic (DLT)

Prometheus ← scrapes /actuator/prometheus every 5s ← Spring Boot Actuator
Grafana    ← queries Prometheus → live dashboard (throughput, latency, errors)
```

## Stack

| Layer | Technology |
|---|---|
| API | Spring Boot 3, REST |
| Messaging | Apache Kafka (3 partitions, idempotent producer) |
| Caching / Rate limiting | Redis |
| Persistence | PostgreSQL + Spring Data JPA |
| Observability | Micrometer + Prometheus + Grafana |
| Containerisation | Docker Compose |
| Testing | JUnit 5, Mockito, Testcontainers |

## Key design decisions

**Idempotent Kafka producer**
`enable.idempotence=true` + `acks=all` — the broker assigns sequence numbers to messages so duplicate retries are automatically discarded. Guarantees exactly-once delivery to Kafka.

**Partition key = source**
Events from the same source always land on the same partition, preserving per-source ordering without a global ordering guarantee (which would require a single partition and kill throughput).

**Redis rate limiting**
Atomic increment + TTL pattern. The first request in a 60-second window sets the counter and TTL. Subsequent requests just increment. No locks, no race conditions.

**Redis deduplication**
EventId stored in Redis with 24-hour TTL on first seen. Protects against double-processing when clients retry failed requests.

**Dead Letter Topic + retry backoff**
Spring Kafka's `DefaultErrorHandler` retries failed consumer messages twice with a 2-second backoff. After exhausting retries, messages route to `events.DLT` with original exception headers for debugging.

**Latency histograms**
`@Timed` on the publish endpoint + `percentiles-histogram: true` in config generates p50/p95/p99 buckets that Prometheus scrapes and Grafana visualises in real time.

## Running locally

Prerequisites: Docker Desktop

```bash
docker-compose up --build
```

| Service | URL |
|---|---|
| EventFlow API | http://localhost:8080 |
| Grafana Dashboard | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |
| Actuator Health | http://localhost:8080/actuator/health |
| Prometheus Metrics | http://localhost:8080/actuator/prometheus |

## Load testing

```bash
pip install requests
python3 load_test.py                        # 1000 events, 20 threads
python3 load_test.py --events 5000 --threads 50
```

Sample output:
```
EventFlow Load Test
  Target : http://localhost:8080/api/events
  Events : 1000
  Threads: 20

  RESULTS
  Total sent      : 1000
  Successful      : 1000
  Failed          : 0
  Elapsed time    : 3.82s
  Throughput      : 261.8 events/sec
  Latency p50     : 68.4 ms
  Latency p95     : 143.2 ms
  Latency p99     : 198.7 ms
```

## API

**Publish an event**
```
POST /api/events
{ "type": "user.signup", "source": "auth-service", "payload": { "userId": "123" } }

202: { "eventId": "uuid", "status": "queued" }
429: { "error": "Rate limit exceeded for source: auth-service" }
400: { "error": "type: Event type is required" }
```

**Query events**
```
GET /api/events
GET /api/events/{eventId}
GET /api/events/source/{source}
```

## Running tests

```bash
mvn test
```

Docker must be running — integration tests spin up real containers via Testcontainers.

## Project structure

```
src/main/java/com/eventflow/
├── controller/     # REST API + @Timed latency tracking + global exception handler
├── service/        # EventService (publish + rate limit check), RedisService
├── consumer/       # Kafka listener with metrics counters + DLT listener
├── model/          # Event JPA entity
├── repository/     # Spring Data JPA
├── dto/            # EventRequest, EventMessage
└── config/         # Kafka topics + DLT error handler

monitoring/
├── prometheus.yml                          # Prometheus scrape config
└── grafana/
    ├── provisioning/datasources/           # Auto-connects Grafana to Prometheus
    ├── provisioning/dashboards/            # Auto-loads dashboard on startup
    └── dashboards/eventflow.json           # Pre-built EventFlow dashboard

load_test.py                                # Benchmarking script
```
