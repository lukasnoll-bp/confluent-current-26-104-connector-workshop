# IEC 104 Kafka Source Connector – Demo

End-to-end demo: IEC 104 Simulator → Kafka Connect Source → ksqlDB (flatten) → JDBC Sink → PostgreSQL → Grafana.

## Architecture

```
IEC 104 Simulator (2404 / 8080)
       │ IEC 104
       ▼
Kafka Connect Source ──► topic: iec104 ──► ksqlDB (UNNEST objects[])
                                                    │
                                            topic: IEC104_FLAT
                                                    │
                                                    ▼
                                          Kafka Connect Sink ──► PostgreSQL
                                                                     │
                                                                  Grafana
```

## Prerequisites

- Docker & Docker Compose v2+
- `curl` and `jq` (for the setup script)

## Quick Start

### 1. Start the stack

```bash
docker compose up -d
```

Wait until all services are healthy:

```bash
docker compose ps
```

### 2. Deploy connectors & streams

#### unix

```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

#### windows

```powershell
./scripts/setup.ps1
```

This will:
1. Wait for Kafka Connect (source + sink) and ksqlDB to be ready
2. Deploy the IEC 104 Source Connector
3. Create ksqlDB streams to flatten the nested `objects[]` array
4. Deploy the JDBC Sink Connector writing to PostgreSQL

### 3. Verify

```bash
# Source connector status
curl -s localhost:8083/connectors/iec104-source/status | jq

# Sink connector status
curl -s localhost:8084/connectors/jdbc-sink/status | jq

# Row count in PostgreSQL
docker exec -it postgres psql -U iec104 -d iec104 \
  -c 'SELECT count(*) FROM iec104_measurements;'
```

### 4. Open dashboards

| Service              | URL                        | Credentials  |
|----------------------|----------------------------|--------------|
| **Grafana**          | http://localhost:3000       | admin/admin  |
| **Simulator**        | http://localhost:8080       | –            |
| **ksqlDB**           | http://localhost:8088/info  | –            |
| **Connect Source**   | http://localhost:8083       | –            |
| **Connect Sink**     | http://localhost:8084       | –            |
| **Prometheus**       | http://localhost:9090       | –            |
| **JMX Exporter**     | http://localhost:9404/metrics | –          |

## Tear Down

```bash
docker compose down -v
```

## Connector Metrics Dashboard

A second Grafana dashboard (**IEC 104 Connector Metrics**) shows technical health
and throughput metrics from the source connector via Prometheus + JMX exporter.

### How it works

The JMX Prometheus JavaAgent (`jmx-exporter/jmx_prometheus_javaagent-1.0.1.jar`)
runs inside the Connect source container and exposes Kafka Connect and custom
IEC 104 connector MBeans as Prometheus metrics on port `9404`. Prometheus scrapes
these every 5 seconds.

### Dashboard panels

| Row                      | Panels                                                  | Source     |
|--------------------------|---------------------------------------------------------|------------|
| Task Health              | Total / Running / Paused / Failed Tasks                 | Prometheus |
| Message Throughput       | Records Read Rate, Records Read Total, Buffer Size      | Prometheus |
| Messages by CommonAddress| Messages per CommonAddress over time, Unique CA count   | PostgreSQL |
| NR Messages over Time    | Total message rate, Message rate per CommonAddress       | PostgreSQL |

All Prometheus panels support filtering by **Target (IEC 104 Server)** via a
dropdown variable populated from the `target_id` label.

### JMX Exporter JAR

The JAR is included in `jmx-exporter/` for zero-friction setup. To update it,
download a newer version from
[Maven Central](https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/)
and update the `KAFKA_OPTS` path in `docker-compose.yml`.

## Services

| Container          | Image                                  | Ports       |
|--------------------|----------------------------------------|-------------|
| kafka              | confluentinc/cp-kafka:8.2.0           | 9092, 29092 |
| connect-source     | *Your image*                           | 8083, 9404  |
| connect-sink       | confluentinc/cp-kafka-connect:8.2.0   | 8084        |
| ksqldb-server      | confluentinc/cp-ksqldb-server:8.2.0   | 8088        |
| postgres           | postgres:16                            | 5432        |
| prometheus         | prom/prometheus:latest                 | 9090        |
| grafana            | grafana/grafana-oss:11.6.0            | 3000        |
| iec104-simulator   | *Your image*                           | 2404, 8080  |

## Why ksqlDB?

The source connector produces messages with an `objects` field of type
`ARRAY<STRUCT>`. The Confluent JDBC Sink Connector only supports primitive
types — it cannot sink arrays or nested structs. ksqlDB's
`CROSS JOIN UNNEST(objects)` explodes each array element into a flat record
on the `IEC104_FLAT` topic, which the JDBC Sink can write directly to PostgreSQL.

## Grafana Dashboard Panels

| Row                | Panels                                        | IOAs                    |
|--------------------|-----------------------------------------------|-------------------------|
| Grid Overview      | Bus Voltages, Frequency gauge, Oil Temp gauge | 2001, 2002, 2003, 2014  |
| Transformer        | XFMR-1 Currents, XFMR-1 Power                | 2010, 2011, 2012, 2013  |
| Feeders            | Feeder Currents, Feeder Active Power          | 2020, 2021, 2022, 2023  |
| Renewables         | Solar PV, Wind Farm                           | 2030, 2031              |
| Energy Counters    | FEEDER-1/2 Energy, XFMR-1 Total Energy       | 3001, 3002, 3003        |
| Protection & Status| Breaker Status table, Alarms table            | 1001–1022, 1003–1031    |


