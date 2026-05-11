# IEC 104 Source Connector Demo — Agent Reference

## Setup

### Prerequisites
- Docker Desktop running
- PowerShell 7+

### Start the stack
```powershell
docker compose up -d
.\scripts\setup.ps1
```

`setup.ps1` waits for all services to be healthy, then deploys the IEC 104 Source Connector, creates the ksqlDB flatten stream, and deploys the JDBC Sink Connector.

### Service URLs

| Service | URL |
|---|---|
| Grafana dashboard | http://localhost:3000 |
| IEC104 Simulator UI | http://localhost:4300 |
| IEC104 Simulator API | http://localhost:8080 |
| Kafka Connect Source | http://localhost:8083/connectors |
| Kafka Connect Sink | http://localhost:8084/connectors |
| ksqlDB | http://localhost:8088/info |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | localhost:5432 — db: `iec104`, user: `iec104`, password: `iec104` |
| Prometheus | http://localhost:9090 |

### Tear down (including volumes)
```powershell
docker compose down -v
```

---

## PostgreSQL Table: `iec104_measurements`

Auto-created by the JDBC Sink Connector (`auto.create=true`). One row per IEC 104 data point received.

| Column | Type | Notes |
|---|---|---|
| `TARGET_ID` | text | Connector target name (e.g. `iec-server`) |
| `TYPEID` | integer | IEC 104 type ID (e.g. 1=M_SP_NA_1, 11=M_ME_NB_1, 13=M_ME_NC_1) |
| `TYPENAME` | text | Human-readable type name |
| `COMMONADDRESS` | integer | Common Address (CA) — identifies the RTU/station |
| `CAUSEOFTRANSMISSION` | integer | IEC 104 COT (1=periodic, 3=spontaneous, 6=interrogation…) |
| `ISTEST` | boolean | Test flag |
| `ISNEGATIVE` | boolean | Negative flag |
| `IOA` | integer | Information Object Address — identifies the data point |
| `TIMESTAMPMS` | bigint | **Always NULL** — non-timestamped type IDs carry no CP56Time2a timestamp |
| `QUALITYFLAGS` | integer | IEC 104 quality descriptor |
| `VALUEFLOAT` | double precision | Used by: M_ME_NB_1 (scaled int), M_ME_NC_1 (float), M_ME_NA_1 (normalized) |
| `VALUEINT` | integer | Reserved — not populated by current simulator |
| `VALUEBOOLEAN` | boolean | Used by: M_SP_NA_1 (single-point), M_DP_NA_1 (double-point) |
| `VALUESTR` | text | Used by double-point states: `ON`, `OFF`, `INTERMEDIATE`, `INDETERMINATE` |
| `inserted_at` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Wall-clock ingestion time — auto-added by event trigger on table creation; use this for all time-range queries |

> **Critical:** `TIMESTAMPMS` is always NULL because the simulator emits non-timestamped IEC 104 type IDs.
> All Grafana time filters must use `inserted_at`, not `TIMESTAMPMS`.
>
> **Persistence:** `inserted_at` is **not** part of the Avro schema written by the JDBC sink. It is added
> automatically via a PostgreSQL event trigger defined in `postgres/init.sql` that fires on `CREATE TABLE`
> for `public.iec104_measurements`. This means it survives `docker compose down -v` restarts without any
> manual intervention.

### Active data points (CA 1 only — CA 20/21 exist in DB but have no dashboard)

| CA | IOA | TYPENAME | Value column | Simulator mode | Description |
|---|---|---|---|---|---|
| 1 | 102 | M_ME_NB_1 | `VALUEFLOAT` | GaussianNoise ~5000 W | Active Power (W) |
| 1 | 103 | M_SP_NA_1 | `VALUEBOOLEAN` | Periodic | Fault Alarm |
| 1 | 104 | M_ME_NC_1 | `VALUEFLOAT` | GaussianNoise ~110 kV | Voltage (kV) |
| 1 | 105 | M_ME_NC_1 | `VALUEFLOAT` | PeriodicWave 0–2800 W | Solar Power Output (W) |
| 1 | 106 | M_ME_NC_1 | `VALUEFLOAT` | RandomWalk 0–3000 W | Wind Power Output (W) |
| 1 | 107 | M_ME_NC_1 | `VALUEFLOAT` | EnergyCounter (linked to IOA 102) | Energy Meter (Wh) |

> **IOA 101 (Transformer Breaker, M_DP_TB_1, Static)** is sent only on interrogation and does not appear in the periodic stream — it is absent from the DB.

---

## Grafana SQL Query Patterns

Datasource UID: `pg-iec104` (PostgreSQL, provisioned automatically).

All column names are **case-sensitive** and must be **double-quoted**.

### Rule 1 — Stat / last-value panels (`format: table`)

Use `ORDER BY inserted_at DESC LIMIT 1`. No time-range filter needed.

```sql
SELECT
  inserted_at AS time,
  "VALUEFLOAT" AS "My Metric"
FROM iec104_measurements
WHERE "COMMONADDRESS" = 1 AND "IOA" = 102
ORDER BY inserted_at DESC
LIMIT 1
```

### Rule 2 — Time-series panels (`format: time_series`)

Alias the timestamp column as `time`. Use `$__timeFrom()` / `$__timeTo()` directly — `inserted_at` is a `TIMESTAMPTZ` so no epoch conversion is needed.

```sql
SELECT
  inserted_at AS time,
  "VALUEFLOAT" AS "Active Power (W)"
FROM iec104_measurements
WHERE "COMMONADDRESS" = 1
  AND "IOA" = 102
  AND inserted_at >= $__timeFrom()
  AND inserted_at <= $__timeTo()
ORDER BY inserted_at
```

### Rule 3 — Boolean → numeric conversion

`VALUEBOOLEAN` cannot be mapped directly by Grafana value mappings. Cast to float first:

```sql
CASE WHEN "VALUEBOOLEAN" = true THEN 1.0 ELSE 0.0 END AS "Alarm"
```

### Rule 4 — Multi-series on one panel

Each series is a separate `target` entry in the panel JSON with a different `refId` (`A`, `B`, …). Both share the same time column alias `time`.

```sql
-- refId A
SELECT inserted_at AS time, "VALUEFLOAT" AS "Solar (W)"
FROM iec104_measurements
WHERE "COMMONADDRESS" = 1 AND "IOA" = 105
  AND inserted_at >= $__timeFrom() AND inserted_at <= $__timeTo()
ORDER BY inserted_at

-- refId B
SELECT inserted_at AS time, "VALUEFLOAT" AS "Wind (W)"
FROM iec104_measurements
WHERE "COMMONADDRESS" = 1 AND "IOA" = 106
  AND inserted_at >= $__timeFrom() AND inserted_at <= $__timeTo()
ORDER BY inserted_at
```

### Do NOT use

```sql
-- WRONG: TIMESTAMPMS is always NULL
WHERE "TIMESTAMPMS" >= $__unixEpochFrom()::BIGINT * 1000

-- WRONG: unquoted column names are lowercased by PostgreSQL and won't match
WHERE COMMONADDRESS = 1
```

---

## Simulator Scenario: `ca1-transformer-trip`

Trigger via the Simulator API:

```
POST http://localhost:8080/api/scenario/ca1-transformer-trip/trigger
```

What happens in the DB:

| Step | IOA | Effect |
|---|---|---|
| 0 ms | 101 | Breaker → INTERMEDIATE (Static, not in DB stream) |
| 2 s | 101 | Breaker → OFF (Static, not in DB stream) |
| 3 s | 102 | Active Power → 0 W (visible in DB) |
| 3.5 s | 103 | Fault Alarm → true (visible in DB) |
| 5.5 s | 104 | Voltage → 0.0 kV (visible in DB) |

Recovery (after `RecoveryMs: 10000`): all values restored to baseline, alarms cleared.
