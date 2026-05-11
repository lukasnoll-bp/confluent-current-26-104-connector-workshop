#!/usr/bin/env bash
###############################################################################
# IEC 104 Demo – Deploy connectors and ksqlDB streams
# Run AFTER  docker compose up -d  and all services are healthy.
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

CONNECT_SOURCE="http://127.0.0.1:8083"
CONNECT_SINK="http://127.0.0.1:8084"
KSQLDB="http://127.0.0.1:8088"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

###############################################################################
# Helpers
###############################################################################
log()  { printf "${GREEN}[✓]${NC} %s\n" "$*"; }
warn() { printf "${YELLOW}[!]${NC} %s\n" "$*"; }
fail() { printf "${RED}[✗]${NC} %s\n" "$*"; exit 1; }

wait_for_url() {
  local url=$1 name=$2 max=${3:-60} i=0
  printf "    Waiting for %s ..." "$name"
  while ! curl -sf "$url" > /dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge "$max" ]; then
      printf " TIMEOUT\n"
      fail "$name did not become ready within ${max}s"
    fi
    printf "."
    sleep 1
  done
  printf " ready\n"
}

###############################################################################
# 1 — Wait for all services
###############################################################################
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  IEC 104 Demo – Setup"
echo "═══════════════════════════════════════════════════════════"
echo ""

wait_for_url "${CONNECT_SOURCE}/connectors" "Connect-Source" 300
wait_for_url "${CONNECT_SINK}/connectors"   "Connect-Sink"   180
wait_for_url "${KSQLDB}/info"               "ksqlDB"         90

###############################################################################
# 2 — Deploy IEC 104 Source Connector
###############################################################################
echo ""
log "Deploying IEC 104 Source Connector ..."

SOURCE_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${CONNECT_SOURCE}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${PROJECT_DIR}/connectors/iec104-source.json")

if [ "$SOURCE_RESPONSE" = "201" ] || [ "$SOURCE_RESPONSE" = "200" ]; then
  log "Source connector created (HTTP ${SOURCE_RESPONSE})"
elif [ "$SOURCE_RESPONSE" = "409" ]; then
  warn "Source connector already exists (HTTP 409)"
else
  fail "Source connector deployment failed (HTTP ${SOURCE_RESPONSE})"
fi

###############################################################################
# 3 — Wait for the iec104 topic to exist (source connector must be producing)
###############################################################################
echo ""
printf "    Waiting for 'iec104' topic ..."
for i in $(seq 1 60); do
  if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -qx 'iec104'; then
    printf " found\n"
    break
  fi
  if [ "$i" -eq 60 ]; then
    printf " TIMEOUT\n"
    fail "Topic 'iec104' was not created within 60s"
  fi
  printf "."
  sleep 1
done

###############################################################################
# 4 — Deploy ksqlDB streams (flatten objects array)
###############################################################################
echo ""
log "Creating ksqlDB streams ..."

# Read each statement from the SQL file (split on semicolons)
while IFS= read -r stmt; do
  # Skip empty lines
  [ -z "$(echo "$stmt" | tr -d '[:space:]')" ] && continue

  KSQL_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -X POST "${KSQLDB}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
    -d "{\"ksql\": \"$(echo "$stmt" | tr '\n' ' ' | sed 's/"/\\"/g')\", \"streamsProperties\": {}}")

  HTTP_CODE=$(echo "$KSQL_RESPONSE" | tail -1)
  BODY=$(echo "$KSQL_RESPONSE" | sed '$d')

  if echo "$HTTP_CODE" | grep -qE "^2"; then
    log "  ksqlDB statement OK (HTTP ${HTTP_CODE})"
  elif echo "$BODY" | grep -qi "already exists"; then
    warn "  ksqlDB stream already exists"
  else
    warn "  ksqlDB response (HTTP ${HTTP_CODE}): $(echo "$BODY" | head -c 200)"
  fi
done < <(
  # Strip SQL comments and split on semicolons
  # Use $'...' quoting so bash expands \n to a real newline before sed sees it;
  # this makes the split work on both macOS (BSD sed) and Linux (GNU sed).
  sed 's/--.*$//' "${PROJECT_DIR}/ksqldb/statements.sql" \
    | tr '\n' ' ' \
    | sed $'s/;/;\\\n/g' \
    | grep -v '^\s*$'
)

###############################################################################
# 5 — Wait briefly for the IEC104_FLAT topic to be created by ksqlDB
###############################################################################
echo ""
log "Waiting 10s for ksqlDB to materialize IEC104_FLAT topic ..."
sleep 10

###############################################################################
# 6 — Deploy JDBC Sink Connector
###############################################################################
echo ""
log "Deploying JDBC Sink Connector ..."

SINK_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${CONNECT_SINK}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${PROJECT_DIR}/connectors/jdbc-sink.json")

if [ "$SINK_RESPONSE" = "201" ] || [ "$SINK_RESPONSE" = "200" ]; then
  log "Sink connector created (HTTP ${SINK_RESPONSE})"
elif [ "$SINK_RESPONSE" = "409" ]; then
  warn "Sink connector already exists (HTTP 409)"
else
  fail "Sink connector deployment failed (HTTP ${SINK_RESPONSE})"
fi

###############################################################################
# 7 — Summary
###############################################################################
echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Setup complete!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "  Kafka Connect Source : ${CONNECT_SOURCE}/connectors"
echo "    └─ Source status : ${CONNECT_SOURCE}/connectors/iec104-source/status"
echo "  Kafka Connect Sink   : ${CONNECT_SINK}/connectors"
echo "    └─ Sink status   : ${CONNECT_SINK}/connectors/jdbc-sink/status"
echo "  ksqlDB               : ${KSQLDB}/info"
echo "  PostgreSQL           : localhost:5432  (db: iec104, user: iec104)"
echo "  Grafana              : http://localhost:3000"
echo "  IEC104 Simulator UI  : http://localhost:4300"
echo "  IEC104 Simulator API : http://localhost:8080"
echo ""

# To inspect the database manually:
#   docker exec -it postgres psql -U iec104 -d iec104
#   SELECT "COMMONADDRESS", "IOA", "TYPENAME", COUNT(*) FROM iec104_measurements GROUP BY 1,2,3 ORDER BY 1,2;
#   SELECT * FROM iec104_measurements WHERE "COMMONADDRESS" = 1 ORDER BY inserted_at DESC LIMIT 20;
