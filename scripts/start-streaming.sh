#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-websocket}"
PORT="${PORT:-8080}"
BROKERS="${KAFKA_BROKERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-canton-ledger-events}"

echo "Canton Event Streaming — Starting ($MODE mode)"
echo "================================================"

case "$MODE" in
  websocket)
    echo "WebSocket server → ws://localhost:$PORT"
    node dist/server/main.js --mode websocket --port "$PORT"
    ;;
  kafka)
    echo "Kafka producer → $BROKERS ($TOPIC)"
    node dist/server/main.js --mode kafka --brokers "$BROKERS" --topic "$TOPIC"
    ;;
  *)
    echo "Unknown mode: $MODE  (use: websocket | kafka)"
    exit 1
    ;;
esac
