#!/usr/bin/env bash
set -euo pipefail

START_OFFSET="${1:-000000000000000000}"
END_OFFSET="${2:-}"
BASE="http://${CANTON_HOST:-localhost}:${CANTON_PORT:-7575}"
TOKEN="${CANTON_JWT:-}"

echo "Canton Event Streaming — Replay from offset $START_OFFSET"

PARAMS="offset=$START_OFFSET"
[[ -n "$END_OFFSET" ]] && PARAMS="$PARAMS&endOffset=$END_OFFSET"

curl -sf "$BASE/v1/stream/query?$PARAMS" \
  -H "Authorization: Bearer $TOKEN" | \
  node dist/replay/consumer.js
