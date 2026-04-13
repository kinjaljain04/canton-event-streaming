#!/bin/bash
#
# ====================================================================================
# Smoke Test for canton-event-streaming
# ====================================================================================
# This script performs an end-to-end test of the event streaming connector.
# It orchestrates the following components:
#
# 1. Starts a local Kafka cluster using Docker Compose.
# 2. Starts a local Canton Sandbox ledger (`dpm sandbox`).
# 3. Builds a simple test Daml model.
# 4. Allocates a party on the sandbox.
# 5. Starts the canton-event-streaming connector, configured to link the
#    sandbox and the Kafka topic.
# 6. Runs a Daml script to create a contract, generating a ledger event.
# 7. Consumes the event from the Kafka topic.
# 8. Verifies that the consumed event data matches the created contract data.
# 9. Cleans up all resources (Docker containers, background processes).
#
# Prerequisites:
# - Docker & Docker Compose
# - Canton SDK (dpm command)
# - jq
# - A compiled connector JAR (e.g., via `sbt assembly` or `mvn package`)
# - A test Daml model located in `test-daml-model/`
# - A Docker compose file for Kafka at `docker/docker-compose-kafka.yml`
# ====================================================================================

set -euo pipefail

# Ensure the script is run from the project root directory.
cd "$(dirname "$0")/.."

# --- Configuration ---
readonly PROJECT_NAME="canton-event-streaming"
readonly KAFKA_TOPIC="canton-ledger-events-smoke-test"
readonly DOCKER_COMPOSE_FILE="docker/docker-compose-kafka.yml"
readonly DAML_PROJECT_PATH="test-daml-model"
readonly TEST_DAMLSCRIPT_NAME="SmokeTest:createInstrument"

# Find the built connector JAR. Adjust the path if your build tool places it elsewhere.
readonly CONNECTOR_JAR=$(find ./target -name "${PROJECT_NAME}*.jar" -print -quit)

# Temporary files for logs and configs
readonly CONNECTOR_CONFIG_FILE=$(mktemp "/tmp/connector.conf.XXXXXX")
readonly CONNECTOR_LOG_FILE=$(mktemp "/tmp/connector.log.XXXXXX")
readonly SANDBOX_LOG_FILE=$(mktemp "/tmp/sandbox.log.XXXXXX")

# Data for the test contract
readonly TEST_TICKER="SMOKETEST_ACME_$(date +%s)"
readonly TEST_PRICE="123.45"
readonly TEST_PARTY_NAME="Operator"

# --- Logging and Cleanup ---
log_step() {
  echo -e "\n\033[1;34m▶ $1\033[0m"
}

log_info() {
  echo "  $1"
}

log_error() {
  echo -e "\033[1;31m✖ ERROR: $1\033[0m" >&2
  # If in CI, dump logs for debugging
  if [[ -n "${CI:-}" ]]; then
    echo "--- Sandbox Log ---"
    cat "$SANDBOX_LOG_FILE"
    echo "--- Connector Log ---"
    cat "$CONNECTOR_LOG_FILE"
  fi
  exit 1
}

cleanup() {
  log_step "Cleaning up resources..."
  # Stop background processes by killing the process group
  if [[ -n "${BG_PIDS:-}" ]]; then
    kill $BG_PIDS 2>/dev/null || true
  fi

  # Stop Docker containers
  if [ -f "$DOCKER_COMPOSE_FILE" ]; then
    docker-compose -f "$DOCKER_COMPOSE_FILE" down -v --remove-orphans --timeout 10
  fi

  # Remove temporary files
  rm -f "$CONNECTOR_CONFIG_FILE" "$CONNECTOR_LOG_FILE" "$SANDBOX_LOG_FILE"
  log_info "Cleanup complete."
}
trap cleanup EXIT

# --- Main Script ---

log_step "Starting Smoke Test for Canton Event Streaming"
BG_PIDS=""

# 1. Prerequisites Check
log_step "Checking prerequisites..."
command -v dpm >/dev/null 2>&1 || log_error "dpm is not installed. Please install the Canton SDK."
command -v docker-compose >/dev/null 2>&1 || log_error "docker-compose is not installed."
command -v jq >/dev/null 2>&1 || log_error "jq is not installed."
[ -f "$DOCKER_COMPOSE_FILE" ] || log_error "Docker compose file not found at $DOCKER_COMPOSE_FILE"
[ -d "$DAML_PROJECT_PATH" ] || log_error "Test Daml model not found at $DAML_PROJECT_PATH"
[ -f "$CONNECTOR_JAR" ] || log_error "Connector JAR not found. Please build the project first."
log_info "All prerequisites are met."

# 2. Setup Infrastructure (Kafka, Sandbox)
log_step "Setting up infrastructure: Kafka and Canton Sandbox..."
log_info "Starting Kafka and Zookeeper with Docker Compose..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d
log_info "Waiting for Kafka to become available..."
timeout 60s bash -c 'until docker-compose -f '"$DOCKER_COMPOSE_FILE"' exec kafka kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do sleep 2; done' \
  || log_error "Kafka did not become available in time."

log_info "Creating Kafka topic: $KAFKA_TOPIC"
docker-compose -f "$DOCKER_COMPOSE_FILE" exec kafka kafka-topics \
  --create --topic "$KAFKA_TOPIC" --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

log_info "Starting Canton Sandbox in the background..."
dpm sandbox --log-level=info > "$SANDBOX_LOG_FILE" 2>&1 &
SANDBOX_PID=$!
BG_PIDS+=" $SANDBOX_PID"
log_info "Waiting for Sandbox gRPC port (6866) to be ready..."
timeout 60s bash -c 'until lsof -i :6866 -sTCP:LISTEN; do sleep 1; done' \
  || (log_error "Sandbox failed to start. See log: $SANDBOX_LOG_FILE")
log_info "Sandbox is running (PID: $SANDBOX_PID)."

# 3. Prepare Daml Model and Parties
log_step "Preparing Daml model and allocating parties..."
log_info "Building test Daml model..."
dpm build --project-root "$DAML_PROJECT_PATH"

readonly TEST_DAR_FILE=$(find "$DAML_PROJECT_PATH/.daml/dist" -name "*.dar" | head -n 1)
[ -f "$TEST_DAR_FILE" ] || log_error "Test model DAR file not found in $DAML_PROJECT_PATH/.daml/dist"

log_info "Allocating party '$TEST_PARTY_NAME'..."
OPERATOR_PARTY_ID=$(dpm ledger allocate-parties --participant-host=localhost --participant-port=6866 "$TEST_PARTY_NAME" | jq -r ".${TEST_PARTY_NAME}")
[ -n "$OPERATOR_PARTY_ID" ] || log_error "Failed to allocate party."
log_info "Allocated party '$TEST_PARTY_NAME' with ID: $OPERATOR_PARTY_ID"

# 4. Configure and Start the Connector
log_step "Configuring and starting the event streaming connector..."
cat > "$CONNECTOR_CONFIG_FILE" <<EOL
connector {
  name = "smoke-test-connector"
  ledger {
    host = "localhost"
    port = 6866
  }
  streaming {
    type = "kafka"
    topic = "$KAFKA_TOPIC"
    properties {
      "bootstrap.servers" = "localhost:9092"
      "client.id" = "canton-event-streaming-smoke-test-producer"
      "acks" = "all"
    }
  }
  filter {
    parties = ["$OPERATOR_PARTY_ID"]
    templates = ["SmokeTest:Instrument"]
  }
  # Start from the beginning of the ledger for this test run
  start-from = "ledger-begin"
}
EOL

java -jar "$CONNECTOR_JAR" --config "$CONNECTOR_CONFIG_FILE" > "$CONNECTOR_LOG_FILE" 2>&1 &
CONNECTOR_PID=$!
BG_PIDS+=" $CONNECTOR_PID"
log_info "Connector started (PID: $CONNECTOR_PID). Waiting for it to connect to Kafka..."
timeout 45s bash -c "until grep -q -i 'connected to kafka' '$CONNECTOR_LOG_FILE'; do sleep 1; done" \
    || (log_error "Connector failed to start or connect to Kafka. See log: $CONNECTOR_LOG_FILE")
log_info "Connector is running and connected."

# 5. Generate a Ledger Event
log_step "Generating a ledger event by creating a contract..."
dpm script \
  --dar "$TEST_DAR_FILE" \
  --script-name "$TEST_DAMLSCRIPT_NAME" \
  --ledger-host localhost \
  --ledger-port 6866 \
  --input-file <(printf '{"operator": "%s", "ticker": "%s", "price": "%s"}' "$OPERATOR_PARTY_ID" "$TEST_TICKER" "$TEST_PRICE") \
  > /dev/null # Suppress script result output
log_info "Contract creation command submitted for ticker $TEST_TICKER."

# 6. Verify Event Delivery
log_step "Verifying event delivery via Kafka..."
log_info "Consuming one message from topic '$KAFKA_TOPIC' (timeout: 30s)..."
KAFKA_OUTPUT=$(timeout 30s docker-compose -f "$DOCKER_COMPOSE_FILE" exec kafka \
  kafka-console-consumer --bootstrap-server kafka:29092 --topic "$KAFKA_TOPIC" --from-beginning --max-messages 1)

if [ -z "$KAFKA_OUTPUT" ]; then
  log_error "Test failed: Timed out waiting for message from Kafka. No event received."
fi

log_info "Received message from Kafka:"
echo "$KAFKA_OUTPUT" | jq .

# Assuming the connector formats events as: { "created": { "payload": { ... } } }
RECEIVED_TICKER=$(echo "$KAFKA_OUTPUT" | jq -r '.created.payload.ticker')
RECEIVED_PRICE=$(echo "$KAFKA_OUTPUT" | jq -r '.created.payload.price')

log_info "Verifying received data..."
log_info "  Expected Ticker: $TEST_TICKER"
log_info "  Received Ticker: $RECEIVED_TICKER"
log_info "  Expected Price:  $TEST_PRICE"
log_info "  Received Price:  $RECEIVED_PRICE"

if [[ "$RECEIVED_TICKER" == "$TEST_TICKER" && "$RECEIVED_PRICE" == "$TEST_PRICE" ]]; then
  echo -e "\n\033[1;32m✅ SMOKE TEST PASSED: Event successfully streamed from Canton to Kafka.\033[0m"
else
  log_error "SMOKE TEST FAILED: Received data does not match expected data."
fi

exit 0