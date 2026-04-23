#!/usr/bin/env bash
#
# =====================================================================================
# canton-event-streaming Smoke Test
# =====================================================================================
#
# This script performs an end-to-end smoke test of the Ledger API to Kafka streaming
# connector. It verifies that Daml ledger events are correctly published to a Kafka
# topic.
#
# The test performs the following steps:
# 1. Checks for required tools (dpm, docker, sbt, kcat).
# 2. Builds the Daml model (DAR) and the Scala connector (fat JAR).
# 3. Starts dependencies: Canton Sandbox and a Kafka cluster using Docker Compose.
# 4. Starts the canton-event-streaming connector application.
# 5. Generates ledger events by running a Daml script (creates and exercises a contract).
# 6. Consumes messages from the target Kafka topic using kcat.
# 7. Verifies that the consumed messages match the expected ledger events.
# 8. Cleans up all running processes and Docker containers.
#
# The script will exit with a non-zero status code if any step fails.
#
# Usage:
#   ./scripts/smoke-test.sh
#
# =====================================================================================

set -euo pipefail

# --- Configuration ---
PROJECT_NAME="canton-event-streaming"
DAML_SOURCE="daml"
KAFKA_TOPIC="canton-ledger-events"
KAFKA_BROKER="localhost:9092"
DOCKER_COMPOSE_FILE="docker/docker-compose.yml"
CANTON_LEDGER_HOST="localhost"
CANTON_LEDGER_PORT=6866
EXPECTED_EVENTS=2 # 1 create, 1 exercise

# --- State Variables ---
CANTON_PID=""
CONNECTOR_PID=""

# --- Color Codes for Logging ---
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

info() {
  echo -e "${BLUE}INFO:${NC} $1"
}

success() {
  echo -e "${GREEN}SUCCESS:${NC} $1"
}

warn() {
  echo -e "${YELLOW}WARN:${NC} $1"
}

error() {
  echo -e "${RED}ERROR:${NC} $1" >&2
}

# --- Cleanup Function ---
# Ensures all background processes and containers are stopped on exit.
cleanup() {
  info "Initiating cleanup..."

  if [[ -n "${CONNECTOR_PID}" ]]; then
    info "Stopping event connector (PID: ${CONNECTOR_PID})..."
    kill "${CONNECTOR_PID}" 2>/dev/null || true
  fi

  if [[ -n "${CANTON_PID}" ]]; then
    info "Stopping Canton Sandbox (PID: ${CANTON_PID})..."
    kill "${CANTON_PID}" 2>/dev/null || true
  fi

  if [ -f "$DOCKER_COMPOSE_FILE" ]; then
    info "Stopping Kafka cluster..."
    docker compose -f "$DOCKER_COMPOSE_FILE" down -v --remove-orphans
  fi

  info "Cleanup complete."
}
trap cleanup EXIT

# --- Prerequisite Checks ---
check_tool() {
  if ! command -v "$1" &> /dev/null; then
    error "Required tool '$1' is not installed or not in PATH. Please install it to continue."
    exit 1
  fi
}

info "Checking for required tools..."
check_tool dpm
check_tool docker
check_tool sbt
check_tool kcat
check_tool jq

# --- Build ---
info "Building Daml model and Scala connector..."
dpm build
sbt assembly
DAR_PATH=$(find .daml/dist -name "*.dar" | head -n 1)
JAR_PATH=$(find target/scala-*/ -name "*-assembly-*.jar" | head -n 1)

if [[ -z "$DAR_PATH" || -z "$JAR_PATH" ]]; then
  error "Build failed. Could not find DAR or Assembly JAR."
  exit 1
fi
success "Build artifacts created successfully."
info "DAR: $DAR_PATH"
info "JAR: $JAR_PATH"

# --- Start Dependencies ---
info "Starting Kafka and Zookeeper via Docker Compose..."
docker compose -f "$DOCKER_COMPOSE_FILE" up -d

info "Waiting for Kafka broker to be ready..."
# Use kcat to poll for the broker's availability
timeout 60s bash -c '
  until kcat -b '"$KAFKA_BROKER"' -L -t '"$KAFKA_TOPIC"'; do
    echo "Waiting for Kafka broker at '"$KAFKA_BROKER"'..."
    sleep 2
  done
'
success "Kafka is ready."

info "Starting Canton Sandbox..."
dpm sandbox --dar "$DAR_PATH" &
CANTON_PID=$!
sleep 5 # Give it a moment to start up

info "Waiting for Canton Sandbox to be ready..."
timeout 60s bash -c '
  until curl -s -f http://'"$CANTON_LEDGER_HOST"':7575/v1/health; do
    echo "Waiting for Canton Sandbox JSON API..."
    sleep 2
  done
'
success "Canton Sandbox is ready."

# --- Start Connector ---
info "Starting the event streaming connector..."
java -jar "$JAR_PATH" &
CONNECTOR_PID=$!
sleep 10 # Allow time for the connector to initialize and connect to Kafka/Canton

# --- Generate Ledger Activity ---
info "Running Daml script to generate ledger events..."
# This assumes a script named `SmokeTest:run` exists in the Daml model.
# This script should create one contract and exercise one choice.
dpm script \
  --dar "$DAR_PATH" \
  --script-name SmokeTest:run \
  --ledger-host "$CANTON_LEDGER_HOST" \
  --ledger-port "$CANTON_LEDGER_PORT"
success "Daml script executed, events generated."

# --- Verify Events in Kafka ---
info "Consuming events from Kafka topic '$KAFKA_TOPIC'..."

# Consume the expected number of messages, waiting up to 30 seconds.
# The output is captured for verification.
CONSUMED_EVENTS=$(kcat -b "$KAFKA_BROKER" -t "$KAFKA_TOPIC" -C -o beginning -c "$EXPECTED_EVENTS" -e -J -q -w 30)

if [[ -z "$CONSUMED_EVENTS" ]]; then
  error "Test failed: No events were consumed from Kafka topic '$KAFKA_TOPIC'."
  exit 1
fi

info "Consumed ${EXPECTED_EVENTS} messages. Verifying content..."

# Verification logic using jq
# Check 1: One 'created' event for the SmokeTest:Contract template
CREATED_COUNT=$(echo "$CONSUMED_EVENTS" | jq -r '
  select(.created.templateId | contains("SmokeTest:Contract")) | .created.templateId
' | wc -l)

if [[ "$CREATED_COUNT" -ne 1 ]]; then
  error "Test failed: Expected 1 'created' event for 'SmokeTest:Contract', but found ${CREATED_COUNT}."
  echo "--- Consumed Events ---"
  echo "$CONSUMED_EVENTS" | jq
  echo "-----------------------"
  exit 1
fi
success "Verified 1 'created' event."

# Check 2: One 'exercised' event for the 'Increment' choice
EXERCISED_COUNT=$(echo "$CONSUMED_EVENTS" | jq -r '
  select(.exercised.templateId | contains("SmokeTest:Contract")) | select(.exercised.choice == "Increment") | .exercised.choice
' | wc -l)

if [[ "$EXERCISED_COUNT" -ne 1 ]]; then
  error "Test failed: Expected 1 'exercised' event for choice 'Increment', but found ${EXERCISED_COUNT}."
  echo "--- Consumed Events ---"
  echo "$CONSUMED_EVENTS" | jq
  echo "-----------------------"
  exit 1
fi
success "Verified 1 'exercised' event."


# --- All Done ---
echo
success "Smoke test PASSED. End-to-end event flow verified."
echo