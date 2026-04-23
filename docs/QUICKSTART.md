# Quickstart: Canton Ledger to Kafka in 5 Minutes

This guide will walk you through setting up the Canton Event Streaming connector to stream contract events from a local Canton ledger to a Kafka topic.

## Prerequisites

Before you begin, ensure you have the following installed:

-   **[DPM (Canton/Daml SDK)](https://www.digitalasset.com/developers)**: For running a local Canton ledger.
-   **[Docker](https://www.docker.com/get-started/) and Docker Compose**: To run Kafka locally.
-   **[Java 11+](https://adoptium.net/) and [sbt](https://www.scala-sbt.org/)**: To build and run the connector.

## Step 1: Start Canton & Kafka

First, we'll start the necessary infrastructure: a local Canton ledger node and a Kafka cluster.

1.  **Start Kafka using Docker Compose.**

    Create a file named `docker-compose.yml` with the following content:

    ```yaml
    version: '3.8'
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.5.3
        hostname: zookeeper
        container_name: zookeeper
        ports:
          - "2181:2181"
        environment:
          ZOOKEEPER_CLIENT_PORT: 2181
          ZOOKEEPER_TICK_TIME: 2000

      kafka:
        image: confluentinc/cp-kafka:7.5.3
        hostname: kafka
        container_name: kafka
        depends_on:
          - zookeeper
        ports:
          - "9092:9092"
          - "29092:29092"
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
          KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

      kafdrop:
        image: obsidiandynamics/kafdrop:latest
        container_name: kafdrop
        depends_on:
          - kafka
        ports:
          - "9000:9000"
        environment:
          KAFKA_BROKERCONNECT: "kafka:29092"
    ```

    Now, start the services in a terminal:
    ```bash
    docker-compose up -d
    ```

2.  **Start the Canton Sandbox Ledger.**

    Open a new terminal and run:
    ```bash
    dpm sandbox
    ```
    This command starts a local Canton node with both gRPC (port 6866) and JSON API (port 7575) services.

## Step 2: Generate Ledger Events

To have something to stream, we need to create some contracts on our Canton ledger.

1.  **Create a new Daml project.**
    ```bash
    dpm new quickstart-daml
    cd quickstart-daml
    ```

2.  **Define a simple Daml model.**

    Replace the contents of `daml/Main.daml` with this `Iou` template:

    ```daml
    module Main where

    import Daml.Script

    template Iou
      with
        issuer: Party
        owner: Party
        currency: Text
        amount: Decimal
      where
        signatory issuer
        observer owner

        choice Transfer : ContractId Iou
          with
            newOwner: Party
          controller owner
          do
            create this with owner = newOwner
    ```

3.  **Create a script to generate contracts.**

    Create a new file `daml/Test.daml` with the following script:

    ```daml
    module Test where

    import Daml.Script
    import Main

    setup : Script ()
    setup = do
      alice <- allocatePartyWithHint "Alice" (PartyIdHint "alice")
      bob <- allocatePartyWithHint "Bob" (PartyIdHint "bob")

      -- Create some IOUs for Alice
      submit alice do
        createCmd Iou with issuer = alice, owner = alice, currency = "USD", amount = 100.0
      submit alice do
        createCmd Iou with issuer = alice, owner = bob, currency = "EUR", amount = 50.0

      return ()
    ```

4.  **Build the Daml model and run the script.**

    ```bash
    # Build the Daml model into a DAR file
    dpm build

    # Run the script against the sandbox ledger
    dpm script \
      --dar .daml/dist/quickstart-daml-0.1.0.dar \
      --script-name Test:setup \
      --ledger-host localhost \
      --ledger-port 6866
    ```
    This creates two `Iou` contracts on the ledger, one where Alice is the owner and one where Bob is.

## Step 3: Configure and Run the Connector

Now we'll configure and launch the event streaming connector.

1.  **Clone the connector repository (if you haven't already).**

    In a new terminal, outside of your `quickstart-daml` directory:
    ```bash
    git clone https://github.com/digital-asset/canton-event-streaming.git
    cd canton-event-streaming
    ```

2.  **Configure the connector.**

    Create a copy of the example configuration:
    ```bash
    cp config/connector.conf.example config/quickstart.conf
    ```

    Edit `config/quickstart.conf` to connect to our local services and stream events for party `Alice`. Your configuration should look like this:

    ```hocon
    connector {
      name = "canton-quickstart-connector"

      # Canton ledger connection settings
      ledger {
        host = "localhost"
        port = 6866
      }

      # Stream all create and archive events for the party "Alice"
      streaming {
        party-configs = [
          {
            party = "Alice"
            # Route all events from the Main:Iou template to a specific Kafka topic
            template-configs = [
              {
                template-id = "Main:Iou"
                destination-topic = "canton.main.iou"
              }
            ]
          }
        ]
      }

      # Destination sink configuration (Kafka)
      sink {
        type = "kafka"
        kafka {
          bootstrap.servers = "localhost:9092"
          # Other Kafka producer properties can go here
          # See: https://kafka.apache.org/documentation/#producerconfigs
        }
      }

      # State store for offset tracking
      offset-storage {
        type = "file"
        file.path = "quickstart-offsets.json"
      }
    }
    ```
    *Note: The `template-id` does not include the package hash. The connector automatically discovers the correct package ID.*

3.  **Run the connector.**

    ```bash
    # Build the connector application
    sbt stage

    # Run it with our custom configuration
    ./target/universal/stage/bin/connector -Dconfig.file=config/quickstart.conf
    ```

    You should see log output indicating a successful connection to the Canton ledger and that it has started streaming transactions for Alice.

## Step 4: Verify Events in Kafka

Finally, let's confirm that the contract events appeared in our Kafka topic.

1.  **Read from the Kafka topic.**

    Open a new terminal and use the `kafka-console-consumer` tool included in the Docker container to view messages in the `canton.main.iou` topic.

    ```bash
    docker-compose exec kafka kafka-console-consumer \
      --bootstrap-server localhost:9092 \
      --topic canton.main.iou \
      --from-beginning \
      --property print.key=true
    ```

2.  **Inspect the output.**

    You will see the JSON representation of the `Iou` contract create event where Alice is a stakeholder. The key is the contract ID.

    ```json
    # Key (Contract ID)
    #0:12204a1b...

    # Value (Event Payload)
    {"meta":{"eventId":"00/1...","workflowId":"","offset":"00000000000000000003","domainId":"default","contractId":"#0:12204a1b...","templateId":"<hash>:Main:Iou","signatories":["Alice"],"observers":["Alice"],"agreementText":""},"event":{"created":{"payload":{"issuer":"Alice","owner":"Alice","currency":"USD","amount":"100.0"},"key":null}}}
    ```

Congratulations! You have successfully streamed your first event from a Canton ledger to Kafka.

## Next Steps

-   Explore the full set of options in `config/connector.conf.example`.
-   Try streaming to other sinks like **Pulsar** or **AWS Kinesis**.
-   Read the [ARCHITECTURE.md](ARCHITECTURE.md) to understand the connector's design.