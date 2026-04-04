# Canton Event Streaming — Setup Guide

## Architecture

```
Canton Ledger → LedgerClient → EventStreamServer (WS) → Browser clients
                             ↘ KafkaProducer → Kafka → Microservices
```

## WebSocket Mode
```bash
npm start -- --mode websocket --port 8080
```
Clients connect to `ws://localhost:8080` and receive JSON events.

## Kafka Mode
```bash
npm start -- --mode kafka --brokers localhost:9092 --topic canton-events
```
Events are published to the `canton-events` Kafka topic.

## Client SDK
```typescript
import { EventStream } from '@canton/event-streaming';

const stream = new EventStream({ ledgerUrl: 'ws://localhost:8080' })
  .on('Created', evt => console.log('New contract:', evt.contractId))
  .on('Archived', evt => console.log('Archived:', evt.contractId))
  .connect();
```
