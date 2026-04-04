# Canton Event Streaming

Real-time Canton ledger event streaming library supporting WebSocket and Kafka consumers.

## Features
- WebSocket server broadcasting ledger events in real-time
- Kafka producer for event fan-out at scale
- Daml trigger for on-chain event emission
- TypeScript client SDK with typed event models
- React hook for live UI updates
- Filtering by template, party, or contract ID

## Quick Start
```bash
npm install @canton/event-streaming
```

```typescript
import { EventStream } from '@canton/event-streaming';
const stream = new EventStream({ ledgerUrl: 'ws://localhost:7575' });
stream.on('Created', evt => console.log(evt.contractId));
```
