# Kafka Integration Guide

## Topic schema
Each message on `canton-ledger-events`:
```json
{
  "type": "Created",
  "contractId": "00abc...",
  "templateId": "MyApp:MyModule:MyTemplate",
  "payload": { ... },
  "offset": "000000000000000042"
}
```

## Consumer example (Node.js)
```typescript
import { Kafka } from 'kafkajs';

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'my-app' });

await consumer.connect();
await consumer.subscribe({ topic: 'canton-ledger-events' });
await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value!.toString());
    console.log(event.type, event.contractId);
  },
});
```
