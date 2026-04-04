import { Kafka, Producer } from 'kafkajs';

interface LedgerEvent {
  type: string;
  contractId: string;
  templateId: string;
  payload?: unknown;
  offset: string;
}

export class KafkaEventProducer {
  private producer: Producer;

  constructor(
    private readonly brokers: string[],
    private readonly topic: string = 'canton-ledger-events',
  ) {
    const kafka = new Kafka({ clientId: 'canton-event-streaming', brokers });
    this.producer = kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    console.log(`Kafka producer connected → topic: ${this.topic}`);
  }

  async publish(event: LedgerEvent): Promise<void> {
    await this.producer.send({
      topic: this.topic,
      messages: [{
        key: event.contractId,
        value: JSON.stringify(event),
        headers: { templateId: event.templateId, eventType: event.type },
      }],
    });
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }
}
