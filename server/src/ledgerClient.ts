import axios from 'axios';

type EventCallback = (event: Record<string, unknown>) => void;

export class LedgerClient {
  private offset = '000000000000000000';

  constructor(
    private readonly baseUrl: string,
    private readonly token: string,
    private readonly batchSize = 100,
  ) {}

  async subscribeToEvents(callback: EventCallback): Promise<void> {
    while (true) {
      try {
        const res = await axios.get(`${this.baseUrl}/v1/stream/query`, {
          headers: { Authorization: `Bearer ${this.token}` },
          params: { offset: this.offset, limit: this.batchSize },
          timeout: 30_000,
        });

        const events = res.data?.result ?? [];
        for (const event of events) {
          callback(event);
          if (event.offset) this.offset = event.offset;
        }

        if (events.length < this.batchSize) {
          await new Promise(r => setTimeout(r, 500)); // backpressure: wait when idle
        }
      } catch (err) {
        await new Promise(r => setTimeout(r, 2000)); // reconnect delay
      }
    }
  }
}
