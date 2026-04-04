type EventType = 'Created' | 'Archived';
type EventHandler = (event: LedgerEvent) => void;

export interface LedgerEvent {
  type: EventType;
  contractId: string;
  templateId: string;
  payload?: Record<string, unknown>;
  offset: string;
}

export interface StreamOptions {
  ledgerUrl: string;
  token?: string;
  templateFilter?: string[];
  partyFilter?: string[];
  reconnectIntervalMs?: number;
}

export class EventStream {
  private ws: WebSocket | null = null;
  private handlers = new Map<EventType | '*', EventHandler[]>();
  private reconnect: boolean = true;

  constructor(private readonly opts: StreamOptions) {}

  on(event: EventType | '*', handler: EventHandler): this {
    const list = this.handlers.get(event) ?? [];
    this.handlers.set(event, [...list, handler]);
    return this;
  }

  connect(): this {
    const url = new URL(this.opts.ledgerUrl);
    if (this.opts.token) url.searchParams.set('token', this.opts.token);
    this.ws = new WebSocket(url.toString());

    this.ws.onmessage = ({ data }) => {
      const event: LedgerEvent = JSON.parse(data as string);
      (this.handlers.get(event.type) ?? []).forEach(h => h(event));
      (this.handlers.get('*') ?? []).forEach(h => h(event));
    };

    this.ws.onclose = () => {
      if (this.reconnect) {
        setTimeout(() => this.connect(), this.opts.reconnectIntervalMs ?? 3000);
      }
    };

    return this;
  }

  disconnect(): void {
    this.reconnect = false;
    this.ws?.close();
  }
}
