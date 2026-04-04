import { WebSocketServer, WebSocket } from 'ws';
import { LedgerClient } from './ledgerClient';

interface LedgerEvent {
  type: 'Created' | 'Archived';
  contractId: string;
  templateId: string;
  payload?: Record<string, unknown>;
  offset: string;
}

export class EventStreamServer {
  private wss: WebSocketServer;
  private clients = new Set<WebSocket>();

  constructor(private readonly port: number, private readonly ledger: LedgerClient) {
    this.wss = new WebSocketServer({ port });
    this.wss.on('connection', (ws) => {
      this.clients.add(ws);
      ws.on('close', () => this.clients.delete(ws));
    });
  }

  async start(): Promise<void> {
    await this.ledger.subscribeToEvents((event: LedgerEvent) => {
      const msg = JSON.stringify(event);
      for (const ws of this.clients) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(msg);
        }
      }
    });
    console.log(`Event stream server listening on ws://localhost:${this.port}`);
  }

  stop(): void {
    this.wss.close();
  }
}
