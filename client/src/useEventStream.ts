import { useEffect, useRef, useState, useCallback } from 'react';
import { EventStream, LedgerEvent, StreamOptions } from './eventStream';

export function useEventStream(opts: StreamOptions) {
  const streamRef = useRef<EventStream | null>(null);
  const [events, setEvents] = useState<LedgerEvent[]>([]);
  const [connected, setConnected] = useState(false);

  const push = useCallback((event: LedgerEvent) => {
    setEvents(prev => [event, ...prev].slice(0, 200));
  }, []);

  useEffect(() => {
    const stream = new EventStream(opts);
    streamRef.current = stream;

    stream.on('*', push);
    stream.connect();
    setConnected(true);

    return () => {
      stream.disconnect();
      setConnected(false);
    };
  }, [opts.ledgerUrl]);

  return { events, connected };
}
