import React from 'react';
import { useEventStream } from '../../client/src/useEventStream';

export const EventFeed: React.FC<{ ledgerUrl: string; token: string }> = ({ ledgerUrl, token }) => {
  const { events, connected } = useEventStream({ ledgerUrl, token });

  return (
    <div style={{ fontFamily: 'Inter, sans-serif', padding: 16 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <span style={{
          width: 8, height: 8, borderRadius: '50%',
          background: connected ? '#22c55e' : '#ef4444',
          display: 'inline-block',
        }} />
        <span style={{ fontSize: 13, color: '#6b7280' }}>
          {connected ? 'Live' : 'Disconnected'}
        </span>
      </div>
      <div style={{ maxHeight: 400, overflowY: 'auto' }}>
        {events.map((e, i) => (
          <div key={i} style={{
            padding: '8px 12px', borderBottom: '1px solid #f1f5f9',
            display: 'flex', gap: 12, alignItems: 'flex-start',
          }}>
            <span style={{
              padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600,
              background: e.type === 'Created' ? '#dcfce7' : '#fee2e2',
              color: e.type === 'Created' ? '#15803d' : '#b91c1c',
            }}>{e.type}</span>
            <code style={{ fontSize: 11, color: '#374151', wordBreak: 'break-all' }}>
              {e.contractId.slice(0, 30)}...
            </code>
            <span style={{ fontSize: 11, color: '#9ca3af', marginLeft: 'auto' }}>
              {e.templateId.split(':').pop()}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};
