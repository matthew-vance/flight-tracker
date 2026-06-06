import type { Response } from 'express';
import type { AircraftState } from '../types.ts';
import type { SnapshotStore } from '../snapshot/store.ts';

export type SseBroker = {
  addClient: (res: Response) => void;
  broadcast: (event: string, data: unknown) => void;
};

export function createSseBroker(store: SnapshotStore): SseBroker {
  const clients = new Set<Response>();

  function writeEvent(res: Response, event: string, data: unknown): void {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }

  return {
    addClient(res) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.flushHeaders();

      for (const state of store.getAll()) {
        writeEvent(res, 'aircraft.updated', state);
      }

      clients.add(res);
      res.on('close', () => clients.delete(res));
    },
    broadcast(event, data) {
      for (const res of clients) {
        writeEvent(res, event, data);
      }
    },
  };
}
