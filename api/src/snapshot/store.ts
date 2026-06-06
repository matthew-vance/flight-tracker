import type { AircraftState } from '../types.ts';

const TTL_MS = 60_000;

export type SnapshotStore = {
  upsert: (state: AircraftState) => void;
  evict: () => string[];
  getAll: () => AircraftState[];
};

export function createSnapshotStore(): SnapshotStore {
  const map = new Map<string, AircraftState>();

  return {
    upsert(state) {
      map.set(state.icao, state);
    },
    evict() {
      const threshold = Date.now() - TTL_MS;
      const evicted: string[] = [];
      for (const [icao, state] of map) {
        if (state.lastSeen < threshold) {
          map.delete(icao);
          evicted.push(icao);
        }
      }
      return evicted;
    },
    getAll() {
      return [...map.values()];
    },
  };
}
