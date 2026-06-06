import express from 'express';
import { createSnapshotStore } from './snapshot/store.ts';
import { createSseBroker } from './sse/broker.ts';
import { createRouter } from './http/router.ts';
import { startConsumer } from './kafka/consumer.ts';

const PORT = Number(process.env.PORT ?? 3000);
const KAFKA_BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:19092';
const EVICTION_INTERVAL_MS = 5_000;

const store = createSnapshotStore();
const broker = createSseBroker(store);

const app = express();
app.use(createRouter(broker));
app.listen(PORT, () => {
  console.log(`Listening on :${PORT}`);
});

setInterval(() => {
  const evicted = store.evict();
  for (const icao of evicted) {
    broker.broadcast('aircraft.removed', { icao });
  }
}, EVICTION_INTERVAL_MS);

await startConsumer(KAFKA_BROKERS, (state) => {
  store.upsert(state);
  broker.broadcast('aircraft.updated', state);
});
