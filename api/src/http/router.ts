import { Router } from 'express';
import type { SseBroker } from '../sse/broker.ts';

export function createRouter(broker: SseBroker): Router {
  const router = Router();

  router.get('/events', (req, res) => {
    broker.addClient(res);
  });

  return router;
}
