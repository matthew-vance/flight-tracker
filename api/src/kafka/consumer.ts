import { KafkaJS } from '@confluentinc/kafka-javascript';
import type { AircraftState } from '../types.ts';

type MessageHandler = (state: AircraftState) => void;

export async function startConsumer(
  brokers: string,
  onMessage: MessageHandler,
): Promise<void> {
  const { Kafka } = KafkaJS;
  const kafka = new Kafka({
    kafkaJS: { brokers: brokers.split(',') },
  });

  const consumer = kafka.consumer({
    kafkaJS: {
      groupId: `api-service-${crypto.randomUUID()}`,
      fromBeginning: true,
    },
  });

  await consumer.connect();
  await consumer.subscribe({ topic: 'adsb.state' });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const raw = parseJsonSr(message.value) as Record<string, string | null>;
      const icao = raw['ICAO'];
      if (!icao) return;

      const state: AircraftState = {
        icao,
        flight: raw['FLIGHT'] ?? null,
        altitude: raw['ALTITUDE'] ?? null,
        speed: raw['SPEED'] ?? null,
        heading: raw['HEADING'] ?? null,
        latitude: raw['LATITUDE'] ?? null,
        longitude: raw['LONGITUDE'] ?? null,
        squawk: raw['SQUAWK'] ?? null,
        lastSeen: Number(message.timestamp),
      };

      onMessage(state);
    },
  });
}

function parseJsonSr(buffer: Buffer): unknown {
  // Strip 5-byte Confluent wire format header (magic byte 0x00 + 4-byte schema ID)
  const payload = buffer.length > 5 && buffer[0] === 0x00 ? buffer.subarray(5) : buffer;
  return JSON.parse(payload.toString('utf8'));
}
