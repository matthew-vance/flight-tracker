export type AircraftState = {
  icao: string;
  flight: string | null;
  altitude: string | null;
  speed: string | null;
  heading: string | null;
  latitude: string | null;
  longitude: string | null;
  squawk: string | null;
  lastSeen: number;
};
