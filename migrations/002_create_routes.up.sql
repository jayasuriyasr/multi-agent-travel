CREATE TABLE routes (
    route_id TEXT PRIMARY KEY,
    name     TEXT NOT NULL,
    mode     TEXT NOT NULL  -- e.g. 'bus', 'rail', 'metro', 'ferry'
);
