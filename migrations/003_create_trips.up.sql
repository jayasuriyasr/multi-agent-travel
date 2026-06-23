CREATE TABLE trips (
    trip_id        TEXT NOT NULL,
    date           DATE NOT NULL,
    route_id       TEXT REFERENCES routes(route_id),
    departure_unix BIGINT NOT NULL,
    PRIMARY KEY (trip_id, date)
);
