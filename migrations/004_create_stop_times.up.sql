CREATE TABLE stop_times (
    trip_id        TEXT NOT NULL,
    date           DATE NOT NULL,
    stop_seq       INTEGER NOT NULL,
    station_id     TEXT REFERENCES stations(id),
    departure_unix BIGINT NOT NULL,
    PRIMARY KEY (trip_id, date, stop_seq),
    FOREIGN KEY (trip_id, date) REFERENCES trips(trip_id, date)
);
