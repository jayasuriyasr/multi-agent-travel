CREATE TABLE trip_poll_schedule (
    trip_id           TEXT NOT NULL,
    date              DATE NOT NULL,
    zone              TEXT NOT NULL,
    poll_interval_sec INTEGER NOT NULL,
    PRIMARY KEY (trip_id, date)
);
