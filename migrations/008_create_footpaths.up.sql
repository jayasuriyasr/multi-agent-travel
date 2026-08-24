-- L6 fix: create footpaths table so walk-transfer data can be loaded
-- into the RouteBuffer's Footpaths map during ReloadRouteArrays.
-- Each row represents a ONE-WAY walkable transfer from station_id to neighbour_id.
-- Insert BOTH directions explicitly when a walk is bidirectional.
CREATE TABLE footpaths (
    station_id    TEXT NOT NULL REFERENCES stations(id),
    neighbour_id  TEXT NOT NULL REFERENCES stations(id),
    walk_seconds  INTEGER NOT NULL CHECK (walk_seconds > 0),
    PRIMARY KEY (station_id, neighbour_id)
);

CREATE INDEX idx_footpaths_station ON footpaths(station_id);
