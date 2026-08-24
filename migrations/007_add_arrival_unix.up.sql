-- L2 fix: add arrival_unix to stop_times so the engine can distinguish
-- the time a train ARRIVES at a stop from the time it DEPARTS.
-- Previously, departure_unix was (incorrectly) used as arrival time.
ALTER TABLE stop_times ADD COLUMN arrival_unix BIGINT;

-- Back-fill existing rows: assume arrival == departure for legacy data.
-- Real data should provide accurate values via the ingestion API.
UPDATE stop_times SET arrival_unix = departure_unix WHERE arrival_unix IS NULL;

-- Enforce NOT NULL after back-fill
ALTER TABLE stop_times ALTER COLUMN arrival_unix SET NOT NULL;
