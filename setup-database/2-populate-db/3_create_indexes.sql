-- 3_creates_indexes.sql
-- ~~~~~~~~~~~~~~~~~~~~~

-- Creates indexes on the 2 measurement tables (lcs_raw and ref_raw) to speed up queries

--CREATE INDEX timestamp_device_version_location_idx ON lcs_raw(timestamp, device, version, location);
--CREATE INDEX device_timestamp_version_location_idx ON lcs_raw(device, timestamp, version, location);
--CREATE INDEX version_device_timestamp_idx ON lcs_raw(version, device, timestamp);
--CREATE INDEX location_idx ON lcs_raw(location);
--
--CREATE INDEX timestamp_device_location_idx ON lcs_latest_raw(timestamp, device, location);
--CREATE INDEX device_timestamp_location_idx ON lcs_latest_raw(device, timestamp, location);
--CREATE INDEX location_latest_idx ON lcs_latest_raw(location);
--
--CREATE INDEX timestamp_device_idx ON lcs_wider_participation_raw(timestamp, device);
--CREATE INDEX device_idx ON lcs_wider_participation_raw(device);
--
--CREATE INDEX timestamp_location_idx ON ref_raw(timestamp, location);
--CREATE INDEX location_ref_idx ON ref_raw(location);

-- TODO Get the most recent calibration name for each measurement
CREATE VIEW lcs AS
SELECT mes.time, dep.location, mes.instrument, mes.calibrationname, mes.measurand, mes.measurement
FROM lcsinstrument ins
INNER JOIN measurement mes USING(instrument)
INNER JOIN deployment dep 
    ON ins.instrument = dep.instrument AND
    mes.time BETWEEN dep.start AND dep.finish
WHERE mes.sensornumber = 1;

-- TODO Get the most recent calibration name for each measurement
CREATE VIEW ref AS
SELECT mes.time, dep.location, mes.calibrationname, mes.measurand, mes.measurement
FROM referenceinstrument ins
INNER JOIN measurement mes USING(instrument)
INNER JOIN deployment dep 
    ON ins.instrument = dep.instrument AND
    mes.time BETWEEN dep.start AND dep.finish
WHERE mes.sensornumber = 1;
