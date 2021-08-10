-- 3_creates_indexes.sql
-- ~~~~~~~~~~~~~~~~~~~~~

-- Creates indexes on the 2 measurement tables (lcs_raw and ref_raw) to speed up queries

CREATE INDEX timestamp_device_version_idx ON lcs_raw(timestamp, device, version);
CREATE INDEX device_timestamp_version_idx ON lcs_raw(device, timestamp, version);
CREATE INDEX version_device_timestamp_idx ON lcs_raw(version, device, timestamp);

CREATE INDEX timestamp_location_idx ON ref_raw(timestamp, location);
CREATE INDEX location_idx ON ref_raw(location);
