-- 3_creates_indexes.sql
-- ~~~~~~~~~~~~~~~~~~~~~
--
-- This script creates the indexes for the lcs and ref views, since 
-- these are materialized views and thus do not use the indexes from their
-- constituent tables.

CREATE INDEX idx_lcs_measurand_instrument_location_time ON lcs(measurand, instrument, version, time, location);
CREATE INDEX idx_lcs_instrument ON lcs(instrument);
CREATE INDEX idx_lcs_version ON lcs(version);
CREATE INDEX idx_lcs_measurand ON lcs(measurand);
CREATE INDEX idx_lcs_time ON lcs(time);

CREATE INDEX idx_lcs_hourly_measurand_instrument_location_time ON lcs_hourly(measurand, instrument, version, time, location);
CREATE INDEX idx_lcs_hourly_instrument ON lcs_hourly(instrument);
CREATE INDEX idx_lcs_hourly_version ON lcs_hourly(version);
CREATE INDEX idx_lcs_hourly_measurand ON lcs_hourly(measurand);
CREATE INDEX idx_lcs_hourly_time ON lcs_hourly(time);

CREATE INDEX idx_ref_measurand_location_time ON ref(measurand, time, location);
CREATE INDEX idx_ref_time ON ref(time);

CREATE INDEX idx_ref_hourly_measurand_location_time ON ref_hourly(measurand, time, location);
CREATE INDEX idx_ref_hourly_time ON ref_hourly(time);
