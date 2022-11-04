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

CREATE INDEX idx_lcslatest_measurand_instrument_location_time ON lcs_latest(measurand, instrument, time, location);
CREATE INDEX idx_lcslatest_instrument ON lcs_latest(instrument);
CREATE INDEX idx_lcslatest_measurand ON lcs_latest(measurand);
CREATE INDEX idx_lcslatest_time ON lcs_latest(time);

CREATE INDEX idx_ref_measurand_location_time ON ref(measurand, time, location);
CREATE INDEX idx_ref_time ON ref(time);
