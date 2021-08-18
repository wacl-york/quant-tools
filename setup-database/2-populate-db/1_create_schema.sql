-- 1_create_schema.sql
-- ~~~~~~~~~~~~~~~~~~~
--
-- Creates the database relations

DROP TABLE IF EXISTS lcs_raw;
DROP TABLE IF EXISTS lcs_latest_raw;
DROP TABLE IF EXISTS ref_raw;
DROP TABLE IF EXISTS deployments_raw;
DROP TABLE IF EXISTS devices_sensors_versions;
DROP VIEW IF EXISTS lcs;
DROP VIEW IF EXISTS lcs_latest;
DROP VIEW IF EXISTS ref;
DROP VIEW IF EXISTS deployments;
DROP VIEW IF EXISTS devices;
DROP VIEW IF EXISTS devices_versions;
DROP VIEW IF EXISTS devices_sensors;

CREATE TABLE lcs_raw(
    timestamp INTEGER,
    device TEXT,
    location TEXT,
    version TEXT,
    O3 REAL,
    NO2 REAL,
    NO REAL,
    CO REAL,
    CO2 REAL,
    PM1 REAL,
    PM25 REAL,
    PM10 REAL,
    Temperature REAL,
    RelHumidity REAL,
    PRIMARY KEY (timestamp, device, version)
);

CREATE TABLE lcs_latest_raw(
    timestamp INTEGER,
    device TEXT,
    location TEXT,
    O3 REAL,
    NO2 REAL,
    NO REAL,
    CO REAL,
    CO2 REAL,
    PM1 REAL,
    PM25 REAL,
    PM10 REAL,
    Temperature REAL,
    RelHumidity REAL,
    PRIMARY KEY (timestamp, device)
);

CREATE TABLE ref_raw(
    timestamp INTEGER,
    location TEXT,
    O3 REAL,
    NO2 REAL,
    NO REAL,
    CO REAL,
    CO2 REAL,
    PM1 REAL,
    PM25 REAL,
    PM10 REAL,
    Temperature REAL,
    RelHumidity REAL,
    PRIMARY KEY (timestamp, location)
);


CREATE TABLE deployments_raw(
    device TEXT,
    location TEXT,
    start INTEGER,
    end INTEGER,
    PRIMARY KEY (device, location, start)
);

CREATE TABLE devices_versions_sensors(
    device TEXT,
    version TEXT,
    O3 INTEGER,
    NO2 INTEGER,
    NO INTEGER,
    CO INTEGER,
    CO2 INTEGER,
    PM1 INTEGER,
    PM25 INTEGER,
    PM10 INTEGER,
    Temperature INTEGER,
    RelHumidity INTEGER,
    PRIMARY KEY (device, version)
);

CREATE VIEW lcs
AS
SELECT
    datetime(timestamp, 'unixepoch') as timestamp,
    device,
    location,
    version,
    O3,
    NO2,
    NO,
    CO,
    CO2,
    PM1,
    PM25,
    PM10,
    Temperature,
    RelHumidity
FROM lcs_raw;

CREATE VIEW lcs_latest
AS
SELECT
    datetime(timestamp, 'unixepoch') as timestamp,
    device,
    location,
    O3,
    NO2,
    NO,
    CO,
    CO2,
    PM1,
    PM25,
    PM10,
    Temperature,
    RelHumidity
FROM lcs_latest_raw;

CREATE VIEW ref
AS
SELECT
    datetime(timestamp, 'unixepoch') as timestamp,
    location,
    O3,
    NO2,
    NO,
    CO,
    CO2,
    PM1,
    PM25,
    PM10,
    Temperature,
    RelHumidity
FROM ref_raw;

CREATE VIEW deployments
AS
SELECT
    device,
    location,
    datetime(start, 'unixepoch') as start,
    datetime(end, 'unixepoch') as end
FROM deployments_raw
;

CREATE VIEW devices_versions
AS
SELECT DISTINCT device, version
FROM lcs_raw
;

CREATE VIEW devices
AS
SELECT DISTINCT device
FROM lcs_raw
;

CREATE VIEW devices_sensors
AS
SELECT device,
       CASE WHEN SUM(O3) > 0 THEN 1 ELSE 0 END AS O3,
       CASE WHEN SUM(NO2) > 0 THEN 1 ELSE 0 END AS NO2,
       CASE WHEN SUM(NO) > 0 THEN 1 ELSE 0 END AS NO,
       CASE WHEN SUM(CO) > 0 THEN 1 ELSE 0 END AS CO,
       CASE WHEN SUM(CO2) > 0 THEN 1 ELSE 0 END AS CO2,
       CASE WHEN SUM(PM1) > 0 THEN 1 ELSE 0 END AS PM1,
       CASE WHEN SUM(PM25) > 0 THEN 1 ELSE 0 END AS PM25,
       CASE WHEN SUM(PM10) > 0 THEN 1 ELSE 0 END AS PM10,
       CASE WHEN SUM(Temperature) > 0 THEN 1 ELSE 0 END AS Temperature,
       CASE WHEN SUM(RelHumidity) > 0 THEN 1 ELSE 0 END AS RelHumidity
FROM devices_versions_sensors
GROUP BY device
;
