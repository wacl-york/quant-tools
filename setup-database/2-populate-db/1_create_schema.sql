-- 1_create_schema.sql
-- ~~~~~~~~~~~~~~~~~~~
--
-- Creates the database relations

DROP TABLE IF EXISTS lcs_raw;
DROP TABLE IF EXISTS ref_raw;
DROP TABLE IF EXISTS LCSDeployments;
DROP VIEW IF EXISTS lcs;
DROP VIEW IF EXISTS ref;

CREATE TABLE lcs_raw(
    timestamp INTEGER,
    device TEXT,
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
    AirPressure REAL,
    PRIMARY KEY (timestamp, device, version)
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
    AirPressure REAL,
    PRIMARY KEY (timestamp, location)
);


CREATE TABLE LCSDeployments(
    device TEXT,
    location TEXT,
    start INTEGER,
    end INTEGER,
    PRIMARY KEY (device, location, start)
);

CREATE VIEW LCS
AS SELECT 
    datetime(timestamp, 'unixepoch') as timestamp,
    device,
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
    RelHumidity,
    AirPressure
FROM lcs_raw;

CREATE VIEW Ref
AS SELECT 
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
    RelHumidity,
    AirPressure
FROM ref_raw;
