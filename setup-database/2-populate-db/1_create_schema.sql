-- 1_create_schema.sql
-- ~~~~~~~~~~~~~~~~~~~
--
-- Creates the database infrastructure including
-- tables, primary and foreign keys and manages access
-- This is all done in the default schema 'waclquant'

-- NB: The `tablefunc` module should be installed on
-- the database prior to running this script.
-- Its installation isn't included in this script
-- as we do not have SU access to ITS hosted DBs

DROP TABLE IF EXISTS LCSDevices CASCADE;
DROP TABLE IF EXISTS LCSManufacturers CASCADE;
DROP TABLE IF EXISTS Locations CASCADE;
DROP TABLE IF EXISTS Measurands CASCADE;
DROP TABLE IF EXISTS LCSDeployments CASCADE;
DROP TABLE IF EXISTS LCSMeasurements CASCADE;
DROP TABLE IF EXISTS ReferenceDevices CASCADE;
DROP TABLE IF EXISTS ReferenceMeasurements CASCADE;
DROP TABLE IF EXISTS LCSMeasurementVersions CASCADE;

CREATE TABLE LCSDevices(
    device_id SERIAL PRIMARY KEY,
    device_name TEXT,
    manufacturer_id INTEGER
);

CREATE TABLE LCSManufacturers(
    manufacturer_id SERIAL PRIMARY KEY,
    manufacturer_name TEXT
);

CREATE TABLE Locations(
    location_id SERIAL PRIMARY KEY,
    location_name TEXT
);

CREATE TABLE Measurands(
    measurand_id SERIAL PRIMARY KEY,
    measurand_name TEXT
);

CREATE TABLE LCSDeployments(
    device_id INTEGER,  
    location_id INTEGER, 
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    PRIMARY KEY (device_id, location_id, start_time)
);

CREATE TABLE LCSMeasurements(
    time TIMESTAMP,
    device_id INTEGER,
    version_id INTEGER,
    measurand_id INTEGER,
    measurement FLOAT,
    PRIMARY KEY (time, device_id, measurand_id, version_id)
);

CREATE TABLE ReferenceDevices(
    reference_device_id SERIAL PRIMARY KEY,
    name TEXT
);

CREATE TABLE ReferenceMeasurements(
    time TIMESTAMP,
    location_id INTEGER,
    reference_device_id INTEGER,
    measurand_id INTEGER,
    measurement FLOAT,
    PRIMARY KEY (time, reference_device_id, location_id, measurand_id)
);

CREATE TABLE LCSMeasurementVersions(
    version_id SERIAL PRIMARY KEY,
    version_name TEXT
);

ALTER TABLE LCSDevices
ADD CONSTRAINT fk_lcsdevices_lcsmanufacturers
FOREIGN KEY(manufacturer_id) 
REFERENCES LCSManufacturers(manufacturer_id)
ON DELETE CASCADE;

ALTER TABLE LCSDeployments
ADD CONSTRAINT fk_lcsdeployments_lcsdevices
FOREIGN KEY(device_id) 
REFERENCES LCSDevices(device_id)
ON DELETE CASCADE;

ALTER TABLE LCSDeployments
ADD CONSTRAINT fk_lcsdeployments_locations
FOREIGN KEY(location_id) 
REFERENCES Locations(location_id)
ON DELETE CASCADE;

ALTER TABLE LCSMeasurements
ADD CONSTRAINT fk_lcsmeasurements_lcsdevices
FOREIGN KEY(device_id) 
REFERENCES LCSDevices(device_id)
ON DELETE CASCADE;

ALTER TABLE LCSMeasurements 
ADD CONSTRAINT fk_lcsmeasurements_lcsmeasurementversions
FOREIGN KEY(version_id) 
REFERENCES LCSMeasurementVersions(version_id)
ON DELETE CASCADE;

ALTER TABLE LCSMeasurements 
ADD CONSTRAINT fk_lcsmeasurements_measurands
FOREIGN KEY(measurand_id) 
REFERENCES Measurands(measurand_id)
ON DELETE CASCADE;

ALTER TABLE ReferenceMeasurements
ADD CONSTRAINT fk_referencemeasurements_referencedevices
FOREIGN KEY(reference_device_id) 
REFERENCES ReferenceDevices(reference_device_id)
ON DELETE CASCADE;

ALTER TABLE ReferenceMeasurements
ADD CONSTRAINT fk_referencemeasurements_locations
FOREIGN KEY(location_id) 
REFERENCES Locations(location_id)
ON DELETE CASCADE;

ALTER TABLE ReferenceMeasurements 
ADD CONSTRAINT fk_referencemeasurements_measurands
FOREIGN KEY(measurand_id) 
REFERENCES Measurands(measurand_id)
ON DELETE CASCADE;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_edit;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_read;

-- User friendly views
CREATE VIEW lcs AS
    SELECT time, location_name AS location, manufacturer_name AS manufacturer, device_name AS device, version_name AS version, measurand_name AS species, measurement AS value 
    FROM lcsmeasurements
    INNER JOIN lcsdevices USING(device_id)
    INNER JOIN lcsmanufacturers using (manufacturer_id)
    INNER JOIN lcsmeasurementversions USING (version_id)
    INNER JOIN measurands USING (measurand_id)
    INNER JOIN lcsdeployments ON (lcsmeasurements.device_id = lcsdeployments.device_id AND lcsmeasurements.time >= lcsdeployments.start_time AND lcsmeasurements.time <= lcsdeployments.end_time)
    INNER JOIN locations USING (location_id)
;

CREATE VIEW ref AS
    SELECT time, location_name AS location, measurand_name AS species, measurement AS value FROM referencemeasurements
    INNER JOIN locations USING (location_id)
    INNER JOIN measurands USING (measurand_id)
;
