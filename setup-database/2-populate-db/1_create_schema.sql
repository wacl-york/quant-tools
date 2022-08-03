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

DROP TABLE IF EXISTS Instrument CASCADE;
DROP TABLE IF EXISTS InstrumentType CASCADE;
DROP TABLE IF EXISTS ReferenceInstrument CASCADE;
DROP TABLE IF EXISTS LCSInstrument CASCADE;
DROP TABLE IF EXISTS LCSCompany CASCADE;
DROP TABLE IF EXISTS Deployment CASCADE;
DROP TABLE IF EXISTS Sensor CASCADE;
DROP TABLE IF EXISTS Measurand CASCADE;
DROP TABLE IF EXISTS SensorCalibration CASCADE;
DROP TABLE IF EXISTS Measurement CASCADE;
DROP TABLE IF EXISTS Flag CASCADE;
DROP VIEW IF EXISTS lcs;
DROP VIEW IF EXISTS ref;

CREATE TABLE InstrumentType (
    InstrumentTypeID INTEGER PRIMARY KEY,
    Name TEXT
);

INSERT INTO InstrumentType(InstrumentTypeID, Name) VALUES (1, 'LCS');
INSERT INTO InstrumentType(InstrumentTypeID, Name) VALUES (2, 'Reference');

CREATE TABLE Instrument (
    Instrument TEXT PRIMARY KEY,
    InstrumentTypeID INTEGER,
    FOREIGN KEY(InstrumentTypeID) REFERENCES InstrumentType(InstrumentTypeID),
    CONSTRAINT instruments_instrumentid_instrumenttypeid_unique UNIQUE(Instrument, InstrumentTypeID)
);

CREATE TABLE LCSCompany(
    COMPANY TEXT PRIMARY KEY
);

CREATE TABLE LCSInstrument(
    Instrument TEXT PRIMARY KEY,
    InstrumentTypeID INTEGER DEFAULT 1,
    InternalID TEXT,
    Study TEXT,
    Company TEXT,
    FOREIGN KEY(Instrument, InstrumentTypeID) REFERENCES Instrument(Instrument, InstrumentTypeID),
    FOREIGN KEY(Company) REFERENCES LCSCompany(Company)
);

CREATE TABLE ReferenceInstrument (
    Instrument TEXT PRIMARY KEY,
    InstrumentTypeID INTEGER DEFAULT 2,
    FOREIGN KEY(Instrument, InstrumentTypeID) REFERENCES Instrument(Instrument, InstrumentTypeID)
);

CREATE TABLE Deployment(
    Instrument TEXT,
    Location TEXT,
    Start TIMESTAMP,
    Finish TIMESTAMP,
    PRIMARY KEY (Instrument, Location, Start),
    FOREIGN KEY(Instrument) REFERENCES Instrument(Instrument)
);

CREATE TABLE Measurand(
    Measurand TEXT PRIMARY KEY,
    Units TEXT
);

CREATE TABLE Sensor(
    Instrument TEXT,
    Measurand TEXT,
    SensorNumber INTEGER,
    PRIMARY KEY (Instrument, Measurand, SensorNumber),
    FOREIGN KEY(Instrument) REFERENCES Instrument(Instrument),
    FOREIGN KEY(Measurand) REFERENCES Measurand(Measurand)
);

CREATE TABLE SensorCalibration(
    Instrument TEXT,
    Measurand TEXT,
    SensorNumber INTEGER,
    CalibrationName TEXT,
    DateApplied TIMESTAMP,
    PRIMARY KEY (Instrument, Measurand, SensorNumber, CalibrationName),
    FOREIGN KEY(Instrument, Measurand, SensorNumber) REFERENCES Sensor(Instrument, Measurand, SensorNumber),
    FOREIGN KEY(Instrument) REFERENCES Instrument(Instrument),
    FOREIGN KEY(Measurand) REFERENCES Measurand(Measurand)
);

CREATE TABLE Measurement(
    Instrument TEXT,
    Measurand TEXT,
    SensorNumber INTEGER,
    CalibrationName TEXT,
    Time TIMESTAMP,
    Measurement REAL,
    PRIMARY KEY (Instrument, Measurand, SensorNumber, CalibrationName, Time),
    FOREIGN KEY(Instrument, Measurand, SensorNumber, CalibrationName) REFERENCES SensorCalibration(Instrument, Measurand, SensorNumber, CalibrationName),
    FOREIGN KEY(Instrument, Measurand, SensorNumber) REFERENCES Sensor(Instrument, Measurand, SensorNumber),
    FOREIGN KEY(Instrument) REFERENCES Instrument(Instrument),
    FOREIGN KEY(Measurand) REFERENCES Measurand(Measurand)
);

CREATE TABLE Flag(
    Instrument TEXT,
    Measurand TEXT,
    SensorNumber INTEGER,
    CalibrationName TEXT,
    Time TIMESTAMP,
    PRIMARY KEY (Instrument, Measurand, SensorNumber, CalibrationName, Time),
    FOREIGN KEY(Instrument, Measurand, SensorNumber, CalibrationName, Time) REFERENCES Measurement(Instrument, Measurand, SensorNumber, CalibrationName, Time),
    FOREIGN KEY(Instrument, Measurand, SensorNumber, CalibrationName) REFERENCES SensorCalibration(Instrument, Measurand, SensorNumber, CalibrationName),
    FOREIGN KEY(Instrument, Measurand, SensorNumber) REFERENCES Sensor(Instrument, Measurand, SensorNumber),
    FOREIGN KEY(Instrument) REFERENCES Instrument(Instrument),
    FOREIGN KEY(Measurand) REFERENCES Measurand(Measurand)
);

--ALTER TABLE LCSDevices
--ADD CONSTRAINT fk_lcsdevices_lcsmanufacturers
--    FOREIGN KEY(manufacturer_id) 
--    REFERENCES LCSManufacturers(manufacturer_id)
--    ON DELETE CASCADE;


-- User friendly views
--CREATE VIEW lcs AS
--    SELECT time, location_name AS location, manufacturer_name AS manufacturer, device_name AS device, version_name AS version, measurand_name AS species, measurement AS value 
--    FROM lcsmeasurements
--    INNER JOIN lcsdevices USING(device_id)
--    INNER JOIN lcsmanufacturers using (manufacturer_id)
--    INNER JOIN lcsmeasurementversions USING (version_id)
--    INNER JOIN measurands USING (measurand_id)
--    INNER JOIN lcsdeployments ON (lcsmeasurements.device_id = lcsdeployments.device_id AND lcsmeasurements.time >= lcsdeployments.start_time AND lcsmeasurements.time <= lcsdeployments.end_time)
--    INNER JOIN locations USING (location_id)
--;
--
--
--CREATE VIEW ref AS
--    SELECT time, location_name AS location, measurand_name AS species, measurement AS value FROM referencemeasurements
--    INNER JOIN locations USING (location_id)
--    INNER JOIN measurands USING (measurand_id)
--;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_edit;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_read;
