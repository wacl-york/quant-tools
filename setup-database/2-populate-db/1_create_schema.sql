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

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_edit;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA waclquant TO waclquant_read;

-- LCS view contains the most recent availble calibration for each timepoint
-- and only returns 1 sensor per measurand per instrument 
-- i.e. PurpleAir will only return one PM2.5 sensor, despite 2 being on board
-- It also adds the location of each measurement
CREATE MATERIALIZED VIEW lcs AS
SELECT t2.time, dep.location, t2.instrument, t2.measurand, t2.measurement
FROM (
    SELECT ROW_NUMBER() OVER(PARTITION BY lcs.instrument, lcs.measurand, lcs.sensornumber, lcs.time ORDER BY cal.dateapplied DESC) as rownum, 
        lcs.instrument, lcs.measurand, lcs.sensornumber, lcs.time, lcs.measurement
    FROM 
        (
            SELECT mes.time, mes.instrument, mes.calibrationname, mes.measurand, mes.measurement, mes.sensornumber
            FROM lcsinstrument ins
            INNER JOIN measurement mes USING(instrument)
            WHERE mes.sensornumber = 1
        ) lcs 
    INNER JOIN sensorcalibration cal 
    USING(instrument, measurand, sensornumber, calibrationname)
    ) t2
INNER JOIN deployment dep 
    ON t2.instrument = dep.instrument 
        AND t2.time BETWEEN dep.start AND dep.finish
WHERE t2.rownum = 1;

-- Reference view adds the location of each measurement and doesn't
-- return the LGR CO measurements, preferring the Thermo instead
CREATE MATERIALIZED VIEW ref AS
SELECT mes.time, dep.location, mes.instrument, mes.measurand, mes.measurement
FROM referenceinstrument ins
INNER JOIN measurement mes USING(instrument)
INNER JOIN deployment dep 
    ON ins.instrument = dep.instrument AND
    mes.time BETWEEN dep.start AND dep.finish
WHERE mes.sensornumber = 1
      AND NOT (mes.instrument = 'LGR_Manchester' AND mes.measurand = 'CO');
