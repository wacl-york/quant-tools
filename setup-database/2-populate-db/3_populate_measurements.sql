-- 3_populate_measurements.sql
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Inserts the LCS and Reference measurements into the DB.
-- Must be run from a client connected to the DB through a user that
-- has write access.

\copy LCSMeasurements FROM 'Data/Aeroqual_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/AQMesh_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/PurpleAir_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/QuantAQ_OOB_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/QuantAQ_Cal1_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/QuantAQ_Cal2_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy LCSMeasurements FROM 'Data/Zephyr_to_insert.csv' DELIMITER ',' CSV HEADER;
\copy ReferenceMeasurements FROM 'Data/Reference_to_insert.csv' DELIMITER ',' CSV HEADER;
