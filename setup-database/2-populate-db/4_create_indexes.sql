CREATE INDEX time_idx ON LCSMeasurements(time);
CREATE INDEX versionid_idx ON LCSMeasurements(version_id);
CREATE INDEX measurandid_deviceid_time_versionid_idx ON LCSMeasurements(measurand_id, device_id, time, version_id);
CREATE INDEX deviceid_measurandid_time_versionid_idx ON LCSMeasurements(device_id, measurand_id, time, version_id);
CREATE INDEX deployment_idx ON LCSDeployments(device_id, start_time, end_time, location_id);
CREATE INDEX measurandid_locationid_time_referencedeviceid ON ReferenceMeasurements(measurand_id, location_id, time, reference_device_id);
CREATE INDEX measurandid_time_locationid_referencedeviceid ON ReferenceMeasurements(measurand_id, time, location_id, reference_device_id);
