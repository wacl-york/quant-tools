import sqlite3
import pandas as pd

##### Database connection
# You open a connection in a very similar way to reading a CSV file
conn = sqlite3.connect("<PATH>/<TO>/quant.db")

##### Accessing LCS data
# The LCS data is accessed through the 'lcs' table
# "SELECT * FROM lcs" means "get all columns from the lcs table"
# "LIMIT 100" means "return the top 100"

# NB: For your queries you'll want to remove the LIMIT 100 part, it is only here
# in the examples to make them quicker
df = pd.read_sql("SELECT * FROM lcs LIMIT 100", conn)
print(df)

# We can subset the data to fields we're interested in by specifying
# them in the SELECT statement
# So the query below only returns NO2 and O3

# NB: It is always better to only request the columns you want as it will be 
# quicker and use less memory!
df_2 = pd.read_sql("SELECT timestamp, device, version, NO2, O3 FROM lcs LIMIT 100", conn)
print(df_2)

# You can filter the rows by time, device, and the dataset version by using the
# WHERE clause
# The following query obtains PM2.5 from PA3, PA3_b, PA4 between March and April
# 2020
# See this webpage for further details on WHERE: https://www.w3schools.com/SQl/sql_where.asp
df_3 = pd.read_sql("SELECT timestamp, device, PM25 FROM lcs WHERE device IN ('PA3', 'PA3_b') AND timestamp BETWEEN '2020-03-01' AND '2020-04-30' LIMIT 100", conn)
print(df_3)

# There are 3 possible values for the version column: 'out-of-box', 'cal1',
# 'cal2'.
# This query finds all O3 and NO2 from AQY874 in March 2020 and orders the results
# firstly in time, and then secondly by version.
# This allows us to see that we have both out-of-box and cal1 values for this
# species in this time period
df_4 = pd.read_sql("SELECT timestamp, device, version, O3, NO2 FROM lcs WHERE device = 'AQY874' AND timestamp BETWEEN '2020-03-01' AND '2020-03-02' ORDER BY timestamp, version ASC LIMIT 100", conn)
print(df_4)

##### Accessing reference data
# The reference data is organised similarly in a table called 'ref'.
# Each row corresponds to a given timestamp and location
# The possible locations are: 'Manchester', 'Birmingham', 'York', and 'London'
df_ref = pd.read_sql("SELECT * FROM ref LIMIT 100", conn)
print(df_ref)

##### Time averaging
# Currently the database just returns 1 min data, so if you need to resample the
# data you can use the usual pandas method.
# Firstly note that you'll need to explicitly set the timestamp as a datetime type, as it is a string by default
df_4['timestamp'] = pd.to_datetime(df_4['timestamp'])
# The groupby allows resampling across multiple devices and dataset versions if present
df_5mins = df_4.set_index("timestamp").groupby(["device", "version"]).resample("5 min", level=0).mean().reset_index()    
print(df_5mins)

###### Meta-data
# There are a number of tables storing meta-data

### deployments
# The deployments table keeps a record of where the devices have been deployed
pd.read_sql("SELECT * FROM deployments", conn)

### devices
# devices contains a list of all devices in the study
pd.read_sql("SELECT * FROM devices", conn)

### devices_versions
# devices_versions has a row for each device and dataset version
pd.read_sql("SELECT * FROM devices_versions", conn)
# I.e. we can see that we only have out-of-box for PurpleAir
pd.read_sql("SELECT * FROM devices_versions WHERE device LIKE 'PA%'", conn)

### devices_sensors
# devices_sensors details which sensors each device has, with a 1 indicating
# it has that sensor, and a 0 saying it does not.
pd.read_sql("SELECT * FROM devices_sensors", conn)

### devices_sensors_versions
# devices_sensors_versoins details which sensors are in which dataset versions
pd.read_sql("SELECT * FROM devices_versions_sensors", conn)
# So for Zephyr we can see that we only have NO in cal1
pd.read_sql("SELECT * FROM devices_versions_sensors WHERE device LIKE 'Zep%'", conn)
