from sqlalchemy import create_engine
import pandas as pd

##### Authentication

# Fill in the database parameters to authenticate access
# NB: NEVER MAKE THESE CREDENTIALS PUBLICLY ACCESSIBLE!
engine = create_engine("postgresql://<user>:<password>@<host>:<port>/<db>")

##### Accessing LCS data

# The LCS data is accessed through the 'lcs' table
# "SELECT * FROM lcs" means "get all columns from the lcs table"
# This data is in long format with 1 row corresponding to 1 measurement from 1
# species per a specific device and calibration version
# "LIMIT 100" means "return the top 100"
df = pd.read_sql("SELECT * FROM lcs LIMIT 100", engine)
print(df)

# We can subset the data to fields we're interested in using the WHERE filter
# See this webpage for further details: https://www.w3schools.com/SQl/sql_where.asp
# The following query obtains PM2.5 from PA3, PA3_b, PA4 between March and April
# 2020
df_2 = pd.read_sql("SELECT * FROM lcs WHERE device IN ('PA3', 'PA3_b') AND species = 'PM2.5' AND time BETWEEN '2020-03-01' AND '2020-04-30' LIMIT 100", engine)
print(df_2)

# There are 3 possible values for the version column: 'out-of-box', 'cal1',
# 'cal2'.
# This query finds all O3 from AQY874 in March 2020 and orders the results
# firstly in time, and then secondly by version.
# This allows us to see that we have both out-of-box and cal1 values for this
# species in this time period
df_3 = pd.read_sql("SELECT * FROM lcs WHERE device = 'AQY874' AND species = 'O3' AND time BETWEEN '2020-03-01' AND '2020-03-02' ORDER BY time, version ASC LIMIT 100", engine)
print(df_3)

##### Accessing reference data

# The reference data is organised similarly with each row giving 1 measurement
# per species and location.
# The possible locations are: 'Manchester', 'Birmingham', 'York', and 'London'
df_ref = pd.read_sql("SELECT * FROM ref LIMIT 100", engine)
print(df_ref)

##### Wide data

# The data returned from the database is in long format, i.e. one row per
# species per device etc...
# If you prefer to work with the data in wide format, such as one column per
# species you can do so using the `pivot` function from pandas
df_4 = pd.read_sql("SELECT * FROM lcs WHERE device IN ('AQY874', 'AQY872') AND species IN ('O3', 'NO2', 'NO') AND time BETWEEN '2020-03-01' AND '2020-03-02' ORDER BY time, version ASC LIMIT 100", engine)
print(df_4)

df_4_wide = df_4.pivot(index=['time', 'location', 'manufacturer', 'device', 'version'],
                       columns='species',
                       values='value').reset_index()
print(df_4_wide)

# You can also pivot multiple columns, i.e. the code below gives one column per
# device and species
df_4_wide_2 = df_4.pivot(index=['time', 'location', 'manufacturer', 'version'],
                         columns=['device', 'species'],
                         values='value').reset_index()
print(df_4_wide_2)

##### Time averaging

# Currently the database just returns 1 min data, so if you need to resample the
# data you can use the usual pandas method
df_5mins = df_4.set_index("time").groupby(["location", "manufacturer", "device", "version", "species"]).resample("5 min", level=0).mean().reset_index()    
print(df_5mins)

# Hopefully running the averaging on the database server would be quicker than
# doing it in pandas
