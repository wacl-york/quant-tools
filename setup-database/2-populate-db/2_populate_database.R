# 2_populate_database.R
# ~~~~~~~~~~~~~~~~~~~~~

# (Nearly) populates the database with data from the first portion
# of the QUANT study of the initial 5 companies from 2019-12-10 to
# 2021-06-30.
# Requires an ODBC DSN setup that has write access.
#
# The LCS and Reference measurements themselves aren't actually
# inserted into the DB by this script, simply because the R ODBC wrapper
# is slow and memory inefficient.
# Instead, these values are saved to disk in exactly the format in which
# they can be inserted into the DB with the \copy psql command,
# which is run in script 2-populate-db/3_populate_measurements.sql

# This script instead then inserts all the other relations, such as the lookup
# tables containing the devices, locations, versions etc... metadata.
# This could easily have been achieved by a single SQL script, however, 
# I felt more comfortable with the data munging in R.
# If I'd known from the start that I wouldn't be able to upload the measurements
# using ODBC I probably would have just written a single SQL script that 
# creates the infrastructure and inserts the initial values from CSV.

library(DBI)
library(odbc)
library(RSQLite)
library(tidyverse)
library(data.table)
library(lubridate)

con <- dbConnect(odbc(), "QUANT")

# Load all saved data
lcs_fns <- paste0("Data/",
              c("Aeroqual.csv",
                "AQMesh.csv",
                "PurpleAir.csv",
                "QuantAQ_Cal1.csv",
                "QuantAQ_Cal2.csv",
                "QuantAQ_OOB.csv",
                "Zephyr.csv"))

############ LCSManufacturers
manufacturers_to_insert <- data.frame(
    manufacturer_name=c("AQMesh", "QuantAQ", "Aeroqual", "Zephyr", "PurpleAir")
)
dbAppendTable(con, "lcsmanufacturers", manufacturers_to_insert)

############ LCSDevices
devices <- bind_rows(lapply(lcs_fns, function(x) fread(x) %>% distinct(manufacturer, device))) %>%
            distinct(manufacturer, device)
# Get manufacturer_id to get names
devices_to_insert <- tbl(con, "lcsmanufacturers") %>%
    collect() %>%
    left_join(devices, by=c("manufacturer_name"="manufacturer")) %>%
    select(manufacturer_id, device_name=device)
dbAppendTable(con, "lcsdevices", devices_to_insert)


############ Measurands
lcs_measurands <- bind_rows(lapply(lcs_fns, function(x) fread(x) %>% distinct(measurand))) %>%
            distinct(measurand)
ref_measurands <- fread("Data/Reference.csv") %>% distinct(variable) %>% rename(measurand=variable)
measurands_to_insert <- rbind(lcs_measurands, ref_measurands) %>%
    distinct(measurand) %>%
    rename(measurand_name = measurand) %>%
    filter(!measurand_name %in% c("Voltage", "PM4"))

dbAppendTable(con, "measurands", measurands_to_insert)

############ Locations
locations_to_insert <- data.frame(location_name=c("London", "Manchester", "York", "Birmingham"))
dbAppendTable(con, "locations", locations_to_insert)

############ Versions
versions_to_insert <- data.frame(
   version_name=c("out-of-box", "cal1", "cal2")
)
dbAppendTable(con, "lcsmeasurementversions", versions_to_insert)

############ Device deployment history
# Deployment start and end datetimes are both inclusive, so 
# in absence of further information set start date as midnight of
# deployment day and end date as 1 second before midnight of final
# deployment day
yesterday <- as.character(today() - days(1))
deployments_to_insert <- bind_rows(
    list(
        data.frame(
            device=c("Zep188", "Zep344"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("Zep311", "Zep716"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device=c("Zep311", "Zep716"),
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device="Zep309",
            start="2012-12-10",
            end="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            device="Zep309",
            start="2020-03-23",
            end=yesterday,
            location="York"
        ),
        data.frame(
            device=c("PA1", "PA3"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("PA2", "PA4"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device=c("PA2", "PA4", "PA9"),
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device=c("PA5", "PA6"),
            start="2020-01-22",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("PA9"),
            start="2020-01-22",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device=c("PA7", "PA8", "PA10"),
            start="2020-01-22",
            end="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            device=c("PA7", "PA8", "PA10"),
            start="2020-03-23",
            end=yesterday,
            location="York"
        ),
        data.frame(
            device=c("Ari063", "Ari078"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("Ari086"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device="Ari086",
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device=c("Ari093"),
            start="2019-12-10",
            end="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            device="Ari093",
            start="2020-03-23",
            end=yesterday,
            location="York"
        ),
        data.frame(
            device=c("AQM388", "AQM390"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("AQM389"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device="AQM389",
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device=c("AQM391"),
            start="2019-12-10",
            end="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            device="AQM391",
            start="2020-03-23",
            end=yesterday,
            location="York"
        ),
        data.frame(
            device=c("AQY872", "AQY873A"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("AQY874"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device="AQY874",
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device=c("AQY875", "AQY875A2"),
            start="2019-12-10",
            end="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            device=c("AQY875", "AQY875A2"),
            start="2020-03-23",
            end=yesterday,
            location="York"
        )
    )
) %>%
    mutate(start = as_datetime(start),
           end=as_datetime(end) + days(1) - seconds(1)) %>%
    rename(device_name = device,
           location_name = location,
           start_time = start,
           end_time = end)

# Add the _b PurpleAir sensors
pa_b_deployments <- deployments_to_insert %>% 
    filter(grepl("^PA", device_name)) %>% 
    mutate(device_name = sprintf("%s_b", device_name))

deployments_to_insert <- rbind(deployments_to_insert, pa_b_deployments)

# Add device id and location id rather than names
deployments_to_insert <- deployments_to_insert %>%
    left_join(tbl(con, "lcsdevices") %>% select(device_id, device_name) %>% collect(), 
              by="device_name") %>%
    left_join(tbl(con, "locations") %>% select(location_id, location_name) %>% collect(), 
              by="location_name") %>%
    select(device_id, location_id, start_time, end_time)

dbAppendTable(con, "lcsdeployments", deployments_to_insert)

############ LCS measurements
dev_dt <- tbl(con, "lcsdevices") %>% 
            select(device_id, device_name) %>% 
            collect() %>%
            setDT()
measurands_dt <- tbl(con, "measurands") %>% 
                collect() %>%
                setDT()
versions_dt <- tbl(con, "lcsmeasurementversions") %>% 
                select(version_id, version_name) %>% 
                collect() %>%
                setDT()
for (fn in lcs_fns) {
    cat(sprintf("Inserting data from file %s...\n", fn))
    dt <- fread(fn) 
    dt <- dt[ !is.na(value)]
    dt[ dataset == "OOB", dataset := "out-of-box"]
    
    # Need to do something different for AQMesh as want to use
    # the rebased data rather than the raw cal 1
    # Note don't actually have Cal_2 for AQMesh at the time of writing this 
    # script but might have it later
    if (fn == "Data/AQMesh.csv") {
        dt <- dt[ dataset != "Cal_1" ]  # Remove unrebased first cals
        dt[ dataset == "Cal_1_Rebased", dataset := "cal1"]
        dt[ dataset == "Cal_2", dataset := "cal2"]
    } else {
        dt[ dataset == "Cal_1", dataset := "cal1"]
        dt[ dataset == "Cal_2", dataset := "cal2"]
    }
    setnames(dt, 
             old=c("timestamp", "value", "device", "dataset", "measurand"),
             new=c("time", "measurement", "device_name", "version_name", "measurand_name"))
    
    dt <- dev_dt[dt, on="device_name", nomatch=0]
    dt <- versions_dt[dt, on="version_name", nomatch=0]
    dt <- measurands_dt[dt, on="measurand_name", nomatch=0]
    dt[, device_name := NULL ]
    dt[, version_name := NULL ]
    dt[, measurand_name := NULL ]
    dt[, manufacturer := NULL ]
    setcolorder(dt, c("time", "device_id", "version_id", "measurand_id", "measurement"))
    
    # Save data to upload via psql
    fwrite(dt, gsub(".csv", "_to_insert.csv", fn))
    #dbAppendTable(con, "lcsmeasurements", dt)
}

############ ReferenceDevices
# Find all reference devices as a unique pair of location-species
# I know that in this initial dataset there is only one reference instrument
# per species per site, so can hardcode these instruments names as <location>-<species>-1
ref_dt <- fread("Data/Reference.csv") 
ref_dt <- ref_dt[ !is.na(reference)]

ref_devices <- ref_dt %>%
    distinct(location, variable) %>%
    mutate(name=sprintf("%s_%s_1", location, variable))

ref_devices_to_insert <- ref_devices %>% select(name)

dbAppendTable(con, "referencedevices", ref_devices_to_insert)

############ ReferenceMeasurements
# Combine the raw measurements back with the newly created reference device ids to insert
ref_to_insert <- ref_dt %>%
    left_join(tbl(con, "locations") %>% collect(), by=c("location"="location_name")) %>%
    left_join(ref_devices, by=c("location", "variable")) %>%
    left_join(tbl(con, "referencedevices") %>% collect(), by="name") %>%
    left_join(measurands_dt, by=c("variable"="measurand_name")) %>%
    select(time=timestamp, location_id, reference_device_id, measurand_id, measurement=reference)

fwrite(ref_to_insert, "Data/Reference_to_insert.csv")
#dbAppendTable(con, "referencemeasurements", ref_to_insert)
