# 2_populate_database.R
# ~~~~~~~~~~~~~~~~~~~~~

# Populates the SQLite database with data from the first portion
# of the QUANT study of the initial 5 companies from 2019-12-10 to
# 2021-06-30.

library(DBI)
library(RSQLite)
library(tidyverse)
library(data.table)
library(lubridate)

con <- dbConnect(SQLite(), "quant.db")

# Load all saved data
lcs_fns <- paste0("Data/",
              c("Aeroqual.csv",
                "AQMesh.csv",
                "PurpleAir.csv",
                "QuantAQ.csv",
                "Zephyr.csv"))

MEASUREMENT_COLS <- c(
    "O3",
    "NO2",
    "NO",
    "CO",
    "CO2",
    "PM1",
    "PM2.5",
    "PM10",
    "Temperature",
    "RelHumidity"
)

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
            device=c("PA1", "PA3", "PA4"),
            start="2019-12-10",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("PA2"),
            start="2019-12-10",
            end="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            device=c("PA2", "PA5", "PA9"),
            start="2020-03-11",
            end=yesterday,
            location="London"
        ),
        data.frame(
            device=c("PA6"),
            start="2020-01-22",
            end=yesterday,
            location="Manchester"
        ),
        data.frame(
            device=c("PA9", "PA5"),
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
           end=as_datetime(end) + days(1) - seconds(1))

# Add the _b PurpleAir sensors
pa_b_deployments <- deployments_to_insert %>% 
    filter(grepl("^PA", device)) %>% 
    mutate(device= sprintf("%s_b", device))

deployments_to_insert <- rbind(deployments_to_insert, pa_b_deployments) %>%
                            select(device, location, start, end) %>%
                            setDT()

dbAppendTable(con, "deployments_raw", deployments_to_insert)

version_order <- data.frame(version=c("out-of-box", "cal1", "cal2"), version_int = 1:3)

############ LCS measurements
for (fn in lcs_fns) {

    cat(sprintf("Inserting data from file %s...\n", fn))
    # QuantAQ data is split amongst multiple files
    if (fn == "Data/QuantAQ.csv") {
        quant_aq_fns <- paste0('Data/QuantAQ_', c('OOB.csv', 'Cal1.csv', 'Cal2.csv'))
        dt <- rbindlist(lapply(quant_aq_fns, fread))
    } else {
        dt <- fread(fn)
    }
    dt <- dt[ !is.na(value)]
    dt <- dt[ measurand %in% MEASUREMENT_COLS ]

    # Rename labels
    dt[ dataset == "OOB", dataset := "out-of-box"]
    dt[ dataset == "Cal_1", dataset := "cal1"]
    dt[ dataset == "Cal_2", dataset := "cal2"]
    
    setnames(dt, 
             old=c("dataset"),
             new=c("version"))
    
    manufacturer <- unique(dt$manufacturer)
    dt[, manufacturer := NULL ]
    
    dt_wide <- dcast(dt, timestamp + version + device ~ measurand, value.var="value")
    # Add any measurands that may not have had measurements for
    cols_to_add <- setdiff(MEASUREMENT_COLS, colnames(dt_wide))
    for (col in cols_to_add) {
        dt_wide[ , (col) := NA ]
    }
    
    # Add deployments
    dt_wide <- dt_wide %>%
        inner_join(deployments_to_insert, by="device") %>%
        filter(timestamp >= start, timestamp <= end) %>%
        select(-start, -end) %>%
        setDT()

    setcolorder(dt_wide, c("timestamp", "device", "location", "version", MEASUREMENT_COLS))
    setnames(dt_wide, old=MEASUREMENT_COLS, new=gsub("\\.", "", MEASUREMENT_COLS))
    
    # Save the devices associated with this manufacturer
    devices_to_insert <- data.frame(manufacturer=manufacturer, device=unique(dt_wide$device))
    dbAppendTable(con, "devices", devices_to_insert)
    
    # Insert into DB
    dbAppendTable(con, "lcs_raw", dt_wide)

    # Find which devices have which sensors and insert into DB
    sensor_availability <- dt_wide[, lapply(.SD, function(x) as.integer(any(!is.na(x)))), .SDcols=gsub("\\.", "", MEASUREMENT_COLS), by=c("device", "version") ][ order(device, version)]
    dbAppendTable(con, "devices_versions_sensors", sensor_availability)

    ###########################################################################
    # Create table of just most recent dataset for each device/species
    # combination
    ###########################################################################
    # Convert to wide format with 3 columns for each of 3 datasets and measurand is column identifier
    dt <- melt(dt_wide, id.vars=c("timestamp", "device", "version", "location"), measure.vars=gsub("\\.", "", MEASUREMENT_COLS))
    dt_wide <- dcast(dt, timestamp + device + location + variable ~ version, value.var="value")
    for (col in c("out-of-box", "cal1", "cal2")) {
        if (! col %in% colnames(dt_wide)) {
            dt_wide[ , (col) := NA ]
        }
    }
    # Coalesce in descending time order of calibration version
    dt_wide[, latest := ifelse(!is.na(cal2), cal2,
                               ifelse(!is.na(cal1), cal1,
                                      ifelse(!is.na(`out-of-box`), `out-of-box`, NA)))]
    # Convert to wide format with 1 column per species
    dt_wide <- dcast(dt_wide, timestamp + device + location ~ variable, value.var="latest")
    setcolorder(dt_wide, c("timestamp", "device", "location", gsub("\\.", "", MEASUREMENT_COLS)))

    # Insert into DB
    dbAppendTable(con, "lcs_latest_raw", dt_wide)
}


############ ReferenceMeasurements
# Combine the raw measurements back with the newly created reference device ids to insert
ref_dt <- fread("Data/Reference.csv") 
ref_dt <- ref_dt[ !is.na(reference)]

ref_wide <- dcast(ref_dt, timestamp + location ~ variable, value.var="reference")
# Add any measurands that may not have had measurements for
cols_to_add <- setdiff(MEASUREMENT_COLS, colnames(ref_wide))
for (col in cols_to_add) {
    ref_wide[ , (col) := NA ]
}

setcolorder(ref_wide, c("timestamp", "location", MEASUREMENT_COLS))
setnames(ref_wide, old=MEASUREMENT_COLS, new=gsub("\\.", "", MEASUREMENT_COLS))

dbAppendTable(con, "ref_raw", ref_wide)
