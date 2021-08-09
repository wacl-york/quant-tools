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

con <- dbConnect(SQLite(), "sqlite-db/quant2.db")

# Load all saved data
lcs_fns <- paste0("Data/",
              c("Aeroqual.csv",
                "AQMesh.csv",
                "PurpleAir.csv",
                "QuantAQ_Cal1.csv",
                "QuantAQ_Cal2.csv",
                "QuantAQ_OOB.csv",
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
    "RelHumidity",
    "AirPressure"
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
           end=as_datetime(end) + days(1) - seconds(1))

# Add the _b PurpleAir sensors
pa_b_deployments <- deployments_to_insert %>% 
    filter(grepl("^PA", device)) %>% 
    mutate(device= sprintf("%s_b", device))

deployments_to_insert <- rbind(deployments_to_insert, pa_b_deployments) %>%
                            select(device, location, start, end)

dbAppendTable(con, "lcsdeployments", deployments_to_insert)

############ LCS measurements
for (fn in lcs_fns) {
    cat(sprintf("Inserting data from file %s...\n", fn))
    dt <- fread(fn) 
    dt <- dt[ !is.na(value)]
    dt <- dt[ measurand %in% MEASUREMENT_COLS ]
    dt[ dataset == "OOB", dataset := "out-of-box"]
    
    # Need to do something different for AQMesh as want to use
    # the rebased data rather than the raw cal 1
    # Note don't actually have Cal_2 for AQMesh at the time of writing this 
    # script but might have it later
    if (fn == "Data/AQMesh.csv") {
        dt <- dt[ dataset != "Cals_1" ]  # Remove unrebased first cals
        dt[ dataset == "Cals_1_Rebased", dataset := "cal1"]
        dt[ dataset == "Cals_2", dataset := "cal2"]
    } else {
        dt[ dataset == "Cal_1", dataset := "cal1"]
        dt[ dataset == "Cal_2", dataset := "cal2"]
    }
    setnames(dt, 
             old=c("dataset"),
             new=c("version"))
    
    dt[, manufacturer := NULL ]
    
    dt_wide <- dcast(dt, timestamp + version + device ~ measurand, value.var="value")
    # Add any measurands that may not have had measurements for
    cols_to_add <- setdiff(MEASUREMENT_COLS, colnames(dt_wide))
    for (col in cols_to_add) {
        dt_wide[ , (col) := NA ]
    }
    
    setcolorder(dt_wide, c("timestamp", "device", "version", MEASUREMENT_COLS))
    setnames(dt_wide, old=c("PM2.5"), new=c("PM25"))
    
    # Insert into DB
    dbAppendTable(con, "lcs_raw", dt_wide)
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
setnames(ref_wide, old=c("PM2.5"), new=c("PM25"))

dbAppendTable(con, "ref_raw", ref_wide)
