# 2_populate_database.R
# ~~~~~~~~~~~~~~~~~~~~~

# Populates the Postgres database with data from the first portion
# of the QUANT study of the initial 5 companies from 2019-12-10 to
# 2021-06-30,
# as well as the Wider Participation study.
#
# Requires Postgres credentials to be available in the following 
# environment variables:
#   - QUANT_DB_HOST 
#   - QUANT_DB_DBNAME
#   - QUANT_DB_USER
#   - QUANT_DB_PASSWORD

# TODO post-hoc:
#    - Check have got all PA data
#    - Add sensor cal dates. For QUANT want to go through and check all makes sense with those exceptions
#       - i.e. zephyr only updated cals for NO on cal1, T + RH never got updated for any company etc...
#       - also do same for WP
#    - Add AQY875 to LCSinstruments

library(DBI)
library(RPostgres)
library(RSQLite)
library(tidyverse)
library(data.table)
library(lubridate)
library(jsonlite)

quant_devices_fn <- "~/repos/QUANTscraper/devices.json"
quant_scraping_config <- jsonlite::fromJSON(quant_devices_fn)
device_lut <- bind_rows(quant_scraping_config$manufacturers$devices)
yesterday <- as.character(today() - days(1))

con <- dbConnect(Postgres(), 
                 host=Sys.getenv("QUANT_DB_HOST"), 
                 dbname=Sys.getenv("QUANT_DB_DBNAME"),
                 user=Sys.getenv("QUANT_DB_USER"),
                 password=Sys.getenv("QUANT_DB_PASSWORD"))
con_sqlite <- dbConnect(SQLite(), "~/Documents/quant_data/quant.db")

# Load all saved data
lcs_fns <- paste0("Data/",
              c(
                "Aeroqual.csv",
                "AQMesh.csv",
                "PurpleAir.csv",
                "QuantAQ.csv",
                "Zephyr.csv"
                )
            )

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

measurands_to_insert <- data.frame(
                                   measurand=MEASUREMENT_COLS
)
#dbAppendTable(con, "measurand", measurands_to_insert)

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
    
    # Add LCSCompany
    dbAppendTable(con, "lcscompany", data.frame(company=manufacturer))
    
    # Due to the limitations of the original schema I had made dummy devices for PA's 
    # duplicate sensors, whereas now I can explicitly model them as multiple
    # sensors housed within the same instrument
    if (manufacturer == "PurpleAir") {
        dt[, sensor_num := ifelse(grepl("_b$", device), 2, 1)]
        dt[, device := gsub("_b$", "", device)]
    } else {
        dt[, sensor_num := 1]
    }
    
    # Save the devices associated with this manufacturer
    # Both in the main instruments table
    instruments_to_insert <- data.frame(instrument=unique(dt$device), instrumenttypeid=1)
    dbAppendTable(con, "instrument", instruments_to_insert)
    # And also with the additional LCS information
    lcsinstruments_to_insert <- data.frame(instrument=unique(dt$device), company=manufacturer,
                      study="QUANT") %>%
        inner_join(device_lut, by=c("instrument"="id")) %>%
        select(instrument, company, study, internalid=webid)
    dbAppendTable(con, "lcsinstrument", lcsinstruments_to_insert)
    
    # Find all measurands have at least one measurement for in any version
    sensors_to_insert <- dt %>% distinct(instrument=device, measurand, sensornumber=sensor_num)
    dbAppendTable(con, "sensor", sensors_to_insert)

    # Add Sensor Cals
    # The preprocessing that produced these data files has blanket applied cal 
    # labels to all measurements at a given timestamp
    # However there are occasions when this wasn't the case:
        # - No one updated Temp/RH
        # - Zephyr only updated NO on 1st cals (2nd cals already not included)
        # - AriSense didn't calibrate PM in first round and didn't update CO2 in second round
    # But, rather than making these changes here I will update the DB afterwards
    sensorcalibrations_to_insert <- dt %>% 
                    group_by(device, sensor_num, measurand, version) %>%
                    summarise(dateapplied=min(timestamp)) %>%
                    ungroup() %>%
                    select(instrument=device, sensornumber=sensor_num, measurand, calibrationname=version, dateapplied)
    dbAppendTable(con, "sensorcalibration", sensorcalibrations_to_insert)
    
    # Add measurements
    setnames(dt, old=c("timestamp", "device", "value", "version", "sensor_num"),
             new=c("time", "instrument", "measurement", "calibrationname", "sensornumber"))
    batch_size <- 1e5
    n_obs <- nrow(dt)
    start_points <- seq(from=1, to=n_obs, by=batch_size)
    for (start in start_points) {
        end <- min(n_obs, start + batch_size - 1)
        dbAppendTable(con, "measurement", dt[start:end])
    }
}

############ WiderParticipation Study
wp_fns <- paste0("Data/",
                  c("RLS.csv",
                    "Bosch.csv",
                    "SCS.csv",
                    "Vortex.csv",
                    "Modulair.csv",
                    "Oizom.csv",
                    "Kunak.csv",
                    "EI.csv",
                    "Clarity.csv"
                  )
)

wp_cal_dates <- bind_rows(list(
    list(
        "company"="RLS",
        # I.e. RLS never applied cals
        "cals_start"="2099-12-31"
    ),
    list(
        "company"="Modulair",
        "cals_start"="2021-07-17"
    ),
    list(
        "company"="EI",
        "cals_start"="2021-08-19"
    ),
    list(
        "company"="Bosch",
        "cals_start"="2021-09-01"
    ),
    list(
        "company"="Oizom",
        "cals_start"="2021-07-30"
    ),
    list(
        "company"="Kunak",
        "cals_start"="2021-08-03"
    ),
    list(
        "company"="Vortex",
        "cals_start"="2021-11-29"
    ),
    list(
        "company"="Clarity",
        "cals_start"="2021-08-18"
    ),
    list(
        "company"="SCS",
        "cals_start"="2021-07-17"
    )
))
for (fn in wp_fns) {

    cat(sprintf("Inserting data from file %s...\n", fn))
    dt <- fread(fn)
    dt <- dt[ !is.na(value)]
    dt <- dt[ measurand %in% MEASUREMENT_COLS ]

    # Rename labels
    manufacturer <- unique(dt$manufacturer)
    dt[, manufacturer := NULL ]
    
    # Add rough calibration version (assuming it is applied to all measurands)
    dt[, calibrationname := ifelse(timestamp < wp_cal_dates$cals_start[wp_cal_dates$company == manufacturer],
                                     "out-of-box",
                                     "cal1")]
    
    # Add LCSCompany
    dbAppendTable(con, "lcscompany", data.frame(company=manufacturer))
    
    # As with main QUANT study, there's only 1 company with multiple sensors
    # per gas: Bosch
    if (manufacturer == "Bosch") {
        dt[, sensor_num := ifelse(grepl("_c$", device), 3, ifelse(grepl("_b$", device), 2, 1))]
        dt[, device := gsub("_b$", "", device)]
        dt[, device := gsub("_c$", "", device)]
    } else {
        dt[, sensor_num := 1]
    }
    
    # Save the devices associated with this manufacturer
    # Both in the main instruments table
    instruments_to_insert <- data.frame(instrument=unique(dt$device), instrumenttypeid=1)
    dbAppendTable(con, "instrument", instruments_to_insert)
    # And also with the additional LCS information
    lcsinstruments_to_insert <- data.frame(instrument=unique(dt$device), company=manufacturer,
                      study="Wider Participation") %>%
        inner_join(device_lut, by=c("instrument"="id")) %>%
        select(instrument, company, study, internalid=webid)
    dbAppendTable(con, "lcsinstrument", lcsinstruments_to_insert)
    
    # Find all measurands have at least one measurement for in any version
    sensors_to_insert <- dt %>% distinct(instrument=device, measurand, sensornumber=sensor_num)
    dbAppendTable(con, "sensor", sensors_to_insert)

    # Add Sensor Cals
    sensorcalibrations_to_insert <- dt %>% 
                    group_by(device, sensor_num, measurand, calibrationname) %>%
                    summarise(dateapplied=min(timestamp)) %>%
                    ungroup() %>%
                    select(instrument=device, sensornumber=sensor_num, measurand, calibrationname, dateapplied)
    dbAppendTable(con, "sensorcalibration", sensorcalibrations_to_insert)
    
    # Add measurements
    setnames(dt, old=c("timestamp", "device", "value", "sensor_num"),
             new=c("time", "instrument", "measurement", "sensornumber"))
    batch_size <- 1e5
    n_obs <- nrow(dt)
    start_points <- seq(from=1, to=n_obs, by=batch_size)
    for (start in start_points) {
        end <- min(n_obs, start + batch_size - 1)
        dbAppendTable(con, "measurement", dt[start:end])
    }
}


############ ReferenceMeasurements
ref_dt <- tbl(con_sqlite, "ref_raw") %>%
            collect() %>%
            setDT()
ref_dt <- melt(ref_dt, id.vars=c("timestamp", "location"), variable.name="variable", value.name="reference")
# PM2.5's column was called PM25 in wide version as can't have . in
# column names. Revert back
ref_dt[ variable == "PM25", variable := "PM2.5"]
ref_dt[, timestamp := as_datetime(timestamp)]
ref_dt <- ref_dt[ !is.na(reference)]
ref_dt[, instrument := sprintf("Ref_%s_%s", location, variable)]

# Insert reference instruments
# Just use dummy instrument name as Ref_<Location>_<Sensor>
# Although obviously many of these instruments contain multiple sensors
# That can be cleaned up later
ref_instruments_to_insert <- ref_dt %>% 
    distinct(instrument) %>% 
    mutate(instrumenttypeid=2) %>%
    select(instrument, instrumenttypeid)

dbAppendTable(con, "instrument", ref_instruments_to_insert)
dbAppendTable(con, "referenceinstrument", ref_instruments_to_insert %>% select(instrument))

# Insert reference instrument deployments
ref_deployments_to_insert <- ref_dt %>% 
    group_by(instrument, location) %>%
    summarise(start=min(timestamp)) %>%
    mutate(finish=as_datetime(yesterday) + days(1) - seconds(1))
dbAppendTable(con, "deployment", ref_deployments_to_insert)

# Insert sensors, again just assume that each instrument has 1 of each sensor
ref_sensors_to_insert <- ref_dt %>% 
    distinct(instrument, measurand=variable) %>% 
    mutate(sensornumber=1) %>%
    select(instrument, measurand, sensornumber)
dbAppendTable(con, "sensor", ref_sensors_to_insert)

# Insert dummy cals
ref_cals_to_insert <- ref_sensors_to_insert %>%
    mutate(calibrationname="Ratified") %>%
    inner_join(ref_deployments_to_insert %>% select(instrument, dateapplied=start), by="instrument")
dbAppendTable(con, "sensorcalibration", ref_cals_to_insert)

# Insert measurements
ref_measurements <- ref_dt %>%
    inner_join(ref_cals_to_insert, by="instrument") %>%
    select(instrument, measurand, sensornumber, calibrationname, time=timestamp, measurement=reference)
batch_size <- 1e5
n_obs <- nrow(ref_measurements)
start_points <- seq(from=1, to=n_obs, by=batch_size)
for (start in start_points) {
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", ref_measurements[start:end])
}

# Flags
# I've got these already saved in the SQLite DB but they are in Wide format
# So I need to pivot to long then compare to the long measurements to find cases
# where the flagged value is NA but we have a measurement
ref_flags <- tbl(con_sqlite, "ref_corrections") %>%
            collect() %>%
            setDT()
ref_flags <- melt(ref_flags, id.vars=c("timestamp", "location"), variable.name="variable", value.name="reference")
ref_flags[ variable == "PM25", variable := "PM2.5"]
ref_flags[, timestamp := as_datetime(timestamp)]
ref_flags[, instrument := sprintf("Ref_%s_%s", location, variable)]
ref_flags[, `:=` (location=NULL, variable =NULL)]
setnames(ref_flags, old=c("timestamp", "reference"), new=c("time", "flagged"))

ref_flags <- ref_measurements[ref_flags, on=c("time", "instrument")]
ref_flags_to_insert <- ref_flags[ is.na(flagged) & !is.na(measurement), .(instrument, measurand, sensornumber, calibrationname, time)]
dbAppendTable(con, "flag", ref_flags_to_insert)

############ Device deployment history
# Deployment start and finish datetimes are both inclusive, so 
# in absence of further information set start date as midnight of
# deployment day and finish date as 1 second before midnight of final
# deployment day
deployments_to_insert <- bind_rows(
    list(
        data.frame(
            instrument=c("Zep188", "Zep344"),
            start="2019-12-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("Zep311", "Zep716"),
            start="2019-12-10",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument=c("Zep311", "Zep716"),
            start="2020-03-11",
            finish="2022-07-04",
            location="London"
        ),
        data.frame(
            instrument="Zep309",
            start="2012-12-10",
            finish="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            instrument="Zep309",
            start="2020-03-23",
            finish="2022-07-01",
            location="York"
        ),
        data.frame(
            instrument=c("PA1", "PA3", "PA4"),
            start="2019-12-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("PA2"),
            start="2019-12-10",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument=c("PA2", "PA5", "PA9"),
            start="2020-03-11",
            finish="2022-07-04",
            location="London"
        ),
        data.frame(
            instrument=c("PA6"),
            start="2020-01-22",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("PA9", "PA5"),
            start="2020-01-22",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument=c("PA7", "PA8", "PA10"),
            start="2020-01-22",
            finish="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            instrument=c("PA7", "PA8", "PA10"),
            start="2020-03-23",
            finish="2022-07-01",
            location="York"
        ),
        data.frame(
            instrument=c("Ari063", "Ari078"),
            start="2019-12-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("Ari086"),
            start="2019-12-10",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument="Ari086",
            start="2020-03-11",
            finish="2022-07-04",
            location="London"
        ),
        data.frame(
            instrument=c("Ari093"),
            start="2019-12-10",
            finish="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            instrument="Ari093",
            start="2020-03-23",
            finish="2022-07-01",
            location="York"
        ),
        data.frame(
            instrument=c("AQM388", "AQM390"),
            start="2019-12-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("AQM389"),
            start="2019-12-10",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument="AQM389",
            start="2020-03-11",
            finish="2022-07-04",
            location="London"
        ),
        data.frame(
            instrument=c("AQM391"),
            start="2019-12-10",
            finish="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            instrument="AQM391",
            start="2020-03-23",
            finish="2022-07-01",
            location="York"
        ),
        data.frame(
            instrument=c("AQY872", "AQY873A"),
            start="2019-12-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("AQY874"),
            start="2019-12-10",
            finish="2020-03-05",
            location="Manchester"
        ),
        data.frame(
            instrument="AQY874",
            start="2020-03-11",
            finish="2022-07-04",
            location="London"
        ),
        data.frame(
            instrument=c("AQY875", "AQY875A2"),
            start="2019-12-10",
            finish="2020-03-17",
            location="Manchester"
        ),
        data.frame(
            instrument=c("AQY875", "AQY875A2"),
            start="2020-03-23",
            finish="2022-07-01",
            location="York"
        )
    )
) %>%
    mutate(start = as_datetime(start),
           finish=as_datetime(finish) + days(1) - seconds(1))

wp_deployments_to_insert <- bind_rows(
    list(
        data.frame(
            instrument=c("Mod1", "Mod2", "Mod3",
                         "AQM1", "AQM2", "AQM3",
                         "Poll1", "Poll2",
                         "AP1", "AP2", "AP3",
                         "SA1", "SA2", "SA3",
                         "NS1", "NS2", "NS3",
                         "Prax1", "Prax2"
                        ), 
            start="2021-06-10",
            finish=yesterday,
            location="Manchester"
        ),
        data.frame(
            instrument=c("IMB1", "IMB2",
                         "Atm1", "Atm2"),
            start="2021-06-15",
            finish=yesterday,
            location="Manchester"
        )
    )
) %>%
    mutate(start = as_datetime(start),
           finish=as_datetime(finish) + days(1) - seconds(1))

dbAppendTable(con, "deployment", deployments_to_insert)
dbAppendTable(con, "deployment", wp_deployments_to_insert)

dbDisconnect(con)

