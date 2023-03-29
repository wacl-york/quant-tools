# This script updates the DB by making it explicit when instruments had sensors
# replaced, or when the manufacturers internal ID changed but the physical sensors
# remained the same.
# This only affects the Aeroqual and Zephyr instruments from the main study.
# 2023-03-01
library(tidyverse)
library(lubridate)
library(odbc)
library(DBI)

con <- dbConnect(odbc(), "QUANT")

update_reference_name <- function(old, new) {
    TYPE_ID <- 2
    # See if instrument already exists
    inst_exists <- tbl(con, "instrument") |>
                        filter(instrument == new) |>
                        summarise(exists = n() == 1) |>
                        pull(exists) |>
                        as.numeric() |>
                        as.logical()
    if (!inst_exists) {
        cat("No instrument exists, creating one\n")
        # Create instrument in the right type
        dbAppendTable(con, "instrument", 
                      tibble(instrument=new, instrumenttypeid=TYPE_ID))
        dbAppendTable(con, "referenceinstrument", 
                      tibble(instrument=new, instrumenttypeid=TYPE_ID))
    } 

    # Add sensors if don't exist, pull from old
    # I.e. get all sensors this old instrument had and add
    old_sensors <- tbl(con, "sensor") |>
                        filter(instrument == old) |>
                        rename(old=instrument) |>
                        collect()
    new_sensors <- tbl(con, "sensor") |>
                        filter(instrument == new) |>
                        rename(new=instrument) |>
                        collect()
    sensors_missing <- old_sensors |>
                          left_join(new_sensors, by=c('measurand', 'sensornumber')) |>
                          filter(is.na(new))

    if (nrow(sensors_missing) > 0) {
        sensors_missing$instrument <- new
        sensors_to_add <- sensors_missing |> 
          select(instrument, measurand, sensornumber)
        dbAppendTable(con, "sensor", sensors_to_add)
    }

    # Ditto for sensorcalibration
    old_sensorcals <- tbl(con, "sensorcalibration") |>
                        filter(instrument == old) |>
                        rename(old=instrument) |>
                        collect()
    new_sensorcals <- tbl(con, "sensorcalibration") |>
                        filter(instrument == new) |>
                        rename(new=instrument) |>
                        collect()
    sensorcals_missing <- old_sensorcals |>
                          left_join(new_sensorcals, by=c('measurand', 'sensornumber', 'calibrationname', 'dateapplied')) |>
                          filter(is.na(new))

    if (nrow(sensorcals_missing) > 0) {
        sensorcals_missing$instrument <- new
        sensorcals_to_add <- sensorcals_missing |> 
          select(instrument, measurand, sensornumber, calibrationname, dateapplied)
        dbAppendTable(con, "sensorcalibration", sensorcals_to_add)
    }
                         
    # NB: Updating measurement should cascade to flag, but best to be explicit
    dbExecute(con, "UPDATE measurement SET instrument = ?
                    WHERE instrument = ?",
              params=list(new, old))
    dbExecute(con, "UPDATE flag SET instrument = ?
                    WHERE instrument = ?",
              params=list(new, old))
}

# Had all values in a list here when ran them all
# List of old=new
vals_to_update <- list(
)

for (i in seq_along(vals_to_update)) {
    old <- names(vals_to_update)[i]
    new <- vals_to_update[[i]]
    cat(sprintf("Updating %s to %s (%d/%d)\n", old, new, i, length(vals_to_update)))
    update_reference_name(old, new)
}

update_reference_metadata <- function(obj) {
    dbExecute(con, "UPDATE referenceinstrument 
                    SET manufacturer = ?, model = ?
                    WHERE instrument = ?",
              params=list(obj$manufacturer, obj$model, obj$instrument))
}


ref_meta <- list(
)
lapply(ref_meta, update_reference_metadata)

delete_cals <- function(instrument, measurand, sensornumber, calibrationname, dateapplied) {
    cat(sprintf("On instrument %s and measurand %s\n", instrument, measurand))
    dbExecute(con, "DELETE FROM sensorcalibration WHERE
                    instrument = ? AND
                    measurand = ? AND
                    sensornumber = ? AND
                    calibrationname = ?",
              params=list(instrument, measurand, sensornumber, calibrationname))
}
missing_cals <- anti_join(tbl(con, "sensorcalibration"), tbl(con, "measurement")) |> collect()
pmap(missing_cals, delete_cals)

delete_sensors <- function(instrument, measurand, sensornumber) {
    cat(sprintf("On instrument %s and measurand %s\n", instrument, measurand))
    dbExecute(con, "DELETE FROM sensor WHERE
                    instrument = ? AND
                    measurand = ? AND
                    sensornumber = ?",
              params=list(instrument, measurand, sensornumber))
}
missing_sensors <- anti_join(tbl(con, "sensor"), tbl(con, "sensorcalibration")) |> collect()
pmap(missing_sensors, delete_sensors)

delete_ref <- function(instrument) {
    cat(sprintf("On instrument %s\n", instrument))
    dbExecute(con, "DELETE FROM referenceinstrument WHERE
                    instrument = ?",
              params=list(instrument))
}
missing_ref <- anti_join(tbl(con, "referenceinstrument"), tbl(con, "sensor")) |> select(instrument) |> collect()
pmap(missing_ref, delete_ref)

# 0 missing LCS so just remove reference from instrument and deployment table
delete_lcs <- function(instrument) {
    cat(sprintf("On instrument %s\n", instrument))
    dbExecute(con, "DELETE FROM lcsinstrument WHERE
                    instrument = ?",
              params=list(instrument))
}
missing_lcs <- anti_join(tbl(con, "lcsinstrument"), tbl(con, "sensor")) |> select(instrument) |> collect()
pmap(missing_lcs, delete_lcs)

# Find missing deployments and carry over, as didn't do this as part of the renaming procedure
missing_deployment <- anti_join(tbl(con, "instrument"), tbl(con, "deployment")) |> collect()
# Add missing deployments (did manually)

delete_ref2 <- function(instrument) {
    cat(sprintf("On instrument %s\n", instrument))
    dbExecute(con, "DELETE FROM deployment WHERE
                    instrument = ?",
              params=list(instrument))
    dbExecute(con, "DELETE FROM instrument WHERE
                    instrument = ?",
              params=list(instrument))
}
missing_ref2 <- anti_join(tbl(con, "instrument") |> filter(instrumenttypeid==2), tbl(con, "referenceinstrument")) |> select(instrument) |> collect()
pmap(missing_ref2, delete_ref2)
