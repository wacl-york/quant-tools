# After the study finished, missing gaps in the LCS data were filled in by rescraping.
# Rather than try to automate labelling these with the correct calibration version, they have
# been given a placeholder value of 'Rescraped'.
# This script was used as part of a manual process to assign the correct calibration label.
library(tidyverse)
library(lubridate)
library(trelliscopejs)
library(plotly)
library(odbc)
library(data.table)
library(dbplyr)
library(DBI)

con <- dbConnect(odbc(), "QUANT")

# Find all instruments with Rescraped data
# 41 instruments, nearly all
has_rescraped <- tbl(con, "measurement") |>
                    filter(calibrationname == 'Rescraped') |>
                    distinct(instrument) |>
                    arrange(instrument) |>
                    pull(instrument)

# For each instrument:
for (inst in has_rescraped) {

    # Get all days with rescraped data
    dates <- tbl(con, "lcs_hourly") |>
                filter(instrument == inst,
                       version == 'Rescraped') |>
                distinct(date=as_date(time)) |>
                pull(date)
    
    # For each day with rescraped data
    # Get all data from that day +/- 1 for all calibration versions
    df <- map_dfr(setNames(dates, dates), function(dt) {
        start_date <- dt - days(1)
        end_date <- dt + days(2)
        tbl(con, "lcs_hourly") |>
            filter(time >= start_date, time < end_date,
                   instrument == inst) |>
            select(time, version, measurand, measurement) |>
            collect()
    }, .id="rescraped_date") |>
        mutate(rescraped_date = as_date(rescraped_date))
    
    # Check what cal versions have
    df |> count(version)
    df |> count(measurand, version)
    df |> count(measurand, year(time), version)

    # Plot with trelliscope and see if can make general update rule
    # Also need to facet by measurand!
    df |>
        ggplot(aes(x=time, y=measurement, colour=version)) +
            geom_line() +
            trelliscopejs::facet_trelliscope(~ measurand + rescraped_date, scales = "free",
                                             nrow=2,
                                             ncol=4)
}

# Go through and flag measurements from rescraped days with a warning flag
# NB: Only pull cal1 version from AQY as had simultaneous measurements
# Can do this since haven't updated lcs view yet so still have 'Rescraped' version
been_rescraped <- tbl(con, "lcs") |>
                    filter(version == 'Rescraped') |>
                    select(-location, flagtype_old=flag, reason_old=flagreason) |>
                    left_join(tbl(con, "measurement"),
                              by=c("time", "instrument", "sensornumber", "measurand", "measurement")) |>
                    # For AQY it was cal1 that was retrospectively added, not MOMA
                    filter(!(instrument %in% c('AQY872', 'AQY873A', 'AQY875A2') & calibrationname == 'MOMA')) |>
                    select(instrument, measurand, sensornumber, calibrationname, time, flagtype_old, reason_old) |>
                    mutate(reason = 'Retrospectively collected', flagtype='Warning') |>
                    collect()
# Combine flags if already had one
been_rescraped <- been_rescraped |> 
    mutate(
        flagtype = ifelse(
            is.na(flagtype_old), flagtype,  # If no previous flag, use current one
            ifelse(flagtype_old == 'Error', flagtype_old, flagtype)  # If had an error before, keep as error, otherwise use warning
        ),
        reason = ifelse(
            is.na(reason_old), reason,
            ifelse(flagtype_old == 'Error',  # Concatenate flag reasons together. If had error previously this reason comes first, else use this first
                   sprintf("%s,%s", reason_old, reason),
                   sprintf("%s,%s", reason, reason_old)
                   )
            # Concatenate flag reasons together
        )
        ) |>
    select(-flagtype_old, -reason_old)

setDT(been_rescraped)

# Rather than doing an upsert, I'll delete the rows that previously had flags and reinsert them
rows_to_delete <- been_rescraped[str_length(been_rescraped[, gsub("Retrospectively collected", "", reason)]) > 0]
for (i in 1:nrow(rows_to_delete)) {
    cat(sprintf("On row %d/%d\n", i, nrow(rows_to_delete)))
    dbExecute(con, 
              "DELETE FROM flag WHERE instrument = ? AND measurand = ? AND sensornumber = ?
              AND calibrationname = ? AND time = ?",
              params=list(
                  rows_to_delete$instrument[i],
                  rows_to_delete$measurand[i],
                  rows_to_delete$sensornumber[i],
                  rows_to_delete$calibrationname[i],
                  rows_to_delete$time[i]
              ))
}


# Add to flag!
batch_size <- 1e5
n_obs <- nrow(been_rescraped)
start_points <- seq(from=1, to=n_obs, by=batch_size)
for (start in start_points) {
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "flag", been_rescraped[start:end])
}

dbDisconnect(con)