# collate_PurpleAir_indoors.R
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Uploads the CF=1 calibrations (i.e. those designed for indoor
# environments) directly to the DB.
source("~/repos/quant-tools/load_data.R")
library(tidyverse)
library(odbc)

# Uploads the 'indoor' calibrations from PurpleAir, i.e. the 'cf' fields, to the database
df_clean <- load_data("~/Documents/quant_data/Clean/",
                      companies="PurpleAir",
                      resample="1 minute",
                      subset=c(
                          "PM1_cf",
                          "PM1_cf_b",
                          "PM2.5_cf",
                          "PM2.5_cf_b",
                          "PM10_cf",
                          "PM10_cf_b"
                      )) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()

to_upload <- df_clean |>
    mutate(sensornumber = ifelse(grepl("_b$", measurand), 2, 1),
           calibrationname = 'indoor',
           measurand = gsub('_.+', '', measurand)) |>
    select(instrument=device, measurand, sensornumber, calibrationname, time=timestamp, measurement=value) |>
    filter(!is.na(measurement))

# Upload straight to DB
con <- dbConnect(odbc(), "QUANT")

# Create calibration version first
sensorcals <- expand_grid(
    instrument=paste0('PA', 1:10),
    measurand=c('PM1', 'PM2.5', 'PM10'),
    sensornumber=1:2,
    calibrationname='indoor',
    dateapplied=as_datetime('2019-12-10 14:37:00')
)
dbAppendTable(con, "sensorcalibration", sensorcals)

batch_size <- 1e5
n_obs <- nrow(to_upload)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", to_upload[start:end, ])
    i <- i + 1
}
dbDisconnect(con)
