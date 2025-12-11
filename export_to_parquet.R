library(DBI)
library(RPostgres)
library(tidyverse)
library(lubridate)
library(jsonlite)
library(arrow)

creds_fn <- 'creds.json'
CREDS <- fromJSON(creds_fn)
DB_DIR <- "db"
con <- dbConnect(
    Postgres(),
    dbname=CREDS$db,
    host=CREDS$host,
    port=CREDS$port,
    user=CREDS$username,
    password=CREDS$password
)

tables <- dbListTables(con)

non_partitioned_tables <- c("deployment", "flagtypes", "instrument", "instrumenttype", "lcscompany", "lcsinstrument",
                            "measurand", "referenceinstrument", "sensor", "sensorcalibration")
partitioned_tables <- c("flag", "measurement")
partitioned_views <- c("lcs", "ref", "lcs_hourly", "ref_hourly")

dump_unpartitioned_table <- function(tab) {
    # Make directory
    table_dir <- file.path(DB_DIR, tab)
    dir.create(table_dir, showWarnings=FALSE)
    
    # Download data and save to <tablename.parquet>
    fn <- file.path(table_dir, sprintf("%s.parquet", tab))
    if (file.exists(fn)) return()
    tbl(con, tab) |>
        collect() |>
        write_parquet(fn)
}

for (i in seq_along(non_partitioned_tables)) {
    tab <- non_partitioned_tables[i]
    cat(sprintf("Non partitioned table %d/%d\n", i, length(non_partitioned_tables)))
    dump_unpartitioned_table(tab)
}

# Partitioned tables are more awkward - how to partition? by measurand? by company? by time?
# measurand / instrument / version (aka calibrationname) / year / month / location (last one only for views)
dump_partitioned_table <- function(tab) {
    # Make directory
    table_dir <- file.path(DB_DIR, tab)
    
    # Get all partitions
    partitions <- tbl(con, tab) |>
        distinct(measurand, instrument, calibrationname) |>
        collect()
    
    for (i in 1:nrow(partitions)) {
        meas <- partitions$measurand[i]
        inst <- partitions$instrument[i]
        vers <- partitions$calibrationname[i]
        cat(sprintf("On partition %d/%d with %s\t%s\t%s\n", i, nrow(partitions), meas, inst, vers))
        
        # Make folders for partitions
        folder_path <- file.path(
            table_dir,
            sprintf("measurand=%s", meas),
            sprintf("instrument=%s", inst),
            sprintf("calibrationname=%s", vers)
        )
        fn <- file.path(folder_path, "data.parquet")
        if (file.exists(fn)) next
        dir.create(folder_path, showWarnings=FALSE, recursive = TRUE)
        
        # Download data and save to parquet
        tbl(con, tab) |>
            filter(
                measurand == meas,
                instrument == inst,
                calibrationname == vers
            ) |>
            collect() |>
            write_parquet(fn)
    }
}

dump_partitioned_view_ref <- function(tab) {
    # Make directory
    table_dir <- file.path(DB_DIR, tab)
    
    # Get all partitions
    partitions <- tbl(con, tab) |>
        distinct(measurand, version, location) |>
        collect()
    
    for (i in 1:nrow(partitions)) {
        meas <- partitions$measurand[i]
        vers <- partitions$version[i]
        loc <- partitions$location[i]
        cat(sprintf("On partition %d/%d with %s\t%s\t%s\n", i, nrow(partitions), meas, vers, loc))
        
        # Make folders for partitions
        folder_path <- file.path(
            table_dir,
            sprintf("measurand=%s", meas),
            sprintf("version=%s", vers),
            sprintf("location=%s", loc)
        )
        fn <- file.path(folder_path, "data.parquet")
        if (file.exists(fn)) next
        dir.create(folder_path, showWarnings=FALSE, recursive = TRUE)
        
        # Download data and save to parquet
        tbl(con, tab) |>
            filter(
                measurand == meas,
                version == vers,
                location == loc
            ) |>
            collect() |>
            write_parquet(fn)
    }
}

dump_partitioned_view_lcs <- function(tab) {
    # Make directory
    table_dir <- file.path(DB_DIR, tab)
    
    # Get all partitions
    partitions <- tbl(con, tab) |>
        distinct(measurand, instrument, version, location) |>
        collect()
    
    for (i in 1:nrow(partitions)) {
        meas <- partitions$measurand[i]
        inst <- partitions$instrument[i]
        vers <- partitions$version[i]
        loc <- partitions$location[i]
        cat(sprintf("On partition %d/%d with %s\t%s\t%s\n", i, nrow(partitions), meas, inst, vers, loc))
        
        # Make folders for partitions
        folder_path <- file.path(
            table_dir,
            sprintf("measurand=%s", meas),
            sprintf("instrument=%s", inst),
            sprintf("version=%s", vers),
            sprintf("location=%s", loc)
        )
        fn <- file.path(folder_path, "data.parquet")
        if (file.exists(fn)) next
        dir.create(folder_path, showWarnings=FALSE, recursive = TRUE)
        
        # Download data and save to parquet
        tbl(con, tab) |>
            filter(
                measurand == meas,
                instrument == inst,
                version == vers,
                location == loc
            ) |>
            collect() |>
            write_parquet(fn)
    }
}

#for (i in seq_along(partitioned_tables)) {
#    tab <- partitioned_tables[i]
#    cat(sprintf("Partitioned table %d/%d\n", i, length(partitioned_tables)))
#    dump_partitioned_table(tab)
#}

partitioned_views_ref <- partitioned_views[grepl("ref", partitioned_views)]
partitioned_views_lcs <- partitioned_views[grepl("lcs", partitioned_views)]

for (i in seq_along(partitioned_views_ref)) {
    tab <- partitioned_views_ref[i]
    cat(sprintf("Partitioned view %d/%d\n", i, length(partitioned_views_ref)))
    dump_partitioned_view_ref(tab)
}

for (i in seq_along(partitioned_views_lcs)) {
    tab <- partitioned_views_lcs[i]
    cat(sprintf("Partitioned view %d/%d\n", i, length(partitioned_views_lcs)))
    dump_partitioned_view_lcs(tab)
}

dbDisconnect(con)