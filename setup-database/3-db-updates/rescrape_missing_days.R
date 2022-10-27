#
#    rescrape_missing_days.py
#    ~~~~~~~~~~~~~~~~~~~~~~~~
#
#    Runs the scraper over missing days of data.
#    Uses the df_avail.rds object saved during execution of
#    update_lcs.R which contains information regarding which instruments
#    have data for which days.
#    Or can be re-collated using the same query as in that script.

library(tidyverse)
library(lubridate)

scrape_script = "~/repos/QUANTscraper/run_scrape.py"

df <- readRDS("~/repos/quant-tools/df_avail.rds")
# Add all times to DB
entries <- distinct(df, company, instrument, instrumenttypeid, internalid, study)
times <- map_dfr(1:nrow(entries), function(i) {
     start <- df |> filter(instrument == entries[[i, 'instrument']]) |> summarize(mn = min(date)) |> pull(mn)
     end <- df |> filter(instrument == entries[[i, 'instrument']]) |> summarize(mn = max(date)) |> pull(mn)
     times <- seq.POSIXt(from=as_datetime(start), to=as_datetime(end), by="1 day")
     tibble(
            instrument=entries[[i, 'instrument']],
            company=entries[[i, 'company']],
            instrumenttypeid=entries[[i, 'instrumenttypeid']],
            internalid=entries[[i, 'internalid']],
            study=entries[[i, 'study']],
            date=times
     )
})
df <- times |>
    left_join(df, by=c("instrument", "date", "company", "instrumenttypeid", "internalid", "study")) |>
    filter(is.na(avail), company != "PurpleAir")

# Want to run separately for each study as have different output folders
studies <- list(
                list(
                    name = "QUANT",
                    raw_id = "1Dd8KWC99WQkV4qBvzWV3ZOUcgs4ATxEJ",
                    clean_id = "1a3HpQ5NadN__eirtYda17SgqNezpwuW2"
                 ),
                list(
                    name = "Wider Participation",
                    raw_id = "1FJSxwrn7pDlofn9pJJbi3nNWNUWlqnEh",
                    clean_id = "1GgUFDq9KfgAMy83RhR4oLKU5WWqeiKoc"
                 )
)

for (tstudy in studies) {
    sub_df <- df |> filter(study == tstudy$name)
    all_dates <- unique(sub_df$date)
    for (tdate in all_dates) {
        # Get all instruments missing on this date
        missing_inst <- sub_df |> filter(date == tdate) |> distinct(instrument) |> pull(instrument)
        inst_str <- paste(missing_inst, collapse=' ')
        sys_call <- sprintf("python %s --scrape-devices %s --preprocess-devices %s --date %s --gdrive-raw-id %s --gdrive-clean-id %s",
                            scrape_script,
                            inst_str,
                            inst_str,
                            format(as_datetime(tdate), "%Y-%m-%d"),
                            tstudy$raw_id,
                            tstudy$clean_id)
        system(sys_call)
    }
}



