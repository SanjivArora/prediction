source("common.R")

require(magrittr)
require(feather)

################################################################################
# Config
################################################################################

input_bucket <- 'ricoh-prediction-data-test2'
output_bucket <- 'ricoh-prediction-data-aligned'

parallel <- TRUE
days <- 1000
#days <- 30

margin_days <- 7

regions <- c('RNZ')
sources <- c('PMCount', 'Count')
#models <- c('E16', 'E15', 'C08') 
models <- c('E15')

###################################################
# Functions
################################################################################

processSource <- function(src) {
  print(paste("Processing", src))
  timeit(
    all <- instancesForBucket(input_bucket, sources=c(src), days=days),
    "getting file instances"
  )
  print(paste(length(all), "total data files"))
  region_to_fs <- groupBy(all, function(f) f$region)
  rs <- keys(region_to_fs)
  if(!identical(regions, NA)) {
    rs <- intersect(rs, regions)
  }
  for(region in rs) {
    region_fs <- region_to_fs[[region]]
    print(paste(length(region_fs), "data files for", region))
    model_to_fs <- groupBy(region_fs, function(f) f$model)
    ms <- keys(model_to_fs)
    if(!identical(models, NA)) {
      ms <- intersect(ms, models)
    }
    for(model in ms) {
      model_fs <- model_to_fs[[model]]
      timeit(
        date_to_df <- processModel(model, model_fs, region, src),
        paste("processing", model)
      )
      #date_to_df[keys(date_to_df)] %>% lapply(nrow) %>% print
      timeit(
        write_data(date_to_df, model, region, src),
        paste("writing", model)
      )
    }
  }
}

# Return a hash mapping dates to dataframes containing rows for those dates
processModel <- function(model, fs, region, src) {
  print(paste("Processing", length(fs), src, "data files for", model, "in", region))
  timeit(
    dfs <- plapply(fs, function(f) f$getDataFrame(), parallel=parallel),
    "getting dataframes"
  )
  # For each dataframe, group lines by date on which the data is retrieved from @remote
  timeit(
    date_groups <- plapply(
      dfs,
      function(df) {
        # Run garbage collection to free up memory for siblings
        gc(verbose=FALSE, full=TRUE)
        df %>% splitDataFrame %>% groupBy(function(row) row$RetrievedDate) %>% return
      },
      parallel=parallel
    ),
    "grouping into dates"
  )

  # Get a complete set of dates
  # Ignore dates from the future and dates older than <days> + <margin>
  timeit({
    dates <- Reduce(union, lapply(date_groups, function(x) x %>% keys))
    dates %<>% filterBy(function(d) d<= Sys.Date() && d >= Sys.Date() - (days + margin_days))
    dates %<>% unlist %>% sort},
    "getting complete set of dates"
  )
  
  timeit(
    dfs <- plapply(
      dates,
      function(date) {
        # Run garbage collection to free up memory for siblings
        gc(verbose=FALSE, full=TRUE)
        parts <- lapply(date_groups, function(h) getWithDefault(h, date, list()))
        rows <- concat(parts)
        # Remove duplicate samples
        rows <- uniqueBy(rows, function(r) c(r$Serial, r$RetrievedDate, r$RetrievedTime))
        res <- bindRowsForgiving(rows)
        return(res)
      },
      parallel=parallel
    ),
    "building final dataframes"
  )
  
  res <- hash(dates, dfs)
  return(res)
}

write_data <- function(date_to_df, model, region, src) {
  plapply(
    keys(date_to_df),
    function(date) {
      df <- date_to_df[[date]]
      write_df(df, date, model, region, src)
    },
    parallel=parallel,
    # Limit parallelism as this is very memory intensive
    ncores=8
  )
}

write_df <- function(df, date, model, region, src) {
  # <date>/region/model/source.feather
  path <- paste(date, region, model, paste(src, "feather", sep="."), sep="/")
  print(paste("Writing", path))
  s3write_using(df, FUN=write_feather, object=path, bucket=output_bucket)
}

################################################################################
# Process each source
################################################################################

timeit(lapply(sources, processSource))
