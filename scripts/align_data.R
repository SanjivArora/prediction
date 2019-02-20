require(magrittr)
require(feather)

source("common.R")

################################################################################
# Config
################################################################################

input_bucket <- 'ricoh-prediction-data-test2'
output_bucket <- 'ricoh-prediction-data-aligned'

parallel <- TRUE
#days <- 1000
days <- 30
margin <- 7

regions <- c('RNZ')
sources <- c('PMCount')
models <- c('E16', 'C08') 

################################################################################
# Establish an S3 connection so library works correctly with child processes
# (using S3 in parallel fails without this)
################################################################################

bucketlist() %>% invisible

################################################################################
# Functions
################################################################################

processSource <- function(source) {
  print(paste("Processing", source))
  all <- instancesForBucket(input_bucket, sources=c(source), days=days)
  print(paste(length(all), "total data files"))
  region_to_fs <- groupBy(all, function(f) f$region)
  rs <- keys(region_to_fs)
  if(!identical(regions, NA)) {
    rs <- intersect(rs, regions)
  }
  for(region in rs) {
    region_fs <- region_to_fs[[region]]
    print(paste("Processing", length(region_fs), " data files for", region))
    model_to_fs <- groupBy(region_fs, function(f) f$model)
    ms <- keys(model_to_fs)
    if(!identical(models, NA)) {
      ms <- intersect(ms, models)
    }
    for(model in ms) {
      model_fs <- model_to_fs[[model]]
      date_to_df <- processModel(model, model_fs, region, source)
      write_data(date_to_df, model, region, source)
    }
  }
}

# Return a hash mapping dates to dataframes containing rows for those dates
processModel <- function(model, fs, region, source) {
  print(paste("Processing", length(fs), source, "for", model, "in", region))
  dfs <- plapply(fs, function(f) f$getDataFrame(), parallel=parallel)
  # For each dataframe, group lines by date on which the data is retrieved from @remote
  date_groups <- plapply(
    dfs,
    function(df) df %>% splitDataFrame %>% groupBy(function(row) row$RetrievedDate),
    parallel=parallel
  )

  # Get a complete set of dates
  # Ignore dates from the future and dates older than <days> + <margin>
  dates <- Reduce(union, lapply(date_groups, function(x) x %>% keys))
  dates %<>% filterBy(function(d) d<= Sys.Date() && d >= Sys.Date() - (days + margin))
  dates %<>% unlist %>% sort
  
  dfs <- plapply(
    dates,
    function(date) {
      parts <- lapply(date_groups, function(h) getWithDefault(h, date, list()))
      rows <- concat(parts)
      # Remove duplicate samples
      rows <- uniqueBy(rows, function(r) c(r$Serial, r$RetrievedDate, r$RetrievedTime))
      res <- bindRowsForgiving(rows)
      return(res)
    },
    parallel=parallel
  )
  
  res <- hash(dates, dfs)
  return(res)
}

write_data <- function(date_to_df, model, region, source) {
  plapply(
    keys(date_to_df),
    function(date) {
      df <- date_to_df[[date]]
      write_df(df, date, model, region, source)
    },
    parallel=parallel
  )
}

write_df <- function(df, date, model, region, source) {
  # <date>/region/model/source.feather
  path <- paste(date, region, model, paste(source, "feather", sep="."), sep="/")
  print(paste("Writing", path))
  s3write_using(df, FUN=write_feather, object=path, bucket=output_bucket)
}

################################################################################
# Process each source
################################################################################

lapply(sources, processSource)
