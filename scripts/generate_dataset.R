source("common.R")

require(magrittr)
require(feather)
require(lubridate)
require(data.table)

################################################################################
# Config
################################################################################

input_bucket <- 'ricoh-prediction-data-aligned'
output_bucket <- 'ricoh-prediction-data-cache'

parallel <- TRUE
#days <- 1000
days <- 30

# Maximum allowed time difference between readings, measured from first source to each subsequent source
max_hours <- 12

regions <- c('RNZ')
sources <- c('PMCount', 'Count', 'RomVer')
#sources <- c('PMCount')
#models <- c('E16', 'E15', 'C08') 
models <- c('E16')

################################################################################
# Functions
################################################################################

getData <- function(days, ...) {
  dates <-  Sys.Date() - lubridate::days(0:days-1)
  parts <- plapply(dates, function(date) getDataForDate(date, ...), parallel=parallel)
  parts %<>% filterNA
  if(length(parts)==0) {
    return(NA)
  }
  res <- bindRowsForgiving(parts)
  return(res)
}

# Match rows in the initial source with rows in subsequent sources that are as close in time as possible.
# Check data from look_before and look_after days before and after the specified date.
# Return NA if there is no valid data
getDataForDate <- function(date, region, model, sources=sources, bucket=input_bucket, look_before=1, look_after=1) {
  res <- readDF(date, region, model, sources[[1]], bucket)
  if(identical(res, NA)) {
    return(NA)
  }
  sources <- tail(sources, n=length(sources)-1)
  dates <- date - lubridate::days(-look_before:look_after)
  for(source in sources) {
    candidates <- readDF(dates, region, model, source, bucket)
    if(identical(candidates, NA)) {
      return(NA)
    }
    #print(nrow(res))
    #print(ncol(res))
    #print(nrow(candidates))
    res %<>% mergeMatching(candidates)
    #print(nrow(res))
    #print(ncol(res))
  }
  return(res)
}

mergeMatching <- function(df1, df2, hours=max_hours) {
  joined <- inner_join(df1, df2, by=c('Serial'))
  # set names for canonical values
  canonical_names <- c('RetrievedDate', 'RetrievedTime', 'FileDate', 'Model')
  for(name in canonical_names) {
    new <- paste(name, ".x", sep="")
    if(new %in% names(joined)) {
      setnames(joined, c(new), c(name))
    }
  }
  # Calculate time deltas
  time_x <- makeDateTimes(joined$RetrievedDate, joined$RetrievedTime)
  time_y <- makeDateTimes(joined$RetrievedDate.y, joined$RetrievedTime.y)
  joined$timeDiff <- time_x - time_y
  # Take reading with lowest time difference
  grouped <- group_by(joined, Serial, RetrievedDate, RetrievedTime)
  res <- grouped %>% arrange(abs(timeDiff)) %>% filter(row_number() == 1) %>% ungroup
  # Filter out results with a time difference greater than max_hours
  res %<>% filter(abs(timeDiff) <= 3600 * hours)
  return(res)
}

# Reads in data for a given (region, model, source) combination for the specified dates as one dataframe
# Return NA if no matching data files exist
readDF <- function(dates, region, model, source, bucket=input_bucket) {
  paths <- dates %>% lapply(function(date) getPath(date, region, model, source))
  parts <- paths %>% lapply(function(path) getCloudFile(path, bucket))
  parts %<>% filterNA
  parts %<>% lapply(readFeatherObject)
  if(length(parts) == 0) {
    return(NA)
  }
  res <- bindRowsForgiving(parts)
  #print(paths)
  return(res)
}

getPath <- function(date, region, model, source) {
  date_string <- as.character(date, '%Y%m%d')
  path <- paste(date_string, region, model, source, sep='/')
  path %<>% paste('.feather', sep='')
  return(path)
}

################################################################################
# Main
################################################################################

res <- NA
for(region in regions) {
  for(model in models) {
    res <<- getData(days, region, model, sources)
    print(nrow(res))
    print(ncol(res))
  }
}

#hist(res$timeDiff %>% as.integer, breaks=300)
