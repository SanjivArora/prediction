

################################################################################
# Functisource("common.R")

require(magrittr)
require(feather)
require(lubridate)

################################################################################
# Config
################################################################################

input_bucket <- 'ricoh-prediction-data-aligned'
output_bucket <- 'ricoh-prediction-data-cache'

parallel <- TRUE
#days <- 1000
days <- 30

regions <- c('RNZ')
sources <- c('PMCount', 'Count')
#sources <- c('PMCount')
#models <- c('E16', 'E15', 'C08') 
models <- c('C08')
################################################################################

getData <- function(days, ...) {
  dates <-  Sys.Date() - lubridate::days(0:days-1)
  parts <- lapply(dates, function(date) getDataForDate(date, ...))
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
    print(nrow(res))
    print(ncol(res))
    print(nrow(candidates))
    res %<>% mergeMatching(candidates)
    print(nrow(res))
    print(ncol(res))
  }
  return(res)
}

x1 <- NA
x2 <- NA
mergeMatching <- function(df1, df2) {
  index_fields <- c('Serial', 'RetrievedDate', 'RetrievedTime')
  x1 <<- df1
  x2 <<- df2
  res <- inner_join(df1, df2, by=index_fields[1:2])
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
  print(paths)
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