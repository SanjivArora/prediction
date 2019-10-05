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
days <- 1000
#days <- 30

max_past_days = 7
max_future_days = 1
# Maximum allowed time difference between readings, measured from first source to each subsequent source
max_hours_default <- 24
max_hours <- hash(
  # PMCount and Count are essential for most uses and usually requires well-aligned data.
  # RomVer is used less, and data alignment is less critical - allow use of Count + PMCount data even if matching RomVer isn't available.
  RomVer = 24*max_past_days
)

regions <- c('RNZ')
sources <- c('PMCount', 'Count', 'RomVer')
#sources <- c('PMCount')
#models <- c('E16', 'E15', 'C08') 
#models <- c('E19')
models <- NA

################################################################################
# Functions
################################################################################

maxHoursForSource <- function(source) {
  getWithDefault(max_hours, source, max_hours_default) %>% unname %>% unlist
}

getData <- function(days, cores, ...) {
  dates <-  Sys.Date() - lubridate::days(0:days-1)
  parts <- plapply(dates, function(date) getDataForDate(date, ...), parallel=parallel, ncores=cores)
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
getDataForDate <- function(date, region, model, sources=sources, bucket=input_bucket, look_after=max_future_days) {
  res <- readDF(date, region, model, sources[[1]], bucket)
  if(identical(res, NA)) {
    return(NA)
  }
  sources <- tail(sources, n=length(sources)-1)
  for(source in sources) {
    look_before <- ceiling(maxHoursForSource(source) / 24)
    dates <- date - lubridate::days(-look_before:look_after)
    candidates <- readDF(dates, region, model, source, bucket)
    if(identical(candidates, NA)) {
      return(NA)
    }
    #print(nrow(res))
    #print(ncol(res))
    #print(nrow(candidates))
    res %<>% mergeMatching(candidates, maxHoursForSource(source))
    #print(nrow(res))
    #print(ncol(res))
  }
  return(res)
  return(res)
}

mergeMatching <- function(df1, df2, hours) {
  joined <- inner_join(df1, df2, by=c('Serial'))
  # set names for canonical values
  canonical_names <- c('RetrievedDate', 'RetrievedTime', 'RetrievedDateTime', 'FileDate', 'Model')
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
  # Filter out results with a time difference outside the maximum window
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

writeDF <- function(df, date, model, region) {
  # <date>/<region>/<model>.feather
  date_string <- date %>% as.character('%Y%m%d')
  path <- paste(date_string, region, paste(model, "feather", sep="."), sep="/")
  print(paste("Writing", path))
  s3write_using(df, FUN=write_feather, object=path, bucket=output_bucket)
}

getModels <- function() {
  fs <- listCloudFiles(input_bucket)
  models <- lapply(fs, function(f) strsplit(f, '/')[[1]][[3]])
  models %<>% unique %>% unlist %>% sort
  return(models)
}

processModel <- function(region, model, cores) {
  tryCatch({
    print(paste("Getting predictors for", model, "in", region))
    res <- getData(days, cores, region, model, sources)
    if(identical(res, NA)) {
      print(paste("No days of complete data for", model))
      next
    }
    nser <- res$Serial %>% unique %>% length
    print(paste("Gathered", print(ncol(res)), "columns and", print(nrow(res)), "rows for", nser, "machines"))
    print(paste("Writing predictors for", model, "in", region))
    writeDF(res, Sys.Date(), model, region)
  }, error=function(e) {
    print(paste("Encountered error processing model", model, "in", region))
    print(e) 
  })
}

################################################################################
# Main
################################################################################

if(identical(models, NA)) {
  models <- getModels()
}

simultaneous_models <- (max(1, detectCores() / 8))
cores_per_model <- max(1, detectCores() / simultaneous_models)
for(region in regions) {
    plapply(
        models,
        function(model) processModel(region, model, cores_per_model),
        parallel=parallel,
        ncores=simultaneous_models
    )
}

#hist(res$timeDiff %>% as.integer, breaks=300)
