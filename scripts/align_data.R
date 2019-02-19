require(magrittr)

source("common.R")

################################################################################
# Config
################################################################################

csv_bucket <- 'ricoh-prediction-data-test2'
parallel <- TRUE
#days <- 1000
days <- 30

regions <- c('RNZ')
sources <- c('PMCount')
models <- c('C08') 

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
  all <- instancesForBucket(csv_bucket, sources=c(source), days=days)
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
      processModel(model, model_fs, region, source)
    }
  }
}

processModel <- function(model, fs, region, source) {
  print(paste("Processing", length(fs), source, "for", model, "in", region))
  dfs <- plapply(fs, function(f) f$getDataFrame(), parallel=parallel)
  # For each dataframe, group lines by date on which the data is retrieved from @remote
  date_groups <- plapply(dfs, function(df) df %>% splitDataFrame %>% groupBy(function(row) row$RetrievedDate))
  x1 <<- date_groups
  # Get a complete set of dates
  # Ignore dates from the future and dates older than <days>
}
  
################################################################################
# Process each source
################################################################################

lapply(sources, processSource)
