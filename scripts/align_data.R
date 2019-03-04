source("common.R")

require(magrittr)
require(feather)
require(purrr)

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
sources <- c('PMCount', 'Count', 'RomVer')
#sources <- c('PMCount')
models <- c('E16', 'E15', 'C08') 
#models <- c('E15')

################################################################################
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
        writeData(date_to_df, model, region, src),
        paste("writing", model)
      )
    }
  }
}

# Return a hash mapping dates to dataframes containing rows for those dates
processModel <- function(model, fs, region, src) {
  index_fields_all <- c('Serial', 'RetrievedDate', 'RetrievedTime', 'FileDate')
  index_fields <- index_fields_all[1:3]
  print(paste("Processing", length(fs), src, "data files for", model, "in", region))
  timeit(
    dfs <- plapply(fs, function(f) f$getDataFrame(), parallel=parallel),
    "getting dataframes"
  )
  all_filedates <- lapply(fs, function(f) f$date %>% as.character('%Y%m%d'))
  filedate_to_df <- hash(all_filedates, dfs)
  # Get indices
  timeit(
    index_parts <- plapply(
      dfs,
      function(df) df[,index_fields_all],
      parallel=parallel
    ),
    "getting indices"
  )

  # Take most recent readings
  timeit({
      indices <- bindRowsForgiving(index_parts)
      # Keep most recent (Serial, RetrievedDate, RetrievedTime, FileDate)
      indices %<>% arrange(desc(FileDate))
      indices %<>% distinct(Serial, RetrievedDate, RetrievedTime, .keep_all=TRUE)
    },
    "getting indices for most recent readings"
  )
  
  # Get a complete set of dates
  # Ignore dates from the future and dates older than <days> + <margin>
  timeit({
      dates <- indices$RetrievedDate %>% unique
      dates %<>% subset(dates<= Sys.Date() && dates >= Sys.Date() - (days + margin_days))
      dates %<>% unlist %>% sort
      date_strings <- dates %>% as.character('%Y%m%d')
    },
    "getting complete set of reading dates"
  )
  
  timeit(
    dfs <- plapply(
      dates,
      function(date) {
        idxs <- indices[indices$RetrievedDate==date,]
        filedates <- idxs$FileDate %>% unique %>% as.character('%Y%m%d')
        parts <- lapply(
          filedates,
          function(filedate) {
            df <- filedate_to_df[[filedate]]
            res <- inner_join(idxs, df, by=index_fields_all)
            return(res)
          }
        )
        res <- bindRowsForgiving(parts)
        return(res)
      },
      parallel=parallel
    ),
    "building final dataframes"
  )
  
  res <- hash(date_strings, dfs)
  return(res)
}

writeData <- function(date_to_df, model, region, src) {
  plapply(
    keys(date_to_df),
    function(date) {
      df <- date_to_df[[date]]
      writeDF(df, date, model, region, src)
    },
    parallel=parallel
  )
}

writeDF <- function(df, date, model, region, src) {
  # <date>/<region>/<model>/<source>.feather
  path <- paste(date, region, model, paste(src, "feather", sep="."), sep="/")
  print(paste("Writing", path))
  s3write_using(df, FUN=write_feather, object=path, bucket=output_bucket)
}

################################################################################
# Process each source
################################################################################

timeit(lapply(sources, processSource))
