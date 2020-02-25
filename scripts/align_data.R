source("common.R")

require(magrittr)
require(feather)
require(purrr)
require(Xmisc)

################################################################################
# Config
################################################################################

input_bucket <- 'ricoh-prediction-data'
output_bucket <- 'ricoh-prediction-data-aligned'

parallel <- TRUE
# Overwritten to incremental_days if --incremental is true
#days <- 30
days <- 1000
incremental_days <- 14

margin_days <- 2

regions <- c('RNZ')
sources <- c('PMCount', 'Count', 'RomVer')
#sources <- c('PMCount')
#models <- c('E16', 'E15', 'C08') 
#models <- c('E15')
models <- NA

ncores <- max(1, detectCores() / 4)

################################################################################
# Functions
################################################################################

processSource <- function(src) {
  # Get a complete set of dates
  # Ignore dates from the future and dates older than <days> - <margin>
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
    date_to_mmr <- readMMRs(region)
    for(model in ms) {
      tryCatch({
        model_fs <- model_to_fs[[model]]
        timeit(
          date_to_df <- processModel(model, model_fs, region, src),
          paste("processing", model)
        )
        #date_to_df[keys(date_to_df)] %>% lapply(nrow) %>% print
        timeit(
          writeData(date_to_df, model, region, src, date_to_mmr),
          paste("writing", model)
        )
      },
      error=function(e) {
        print(paste("Encountered error processing model", model, "in", region))
        print(e)
      })
    }
  }
}

readMMRs <- function(region) {
  print(paste("Readings MMR files for", region))
  mmr_pattern <- paste(region, '_ARemote_MMR', sep='')
  timeit(
    fs <- instancesForBucket(input_bucket, days=days+margin_days, pattern=mmr_pattern, parallel=parallel),
    "instantiating MMRs"
  )
  all_filedates <- lapply(fs, function(f) f$date %>% as.character('%Y%m%d'))
  gc()
  timeit(
    # Only take serial
    mmrs <- plapply(fs, function(f) {gc(); return(f$getDataFrame(skip_processing=T))[,c("Device.Serial.Number", "Site.Name")]}, parallel=parallel, ncores=ncores),
    "getting MMRs"
  )
  datestring_to_mmrs <- hash(all_filedates, mmrs)
  return(datestring_to_mmrs)
}


# Return a hash mapping dates to dataframes containing rows for those dates
processModel <- function(model, fs, region, src) {
  index_fields_all <- c('Serial', 'RetrievedDate', 'RetrievedTime', 'FileDate')
  index_fields <- index_fields_all[1:3]
  print(paste("Processing", length(fs), src, "data files for", model, "in", region))
  gc()
  timeit(
    dfs <- plapply(fs, function(f) {gc(); return(f$getDataFrame())}, parallel=parallel, ncores=ncores),
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
  
  timeit({
    dates <- indices$RetrievedDate %>% unique
    dates %<>% subset(dates <= Sys.Date() & dates >= Sys.Date() - (days - margin_days))
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

writeData <- function(date_to_df, model, region, src, date_to_mmr) {
  plapply(
    keys(date_to_df),
    function(date) {
      df <- date_to_df[[date]]
      # Use MMR to split out RAP MIF from RNZ - necessary hack
      # TODO: if no MMR file is available on the given date, find the closest version
      if(region=='RNZ') {
        mmr <- getWithDefault(date_to_mmr, date, F)
        if(mmr==F) {
          print(paste("No MMR for", region, "on", date, "- skipping"))
        }
        print("Writing separate RAP data file")
        '%ni%' <- Negate('%in%')
        df_rap <- df[df$Serial %ni% mmr$Device.Serial.Number,]
        df <- df[df$Serial %in% mmr$Device.Serial.Number,]
        writeDF(df_rap, date, model, 'RAP', src)
      }
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

getDeviceModels <- function(...) {
  res <- device_groups[[device_group]]
  res %<>% unlist %>% unname
  return(res)
}

################################################################################
# Arguments
################################################################################

makeParser <- function() {
  parser <- ArgumentParser$new()
  parser$add_argument(
    '--incremental', type='logical',
    action='store_true', default=FALSE,
    help='Limit to 10 days of data'
  )

  return(parser)
}


################################################################################
# Process each source
################################################################################

makeParser()
if(incremental) {
  days <- incremental_days
}
timeit(lapply(sources, processSource))
