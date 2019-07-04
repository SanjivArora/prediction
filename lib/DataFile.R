require(stringr)
require(lubridate)
require(R.utils)
require(testit)
require(utils)
source('lib/Util.R')
source('lib/Parallel.R')
source('lib/Feature.R')
source('lib/Storage.R')

default_bucket="ricoh-prediction-data-test2"
base_path=paste("s3://", default_bucket, sep="")

timezone="UTC"

data_log <- getModuleLogger("Model")

# CSV @Remote data file, if path is not absolute then interpret it as relative to base_path
DataFile <- setRefClass("DataFile",
  fields = list(
    path = "character",
    region = "character",
    model = "character",
    source = "character",
    date = "Date",
    default_date_fields = "list",
    default_time_fields = "list"
  ),
  methods = list(
    initialize = function(...) {
      callSuper(...)
      parts <- .self$pathToParts(.self$path)
      .self$region <- parts[[1]]
      .self$model <- parts[[2]]
      .self$source <- parts[[3]]
      .self$date <- as.Date(parts[[4]], format="%Y%m%d")
      .self$default_date_fields <- list("GetDate", "ChargeCounterDate")
      .self$default_time_fields <- list("GetTime", "ChargeCounterTime")
    },
    isS3 = function() {isS3Path(.self$getFullPath())},
    getFullPath = function() {
      if(isS3Path(.self$path) | isAbsolutePath(.self$path)) {
        return(path)
      } else {
        return(pathJoin(base_path, .self$path))
    }},
    getDataFrame = function(filter_no_data=TRUE, filter_outdated=TRUE, date_field=NA, time_field=NA, rename=TRUE, na_strings=c("","NA"), max_data_age=7, prepend_source=TRUE) {
      # Use as.is to disable representing values as factors
      if(.self$isS3()) {
        df <- withCloudFile(.self$getFullPath(), function(p) read.csv(p, header = TRUE, na.strings=na_strings, as.is=TRUE))
      } else {
        df <- read.csv(.self$getFullPath(), header = TRUE, na.strings=na_strings, as.is=TRUE)
      }
      if(is.na(date_field)) {
        # Use the first field matching default_date_fields
        for(name in names(df)) {
          if(name %in% .self$default_date_fields) {
            date_field <- name
            break
          }
        }
        if(is.na(date_field)) {
          stop(paste("Could not find a date field for", .self$path))
        }
        # Ditto for time field
        for(name in names(df)) {
          if(name %in% .self$default_time_fields) {
            time_field <- name
            break
          }
        }
        if(is.na(time_field)) {
          stop(paste("Could not find a time field for", .self$path))
        }
      }
      # Filter rows where the date field isn't set
      if(filter_no_data) {
        df <- df[!is.na(df[,date_field]),]
      }
      # Filter rows where the date field is corrupt
      check_date_string <- function(s) {
        x <- try( as.Date(s))
        return(class(x) != "try-error")
      }
      valid_dates <- lapply(df[,date_field], check_date_string)
      df <- df[unlist(valid_dates),]
      # Now that we have removed problem row, infer types
      df <- type.convert(df, as.is=TRUE, na.strings=na_strings)
      #Force required types
      df <- transform(
        df,
        Serial = as.character(Serial)
      )
      df[,date_field] <- as.Date(df[,date_field])
      if(nrow(df)==0) {
        data_log$warn(paste("Reading dataframe from file with no valid data:", .self$path))
      }
      # We don't know the precise timing of acquisition with respect to file naming and international datelines, so allow +1 day, -3 days.
      if(filter_outdated) {
        valid_dates <- .self$date - lubridate::days(-1:max_data_age)
        df <- df[df[,date_field] %in% valid_dates,]
        if(nrow(df)==0) {
          data_log$warn(paste("File contains no readings within freshness window:", .self$path))
        }
      }
      # Rewrite serial as concatenation of model and partial serial (i.e. as actual serial)
      df$Serial <- paste(df$Model, df$Serial, sep="")
      # Create a standardized RetrievedDate date field for convenience
      df[,"RetrievedDate"] <- df[,date_field]
      # Ditto for time and datetime
      df[,"RetrievedTime"] <- df[,time_field]
      df[,"RetrievedDateTime"] <- makeDateTimes(df[,"RetrievedDate"], df[,"RetrievedTime"])
      # Set file date
      if(nrow(df) > 0) {
       df[,"FileDate"] <- .self$date
      } else {
        # Work around for 0 row dataframes
        df[,"FileDate"] <- df[,date_field]
      }
      # Set row names to Serial
      row.names(df) <- unlist(df[,'Serial'])
      # Prepend source to feature names
      if(prepend_source) {
        colnames(df) <- lapply(colnames(df), function(name) {
          if(name %in% c('Serial', date_field, 'FileDate', 'RetrievedDate', 'RetrievedTime', 'RetrievedDateTime', 'Model')) {
            return(name)
          } else {
            return(paste(.self$source, name, sep="."))
          }
        })
      }
      # Canonicalise names
      if(rename) {
        colnames(df) <- lapply(colnames(df), canonicalFeatureName)
      }
      return(df)
    },
    # Accept a path to a data file and return list of strings for region, model, data type, date
    pathToParts = function(path) {
      r='(.*/)?([^_]+)_([^_]+)_([^_]+)_([^_]+).csv'
      m <- str_match(path, r)
      if(is.na(m[[1]])) {
        stop(paste("Path '", path, "' Does not match: ", r, sep=""))
      }
      # Don't want whole match or first capture group
      return(m[3:length(m)])
    }
  )
)


# Reference classes have no supprt for class or static methods, so define functions that notionally belong to the class but not the instances here

# Class method to return merged dataframe from list of instances. Merge on Serial and date and set row names to serial numbers.
dataFilesToDataframe <- function(instances, features=FALSE, parallel=TRUE) {
  data_log$debug(paste("Getting data file for", length(instances), "instances"))
  assert(length(unique(instances))==length(instances))
  dataframes <- plapply(instances, function(instance) instance$getDataFrame(), parallel=FALSE)
  res <- dataframes[[1]]
  if(length(dataframes) >= 2) {
    merge_on_candidates <- c("Serial", "Model", "FileDate")
    
    for (frame in dataframes[2:length(dataframes)]) {
      current <- res
      merge_on <- intersect(names(res), names(frame))
      merge_on <- intersect(merge_on, merge_on_candidates)
      res <- merge(res, frame, by.x = merge_on,by.y = merge_on)
      row.names(res) <- unlist(res[,'Serial'])
      # Use GetDate of of the first datafile
      if("GetDate" %in% names(current)) {
        res$GetDate <- current[res$Serial,]$GetDate
      }
    }
  }
  if(!isFALSE(features)) {
    # TODO: handle deltas
    additional <- c("Serial", "FileDate", "GetDate", "GetTime", "GetDateTime", "ChargeCounterDate", "Model")
    features <- append(features, additional)
    features <- intersect(colnames(res), features)
    res <- res[,unlist(features)]
  }
  if(nrow(res)==0) {
    data_log$warn(
      paste("Empty dataframe from the following set of files:",
            paste(lapply(instances, function(f) f$path), collapse=",")
      )
    )
  }
  return(res)
}

# Class method to return merged dataframe for a list of paths to files. Merge on Serial and set row names to serial numbers.
pathsToDataFrame <- function(paths, cls=DataFile) {
  instances <- lapply(paths, function(path) cls(path=path))
  res <- dataFilesToDataframe(instances)
  return(res)
}


makePattern <- function(regions=NA, models=NA, sources=NA) {
  if(identical(regions, NA)) {
    region_pattern <- '[^_]*'
  } else {
    region_pattern <- paste(regions, collapse='|', sep="")
  }
  if(identical(models, NA)) {
    model_pattern <- '[^_]*'
  } else {
    model_pattern <- paste(models, collapse='|', sep="")
  }
  if(identical(sources, NA)) {
    source_pattern <- '[^_]*'
  } else {
    source_pattern <- paste(sources, collapse='|', sep="")
  }
  pattern <- paste(
    paste("(", region_pattern, ")", collapse="", sep=""),
    paste("(", model_pattern, ")", collapse="", sep=""),
    paste("(", source_pattern, ")", collapse="", sep=""),
    sep='_'
  )
  return(pattern)
}

# Instances for directory, default to base_path
# regions and models are best effort - backend may not support this restriction
instancesForDir <- function(directory=base_path, regions=NA, models=NA, sources=NA, cls=DataFile) {
  pattern <- makePattern(regions=regions, models=models, sources=sources)
  if(isS3Path(directory)) {
    file_data <- getBucketAll(paste(directory, "/", sep=""))
    paths <- lapply(file_data, function(x) x$Key) %>% unlist
    paths <- lapply(paths, function(p) pathJoin(directory, p))
  } else {
  paths <- list.files(directory, pattern=pattern, full.names=TRUE)
  }
  paths <- sort(unlist(paths))
  res <- lapply(paths, function(path) cls(path=path))
  return(res)
}

# Get instances for cloud files stored as <date>/<x> for specified number of days.
# Filter for regions, models and sources if given (match all by default)
instancesForBucket <- function(bucket=default_bucket, days=90, end_date=NA, regions=NA, models=NA, sources=NA, cls=DataFile, verbose=TRUE, parallel=TRUE) {
  pattern <- makePattern(regions=regions, models=models, sources=sources)
  timeit(
    paths <- listCloudFiles(bucket, days=days, end_date=end_date, pattern=pattern, parallel=parallel),
    "listing cloud files",
    verbose=verbose
  )
  paths <- lapply(paths, function(path) paste("s3://", bucket, '/', path, sep=''))
  timeit(
    res <- plapply(paths, function(path) cls(path=path), parallel=parallel),
    "instantiating file objects",
    verbose=verbose
  )
  return(res)
}

getEligibleModelDataFiles <- function(region, model, sources, days=NA, end_date=NA, all_files=NA) {
  # Don't use is.na here as it generates a warning message when used with a vector
  if(identical(all_files, NA)) {
    all_files <- instancesForBucket(regions=c(region), models=c(model), sources=sources, days=days, end_date=end_date)
  }
  # Restrict data set to days for which we have current SC data, and a maximum of data_days
  sc_files <- filterBy(all_files, function(f) f$source=="SC" && f$region==region && f$model==model)
  sc_files <- sortBy(sc_files, function(f) f$date)
  if(length(sc_files)==0) {
    return(list())
  }
  latest <- last(sortBy(sc_files, function(f) f$date))
  latest_sc_file_date <- as.Date(latest$date)
  latest_data_file_date <- latest_sc_file_date
  filtered_data_files <- filterBy(
    all_files,
    function(f) {
      as.Date(f$date) <= latest_data_file_date &&
      f$region == region &&
      f$model == model &&
      f$source %in% sources
    }
  )
  return(filtered_data_files)
}

getEligibleFileSets <- function(regions, models, sources, days=NA, end_date=NA, ...) {
  # Don't restrict sources, we need code files for filtering date ranges
  all_files <- instancesForBucket(regions=regions, models=models, days=days, end_date=end_date)
  
  filtered_files <- list()
  for(region in regions) {
    for(model in models) {
      files <- getEligibleModelDataFiles(region, model, sources, all_files=all_files, days=days, end_date=end_date, ...)
      filtered_files <- append(filtered_files, files)
    }
  }

  file_sets <- getDailyFileSets(filtered_files, sources)
  return(file_sets)
}

latestFileDate <- function(fs=FALSE) {
  if(isFALSE(fs)) {
    fs <- instancesForBucket()
  }
  latest <- sortBy(fs, function(f) f$date, desc=TRUE)[[1]]
  return(latest$date)
}

serialToModel <- function(serial) {
  substr(serial, 1, 3)
}
