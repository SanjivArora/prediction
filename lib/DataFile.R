require(stringr)
require(lubridate)
require(R.utils)
require(testit)

source('lib/Util.R')
source('lib/Parallel.R')
source('lib/Feature.R')

base_path="~/data/"
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
    default_date_fields = "list"
  ),
  methods = list(
    initialize = function(...) {
      callSuper(...)
      if (!file.exists(.self$getFullPath())) {
        stop(paste("File does not exist at: ", .self$getFullPath()))
      }
      parts <- .self$pathToParts(.self$path)
      .self$region <- parts[[1]]
      .self$model <- parts[[2]]
      .self$source <- parts[[3]]
      # Specify date format as recognizing this is, surprisingly, a performance bottleneck
      .self$date <- lubridate::as_date(parts[[4]], tz=timezone, format="%Y%m%d")
      if(is.na(.self$date)) {
        stop(paste("Unable to parse date string: ", parts[[4]]))
      }
      .self$default_date_fields <- list("GetDate", "ChargeCounterDate")
    },
    getDate = function() {.self$test},
    getFullPath = function() {joinPaths(base_path, path)},
    getDataFrame = function(filter_no_data=TRUE, filter_outdated=TRUE, date_field=NA, rename=TRUE) {
      # Use as.is to disable representing values as factors
      df <- read.csv(.self$getFullPath(), header = TRUE, na.strings=c("","NA"), as.is=TRUE)
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
      #Force required types
      df <- transform(
        df,
        Serial = as.character(Serial)
      )
      df[,date_field] <- as.Date(df[,date_field])
      if(nrow(df)==0) {
        data_log$warn(paste("Reading dataframe from file with no data:", .self$path))
      }
      # We don't know the precise timing of acquisition with respect to file naming and international datelines, so allow +1 day, -3 days.
      if(filter_outdated) {
        valid_dates <- .self$date - lubridate::days(-1:3)
        df <- df[df[,date_field] %in% valid_dates,]
        if(nrow(df)==0) {
          data_log$warn(paste("File contains no current data:", .self$path))
        }
      }
      # Rewrite serial as concatenation of model and partial serial (i.e. as actual serial)
      df$Serial <- paste(df$Model, df$Serial, sep="")
      # Create a standardized RetrievedDate date field for convenience
      df[,"RetrievedDate"] <- df[,date_field]
      # Set file date
      if(nrow(df) > 0) {
       df[,"FileDate"] <- .self$date
      } else {
        # Work around for 0 row dataframes
        df[,"FileDate"] <- df[,date_field]
      }
      # Set row names to Serial
      row.names(df) <- unlist(df[,'Serial'])
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

# Class method to return merged dataframe from list of instances. Merge on Serial and FileDate and set row names to serial numbers.
dataFilesToDataframe <- function(instances, features=FALSE, parallel=TRUE) {
  data_log$debug(paste("Getting data file for", length(instances), "instances"))
  assert(length(unique(instances))==length(instances))
  dataframes <- plapply(instances, function(instance) instance$getDataFrame(), parallel=FALSE)
  res <- dataframes[[1]]
  if(length(dataframes) >= 2) {
    merge_on <- c("Serial", "FileDate", "Model")
    for (frame in dataframes[2:length(dataframes)]) {
      res <- merge(res, frame, by.x = merge_on,by.y = merge_on)
    }
  }
  row.names(res) <- unlist(res[,'Serial'])
  if(!isFALSE(features)) {
    # TODO: handle deltas
    additional <- c("Serial", "FileDate", "GetDate", "ChargeCounterDate", "Model")
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

# Instances for directory, default to base_path
instancesForDir <- function(directory=base_path, regions=NA, models=NA, cls=DataFile) {
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
  pattern <- paste(
    paste("(", region_pattern, ")", collapse="", sep=""),
    paste("(", model_pattern, ")", collapse="", sep=""),
    sep='_'
  )
  paths <- list.files(directory,pattern=pattern, full.names=TRUE)
  paths <- sort(unlist(paths))
  res <- lapply(paths, function(path) cls(path=path))
  return(res)
}

getEligibleModelDataFiles <- function(region, model, sources, sc_code_days=14, sc_data_buffer=4, earliest_file_date=NA, latest_file_date=NA, all_files=NA) {
  # Don't use is.na here as it generates a warning message when used with a vector
  if(identical(all_files, NA)) {
    all_files <- instancesForDir()
  }
  if(!identical(earliest_file_date, NA)) {
    all_files <- filterBy(all_files, function(f) f$date >= earliest_file_date)
  }
  if(!identical(latest_file_date, NA)) {
    all_files <- filterBy(all_files, function(f) f$date <= latest_file_date)
  }
  # Restrict data set to days for which we have current SC code data, and a maximum of data_days
  sc_files <- filterBy(all_files, function(f) f$source=="SC" && f$region==region && f$model==model)
  sc_files <- sortBy(sc_files, function(f) f$date)
  if(length(sc_files)==0) {
    return(list())
  }
  latest <- last(sortBy(sc_files, function(f) f$date))
  latest_sc_file_date <- as.Date(latest$date)
  # Wait an additional period for SC data to be up to date for a machine, the lag can be a few days.
  latest_data_file_date <- latest_sc_file_date - sc_code_days - sc_data_buffer
  # TODO: check per region+model combination
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

getEligibleFileSets <- function(regions, models, sources, ...) {
  all_files <- instancesForDir(regions=regions, models=models)
  
  filtered_files <- list()
  for(region in regions) {
    for(model in models) {
      files <- getEligibleModelDataFiles(region, model, sources, all_files=all_files, ...)
      filtered_files <- append(filtered_files, files)
    }
  }
  
  file_sets <- getDailyFileSets(filtered_files, sources)
  return(file_sets)
}