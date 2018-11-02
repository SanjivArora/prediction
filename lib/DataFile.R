require(stringr)
require(lubridate)
require(R.utils)

source('lib/Util.R')
source('lib/Parallel.R')

default_sources=c('Count', 'PMCount')
base_path="~/data/"
timezone="UTC"


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
    getDataFrame = function(filter_no_data=TRUE, filter_outdated=TRUE, date_field=NA) {
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
      # We don't know the precise timing of acquisition with respect to file naming and international datelines, so allow +1 day, -1 week.
      if(filter_outdated) {
        valid_dates <- .self$date - lubridate::days(-1:7)
        df <- df[df[,date_field] %in% valid_dates,]
      }
      # Rewrite serial as concatenation of model and partial serial (i.e. as actual serial)
      df$Serial <- paste(df$Model, df$Serial, sep="")
      # Create a standardized RetrievedDate date field for convenience
      df[,"RetrievedDate"] <- df[,date_field]
      # Set row names to Serial
      row.names(df) <- unlist(df[,'Serial'])

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

# Class method to return merged dataframe from list of instances. Merge on Serial and set row names to serial numbers.
dataFilesToDataframe <- function(instances, parallel=TRUE) {
  dataframes <- plapply(instances, function(instance) instance$getDataFrame(), parallel)
  res <- dataframes[[1]]
  if(length(dataframes) >= 2) {
    merge_on <- c("Serial")
    for (frame in dataframes[2:length(dataframes)]) {
      res <- merge(res, frame, by.x = merge_on,by.y = merge_on)
    }
  }
  row.names(res) <- unlist(res[,'Serial'])
  return(res)
}

# Class method to return merged dataframe for a list of paths to files. Merge on Serial and set row names to serial numbers.
pathsToDataFrame <- function(paths, cls=DataFile) {
  instances <- lapply(paths, function(path) cls(path=path))
  res <- dataFilesToDataframe(instances)
  return(res)
}

# Instances for directory, default to base_path
instancesForDir <- function(directory=base_path, pattern=".*", cls=DataFile) {
  paths <- list.files(directory,pattern=pattern, full.names=TRUE)
  paths <- sort(unlist(paths))
  res <- lapply(paths, function(path) cls(path=path))
  return(res)
}


#d <- DataFile(path="RNZ_E16_Count_20180714.csv")