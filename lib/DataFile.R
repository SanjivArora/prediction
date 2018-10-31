require(stringr)
require(lubridate)
require(R.utils)

source('lib/Util.R')
source('lib/Parallel.R')

default_sources=c('Count', 'PMCount')
base_path="~/data/"
timezone="UTC"


# CSV data file, if path is not absolute then interpret it as relative to base_path
DataFile <- setRefClass("DataFile",
  fields = list(
    path = "character",
    region = "character",
    model = "character",
    source = "character",
    date = "Date"),
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
    },
    getDate = function() {.self$test},
    getFullPath = function() {joinPaths(base_path, path)},
    getDataFrame = function() {
      df <- read.csv(.self$getFullPath(), header = TRUE, na.strings=c("","NA"))
      # Force serial to be a string
      df <- transform(
        df,
        Serial = as.character(Serial)
      )
      # Rewrite serial as concatenation of model and partial serial (i.e. as actual serial)
      df$Serial <- paste(df$Model, df$Serial, sep="")
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
    for (frame in dataframes[2:length(dataframes)]) {
      res <- merge(res, frame, by.x = "Serial",by.y = "Serial")
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