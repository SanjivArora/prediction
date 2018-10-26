require(hash)

source("lib/Util.R")
source("lib/Parallel.R")

# Return groups of daily predictor data files where all required sources are present for a model on a given day. Sort these by date.
getFileSets <- function(data_files, required_sources) {
  group_hash <- groupBy(data_files, function(f) list(f$date, f$region, f$model))
  res <- list()
  for(fs in values(group_hash, simplify=FALSE)) {
    matches <- filterBy(fs, function(f) f$source %in% required_sources)
    if(length(matches) == length(required_sources)) {
      res <- append(res, list(matches))
    }
  }
  res <- sortBy(res, function(fs) fs[[1]]$date)
  return(res)
}

#getFileSets(fs, c("Count", "PMCount"))

# Return list containing valid sets of data files for each day (i.e. top level lest containing a list per day with data, these lists contain sets of DataFile objects grouped by {region, model}). Top level list is sorted by date.
getDailyFileSets <- function(data_files, required_sources) {
  file_sets <- getFileSets(data_files, required_sources)
  h <- groupBy(file_sets, function (fs) fs[[1]]$date)
  res <- sortBy(h, function (fs) fs[[1]][[1]]$date)
  return(res)
}

InstanceCounter <- setRefClass(
  "SCInstanceCounter",
  fields = list(
    # Dataframe of code instances
    codes = "data.frame",
    # Look ahead 0-n days to check for code
    max_days = "numeric",
    ###################
    # Private variables
    ###################
    serial_to_codes = "hash",
    # Hash mapping code number to count.
    counts = "hash",
    # Total number of predictor rows
    n_total = "numeric",
    # Number of predictor rows with no counts
    n_negative = "numeric"
  ),
  methods = list(
    initialize = function(...) {
      callSuper(...)
      code_list <- splitDataframe(.self$codes)
      .self$serial_to_codes <- groupBy(code_list, function (c) c$Serial)
      .self$counts <- hash()
      .self$n_total <- 0
      .self$n_negative <- 0
    },
    # Accept a dataframe for predictor data for the day - expects to be called in order but OK to skip calls for days with no data
    processDay = function(day_data, date) {
      serials <- day_data$Serial
      for(serial in serials) {
        cs <- .self$serial_to_codes[[serial]]
        hits <- filterBy(cs, function(c) {
          delta <- c$OCCUR_DATE - date
          in_window <- delta >= 0 && delta <= max_days
          return(in_window)
        })
        for(c in hits) {
          count <- getWithDefault(.self$counts, c$SC_CD, 0)
          .self$counts[[c$SC_CD]] <- count+1
        }
        if(length(hits) == 0) {
          .self$n_negative <= .self$n_negative + 1
        }
        .self$n_total <- .self$n_total + 1
      }
    },
    getCounts = function() .self$counts,
    getTotal = function() .self$n_total,
    getNegative = function() .self$negative
  )
)

# Call function for each day with valid data, pass a dataframe containing merged predictor data for that day. Run this in parallel over models and sources.
visitPredictorDataframes <- function(data_files, required_sources, f) {
  daily_file_sets <- getDailyFileSets(data_files, required_sources)
  for(file_sets in daily_file_sets) {
    date <- file_sets[[1]][[1]]$date
    parts <- plapply(file_sets, function(fs) dataFilesToDataframe(fs))
    df <- bind_rows(parts)
    f(df, date)
  }
}


sampleDataset <- function(data_files, required_sources, sc_codes, max_days=14) {
  counter <- InstanceCounter(codes=sc_codes, max_days=max_days)
  visitPredictorDataframes(data_files, required_sources, function(df, date) counter$processDay(df, date))
  print(counter)
}